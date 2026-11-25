// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The native engine of the Postgres CDC source, selected by `cdc.engine = 'native'`.
//!
//! It reads the logical replication stream of the upstream database directly, instead of
//! running the Debezium connector in the embedded JVM, and validates the sources and their
//! tables against the upstream database without the JVM either. It produces the same messages and
//! offsets as the Debezium based engine, so that everything downstream of the reader is
//! shared by the two engines.

mod pgoutput;
mod protocol;
mod reader;
mod translator;
mod validate;
mod value;

use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
pub use reader::{NativeCdcReceiver, start_postgres_cdc_stream};
use tokio::sync::watch;
pub use validate::validate_postgres_source;

use self::pgoutput::Lsn;
use super::split::extract_postgres_lsn_commit_from_offset_str;

/// The option to select the engine that reads the changes from the upstream database.
pub const CDC_ENGINE_KEY: &str = "cdc.engine";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CdcEngine {
    /// The Debezium connector running in the embedded JVM.
    #[default]
    Debezium,
    /// The reader implemented in Rust, see the [module](self) documentation.
    Native,
}

impl CdcEngine {
    pub const DEBEZIUM: &'static str = "debezium";
    pub const NATIVE: &'static str = "native";

    /// Parse the value of the [`CDC_ENGINE_KEY`] option, `None` if the option is not specified.
    pub fn from_option(value: Option<&str>) -> anyhow::Result<Self> {
        match value {
            None => Ok(Self::default()),
            Some(v) if v.eq_ignore_ascii_case(Self::DEBEZIUM) => Ok(Self::Debezium),
            Some(v) if v.eq_ignore_ascii_case(Self::NATIVE) => Ok(Self::Native),
            Some(v) => Err(anyhow::anyhow!(
                "invalid value `{}` of option `{}`, expect `{}` or `{}`",
                v,
                CDC_ENGINE_KEY,
                Self::DEBEZIUM,
                Self::NATIVE
            )),
        }
    }
}

/// The generation of a reader and the channel to pass it the durably consumed position.
type AckChannel = (u64, watch::Sender<Lsn>);

/// The channels to pass the durably consumed position to the running readers, by source id.
/// A channel is tagged with the generation of its reader.
///
/// The entry of a source is kept as `None` after its reader is gone, to tell that the source
/// is read by the native engine and its offset must not be committed through the JVM.
static ACK_CHANNELS: LazyLock<Mutex<HashMap<u64, Option<AckChannel>>>> =
    LazyLock::new(Default::default);

static NEXT_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Unregisters the channel of a reader when dropped.
struct AckRegistration {
    source_id: u64,
    generation: u64,
}

impl Drop for AckRegistration {
    fn drop(&mut self) {
        let mut channels = ACK_CHANNELS.lock();
        if let Some(entry) = channels.get_mut(&self.source_id)
            && entry
                .as_ref()
                .is_some_and(|(generation, _)| *generation == self.generation)
        {
            *entry = None;
        }
    }
}

/// Register the channel of a new reader of the source, which replaces the one of the previous
/// reader, if any. The previous reader stops when it notices that its channel is closed.
fn register_ack_channel(source_id: u64, initial: Lsn) -> (AckRegistration, watch::Receiver<Lsn>) {
    let (sender, receiver) = watch::channel(initial);
    let generation = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
    ACK_CHANNELS
        .lock()
        .insert(source_id, Some((generation, sender)));
    (
        AckRegistration {
            source_id,
            generation,
        },
        receiver,
    )
}

/// Commit the offset of a source read by the native engine to the upstream database, after
/// the offset is persisted by a checkpoint.
///
/// Returns `None` if the source is not read by the native engine.
pub fn commit_offset(source_id: u64, offset: &str) -> Option<anyhow::Result<()>> {
    let channels = ACK_CHANNELS.lock();
    let entry = channels.get(&source_id)?;
    let Some((_, sender)) = entry else {
        // The reader is gone, the offset will be committed by the next reader on start.
        return Some(Ok(()));
    };
    // The offset has no commit position before the first change is consumed.
    if let Some(lsn) = extract_postgres_lsn_commit_from_offset_str(offset) {
        sender.send_if_modified(|current| {
            if lsn > *current {
                *current = lsn;
                true
            } else {
                false
            }
        });
    }
    Some(Ok(()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_option() {
        assert_eq!(CdcEngine::from_option(None).unwrap(), CdcEngine::Debezium);
        assert_eq!(
            CdcEngine::from_option(Some("debezium")).unwrap(),
            CdcEngine::Debezium
        );
        assert_eq!(
            CdcEngine::from_option(Some("Native")).unwrap(),
            CdcEngine::Native
        );
        assert!(CdcEngine::from_option(Some("rust")).is_err());
    }

    #[test]
    fn test_commit_offset() {
        let offset = |lsn_commit: u64| {
            format!(
                r#"{{"sourcePartition":{{"server":"RW_CDC_1"}},"sourceOffset":{{"lsn":1,"txId":1,"ts_usec":1,"lsn_commit":{},"lsn_proc":{}}},"isHeartbeat":false}}"#,
                lsn_commit, lsn_commit
            )
        };
        let source_id = 987_654_321;
        // not a source of the native engine
        assert!(commit_offset(source_id, &offset(100)).is_none());

        let (registration, rx) = register_ack_channel(source_id, 100);
        commit_offset(source_id, &offset(200)).unwrap().unwrap();
        assert_eq!(*rx.borrow(), 200);
        // the position never goes backwards
        commit_offset(source_id, &offset(150)).unwrap().unwrap();
        assert_eq!(*rx.borrow(), 200);
        // nothing to commit before the first change is consumed
        commit_offset(source_id, "").unwrap().unwrap();
        assert_eq!(*rx.borrow(), 200);

        // a new reader takes over, dropping the old registration keeps the new channel
        let (new_registration, new_rx) = register_ack_channel(source_id, 200);
        assert!(rx.has_changed().is_err());
        drop(registration);
        commit_offset(source_id, &offset(300)).unwrap().unwrap();
        assert_eq!(*new_rx.borrow(), 300);

        // the source is still known after its reader is gone
        drop(new_registration);
        commit_offset(source_id, &offset(400)).unwrap().unwrap();
    }
}
