// Copyright 2022 RisingWave Labs
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

use std::sync::Arc;

use futures::{StreamExt, pin_mut};
use futures_async_stream::try_stream;

use crate::executor::error::StreamExecutorError;
use crate::executor::{ExecutorInfo, Message, MessageStream};
use crate::task::FragmentId;

/// Streams wrapped by `epoch_check` will check whether the first message received is a barrier, and
/// the epoch in the barriers are monotonically increasing.
///
/// Barriers are contiguous (`prev` equals the last `curr`), except for a barrier that wakes up the
/// fragment after it slept during selective barrier injection, whose `prev` may be newer.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn epoch_check(
    info: Arc<ExecutorInfo>,
    fragment_id: FragmentId,
    input: impl MessageStream,
) {
    // Epoch number recorded from last barrier message.
    let mut last_epoch = None;

    pin_mut!(input);
    while let Some(message) = input.next().await {
        let message = message?;

        if let Message::Barrier(b) = &message {
            let new_epoch = b.epoch.curr;
            let stale = last_epoch
                .map(|last_epoch| last_epoch > new_epoch)
                .unwrap_or(false);

            if stale {
                panic!(
                    "epoch check failed on {}: last epoch is {:?}, while the epoch of incoming barrier is {}.\nstale barrier: {:?}",
                    info.identity, last_epoch, new_epoch, b
                );
            }

            if let Some(last_epoch) = last_epoch
                && !b.is_with_stop_mutation()
            {
                if b.is_fragment_waking(fragment_id) {
                    assert!(
                        b.epoch.prev >= last_epoch,
                        "stale wake-up barrier: last barrier's epoch = {}, while current barrier prev={} curr={}",
                        last_epoch,
                        b.epoch.prev,
                        b.epoch.curr
                    );
                } else {
                    assert_eq!(
                        b.epoch.prev, last_epoch,
                        "missing barrier: last barrier's epoch = {}, while current barrier prev={} curr={}",
                        last_epoch, b.epoch.prev, b.epoch.curr
                    );
                }
            }

            last_epoch = Some(new_epoch);
        } else if last_epoch.is_none() && !info.identity.contains("BatchQuery") {
            panic!(
                "epoch check failed on {}: the first message must be a barrier",
                info.identity
            )
        }

        yield message;
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use futures::pin_mut;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::util::epoch::test_epoch;

    use super::*;
    use crate::executor::test_utils::MockSource;

    #[tokio::test]
    async fn test_epoch_ok() {
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(Default::default(), vec![]);
        tx.push_barrier(test_epoch(1), false);
        tx.push_chunk(StreamChunk::default());
        tx.push_barrier(test_epoch(2), false);
        tx.push_barrier(test_epoch(3), false);
        tx.push_barrier(test_epoch(4), false);

        let checked = epoch_check(source.info().clone().into(), 0.into(), source.execute());
        pin_mut!(checked);

        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Barrier(b) if b.epoch.curr == test_epoch(1));
        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Chunk(_));
        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Barrier(b) if b.epoch.curr == test_epoch(2));
        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Barrier(b) if b.epoch.curr == test_epoch(3));
        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Barrier(b) if b.epoch.curr == test_epoch(4));
    }

    #[should_panic]
    #[tokio::test]
    async fn test_epoch_bad() {
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(Default::default(), vec![]);
        tx.push_barrier(test_epoch(100), false);
        tx.push_chunk(StreamChunk::default());
        tx.push_barrier(test_epoch(514), false);
        tx.push_barrier(test_epoch(514), false);
        tx.push_barrier(test_epoch(114), false);

        let checked = epoch_check(source.info().clone().into(), 0.into(), source.execute());
        pin_mut!(checked);

        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Barrier(b) if b.epoch.curr == test_epoch(100));
        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Chunk(_));
        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Barrier(b) if b.epoch.curr == test_epoch(514));
        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Barrier(b) if b.epoch.curr == test_epoch(514));

        checked.next().await.unwrap().unwrap(); // should panic
    }

    #[should_panic]
    #[tokio::test]
    async fn test_epoch_first_not_barrier() {
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(Default::default(), vec![]);
        tx.push_chunk(StreamChunk::default());
        tx.push_barrier(test_epoch(114), false);

        let checked = epoch_check(source.info().clone().into(), 0.into(), source.execute());
        pin_mut!(checked);

        checked.next().await.unwrap().unwrap(); // should panic
    }

    fn wake_up_barrier(curr: u64, prev: u64, waking: Option<u32>) -> crate::executor::Barrier {
        let mut barrier = crate::executor::Barrier::with_prev_epoch_for_test(curr, prev);
        barrier.selective = Some(Arc::new(crate::executor::SelectiveBarrierInfo {
            sleeping_fragments: Default::default(),
            waking_fragments: waking.into_iter().map(Into::into).collect(),
        }));
        barrier
    }

    #[tokio::test]
    async fn test_epoch_jump_on_wake_up() {
        let (tx, source) = MockSource::channel();
        let source = source.into_executor(Default::default(), vec![]);
        tx.send_barrier(crate::executor::Barrier::new_test_barrier(test_epoch(1)));
        // The fragment slept during epochs 2 to 4 and wakes up at epoch 5.
        tx.send_barrier(wake_up_barrier(test_epoch(5), test_epoch(4), Some(7)));
        tx.send_barrier(crate::executor::Barrier::with_prev_epoch_for_test(
            test_epoch(6),
            test_epoch(5),
        ));

        let checked = epoch_check(source.info().clone().into(), 7.into(), source.execute());
        pin_mut!(checked);

        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Barrier(b) if b.epoch.curr == test_epoch(1));
        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Barrier(b) if b.epoch.curr == test_epoch(5));
        assert_matches!(checked.next().await.unwrap().unwrap(), Message::Barrier(b) if b.epoch.curr == test_epoch(6));
    }

    #[should_panic(expected = "missing barrier")]
    #[tokio::test]
    async fn test_epoch_jump_without_wake_up() {
        let (tx, source) = MockSource::channel();
        let source = source.into_executor(Default::default(), vec![]);
        tx.send_barrier(crate::executor::Barrier::new_test_barrier(test_epoch(1)));
        // Another fragment wakes up: a jump for fragment 7 is still a missing barrier.
        tx.send_barrier(wake_up_barrier(test_epoch(5), test_epoch(4), Some(8)));

        let checked = epoch_check(source.info().clone().into(), 7.into(), source.execute());
        pin_mut!(checked);

        checked.next().await.unwrap().unwrap();
        checked.next().await.unwrap().unwrap(); // should panic
    }

    #[tokio::test]
    async fn test_empty() {
        let (_, source) = MockSource::channel();
        let source = source
            .stop_on_finish(false)
            .into_executor(Default::default(), vec![]);
        let checked = epoch_check(source.info().clone().into(), 0.into(), source.execute());
        pin_mut!(checked);

        assert!(checked.next().await.transpose().unwrap().is_none());
    }
}
