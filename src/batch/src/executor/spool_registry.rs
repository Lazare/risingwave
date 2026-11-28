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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use parking_lot::Mutex;

use crate::error::Result;

/// The spools of one plan fragment (`batch_share_identical_subplans`), by spool id.
///
/// The nodes of a spool (the producer, which computes the sub-plan, and its readers) are built
/// separately: each one gets the same spool from the registry, whatever the order in which they
/// are built. The registry lives as long as the builder of the plan fragment: once built, a
/// spool is only held by its nodes, and is dropped with the last of them.
///
/// The spool type is defined by the executors, hence the type erasure.
#[derive(Default)]
pub struct SpoolRegistry {
    spools: Mutex<HashMap<u32, Arc<dyn Any + Send + Sync>>>,
}

impl SpoolRegistry {
    /// The spool `spool_id`, created by `create` if it does not exist yet.
    pub fn get_or_create<T: Any + Send + Sync>(
        &self,
        spool_id: u32,
        create: impl FnOnce() -> T,
    ) -> Result<Arc<T>> {
        let spool = self
            .spools
            .lock()
            .entry(spool_id)
            .or_insert_with(|| Arc::new(create()))
            .clone();
        spool
            .downcast::<T>()
            .map_err(|_| anyhow!("spool {spool_id} has another type").into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_or_create() {
        let registry = SpoolRegistry::default();
        let a = registry.get_or_create(1, || 10u32).unwrap();
        let b = registry.get_or_create(1, || 20u32).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(*b, 10);
        let c = registry.get_or_create(2, || 30u32).unwrap();
        assert_eq!(*c, 30);
        assert!(registry.get_or_create(2, || 1u64).is_err());
        drop(registry);
        // The spools outlive the registry while their nodes hold them.
        assert_eq!(*a, 10);
    }
}
