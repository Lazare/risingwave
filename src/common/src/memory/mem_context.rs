// Copyright 2023 RisingWave Labs
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

use std::ops::Deref;
use std::sync::Arc;

use prometheus::core::Atomic;
use risingwave_common_metrics::TrAdderAtomic;

use super::MonitoredGlobalAlloc;
use crate::metrics::{LabelGuardedIntGauge, TrAdderGauge};

pub trait MemCounter: Send + Sync + 'static {
    fn add(&self, bytes: i64);
    fn get_bytes_used(&self) -> i64;

    /// Called when an allocation is refused because it would exceed the limit of the context that
    /// owns this counter.
    fn on_limit_exceeded(&self) {}
}

impl MemCounter for TrAdderGauge {
    fn add(&self, bytes: i64) {
        self.add(bytes)
    }

    fn get_bytes_used(&self) -> i64 {
        self.get()
    }
}

impl MemCounter for TrAdderAtomic {
    fn add(&self, bytes: i64) {
        self.inc_by(bytes)
    }

    fn get_bytes_used(&self) -> i64 {
        self.get()
    }
}

impl MemCounter for LabelGuardedIntGauge {
    fn add(&self, bytes: i64) {
        self.deref().add(bytes)
    }

    fn get_bytes_used(&self) -> i64 {
        self.get()
    }
}

struct MemoryContextInner {
    counter: Box<dyn MemCounter>,
    parent: Option<MemoryContext>,
    mem_limit: u64,
    /// See [`MemoryContext::new_with_reservation`].
    reservation: Option<Reservation>,
}

/// The first `bytes` used by a context are charged to `charged_to` instead of its parent.
struct Reservation {
    bytes: i64,
    charged_to: MemoryContext,
}

impl Reservation {
    /// Splits a usage into the part covered by the reservation and the part beyond it.
    fn split(&self, used: i64) -> (i64, i64) {
        (used.clamp(0, self.bytes), (used - self.bytes).max(0))
    }
}

#[derive(Clone)]
pub struct MemoryContext {
    /// Add None op mem context, so that we don't need to return [`Option`] in
    /// `BatchTaskContext`. This helps with later `Allocator` implementation.
    inner: Option<Arc<MemoryContextInner>>,
}

impl MemoryContext {
    pub fn new(parent: Option<MemoryContext>, counter: impl MemCounter) -> Self {
        let mem_limit = parent.as_ref().map_or_else(|| u64::MAX, |p| p.mem_limit());
        Self::new_with_mem_limit(parent, counter, mem_limit)
    }

    pub fn new_with_mem_limit(
        parent: Option<MemoryContext>,
        counter: impl MemCounter,
        mem_limit: u64,
    ) -> Self {
        let c = Box::new(counter);
        Self {
            inner: Some(Arc::new(MemoryContextInner {
                counter: c,
                parent,
                mem_limit,
                reservation: None,
            })),
        }
    }

    /// Creates a context with a reserved amount of memory.
    ///
    /// The first `reserved` bytes that the context uses are charged to `reserved_in`, where they
    /// are expected to be set aside for it. Only what it uses beyond them is charged to `shared`,
    /// which it shares with other contexts and which decides whether it may grow.
    pub fn new_with_reservation(
        shared: MemoryContext,
        reserved_in: MemoryContext,
        reserved: u64,
        counter: impl MemCounter,
    ) -> Self {
        Self {
            inner: Some(Arc::new(MemoryContextInner {
                counter: Box::new(counter),
                parent: Some(shared),
                mem_limit: u64::MAX,
                reservation: Some(Reservation {
                    bytes: reserved.min(i64::MAX as u64) as i64,
                    charged_to: reserved_in,
                }),
            })),
        }
    }

    /// Creates a noop memory context.
    pub fn none() -> Self {
        Self { inner: None }
    }

    pub fn root(counter: impl MemCounter, mem_limit: u64) -> Self {
        Self::new_with_mem_limit(None, counter, mem_limit)
    }

    pub fn for_spill_test() -> Self {
        Self::new_with_mem_limit(None, TrAdderAtomic::new(0), 0)
    }

    /// Add `bytes` memory usage. Pass negative value to decrease memory usage.
    /// Returns `false` if the memory usage exceeds the limit.
    pub fn add(&self, bytes: i64) -> bool {
        if let Some(inner) = &self.inner {
            if let Some(reservation) = &inner.reservation {
                return Self::add_with_reservation(inner, reservation, bytes);
            }
            if (inner.counter.get_bytes_used() + bytes) as u64 > inner.mem_limit {
                inner.counter.on_limit_exceeded();
                return false;
            }
            if let Some(parent) = &inner.parent {
                if parent.add(bytes) {
                    inner.counter.add(bytes);
                } else {
                    return false;
                }
            } else {
                inner.counter.add(bytes);
            }
        }
        true
    }

    fn add_with_reservation(
        inner: &MemoryContextInner,
        reservation: &Reservation,
        bytes: i64,
    ) -> bool {
        let used = inner.counter.get_bytes_used();
        let (reserved_before, shared_before) = reservation.split(used);
        let (reserved_after, shared_after) = reservation.split(used + bytes);
        let reserved_diff = reserved_after - reserved_before;
        let shared_diff = shared_after - shared_before;

        if shared_diff != 0
            && let Some(shared) = &inner.parent
            && !shared.add(shared_diff)
        {
            inner.counter.on_limit_exceeded();
            return false;
        }
        if reserved_diff != 0 && !reservation.charged_to.add(reserved_diff) {
            if shared_diff != 0
                && let Some(shared) = &inner.parent
            {
                shared.add(-shared_diff);
            }
            inner.counter.on_limit_exceeded();
            return false;
        }
        inner.counter.add(bytes);
        true
    }

    pub fn get_bytes_used(&self) -> i64 {
        if let Some(inner) = &self.inner {
            inner.counter.get_bytes_used()
        } else {
            0
        }
    }

    /// The memory that other contexts hold in the parent of this context. When it is zero, a
    /// refused [`add`](Self::add) cannot succeed later on by waiting for others to release memory.
    pub fn parent_bytes_used_by_others(&self) -> i64 {
        let Some(inner) = &self.inner else {
            return 0;
        };
        let Some(parent) = &inner.parent else {
            return 0;
        };
        let used = inner.counter.get_bytes_used();
        let own = match &inner.reservation {
            Some(reservation) => reservation.split(used).1,
            None => used,
        };
        (parent.get_bytes_used() - own).max(0)
    }

    pub fn mem_limit(&self) -> u64 {
        if let Some(inner) = &self.inner {
            inner.mem_limit
        } else {
            u64::MAX
        }
    }

    /// Check if the memory usage exceeds the limit.
    /// Returns `false` if the memory usage exceeds the limit.
    pub fn check_memory_usage(&self) -> bool {
        if let Some(inner) = &self.inner {
            if inner.counter.get_bytes_used() as u64 > inner.mem_limit {
                return false;
            }
            if let Some(parent) = &inner.parent {
                return parent.check_memory_usage();
            }
        }

        true
    }

    /// Creates a new global allocator that reports memory usage to this context.
    pub fn global_allocator(&self) -> MonitoredGlobalAlloc {
        MonitoredGlobalAlloc::with_memory_context(self.clone())
    }
}

impl Drop for MemoryContextInner {
    fn drop(&mut self) {
        let used = self.counter.get_bytes_used();
        if let Some(reservation) = &self.reservation {
            let (reserved, shared) = reservation.split(used);
            reservation.charged_to.add(-reserved);
            if let Some(p) = &self.parent {
                p.add(-shared);
            }
        } else if let Some(p) = &self.parent {
            p.add(-used);
        }
    }
}
