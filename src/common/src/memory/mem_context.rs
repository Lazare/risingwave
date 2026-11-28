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
use std::sync::atomic::{AtomicU64, Ordering};

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
    /// See [`MemoryContext::set_mem_limit`].
    mem_limit: AtomicU64,
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
                mem_limit: AtomicU64::new(mem_limit),
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
                mem_limit: AtomicU64::new(u64::MAX),
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
    /// Returns `false` if the memory usage exceeds the limit. Releasing memory always succeeds,
    /// even when the context uses more than its limit, which happens after the limit was lowered
    /// with [`set_mem_limit`](Self::set_mem_limit).
    pub fn add(&self, bytes: i64) -> bool {
        if let Some(inner) = &self.inner {
            if let Some(reservation) = &inner.reservation {
                return Self::add_with_reservation(inner, reservation, bytes);
            }
            if bytes > 0
                && (inner.counter.get_bytes_used() + bytes) as u64
                    > inner.mem_limit.load(Ordering::Relaxed)
            {
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

    /// Counts `bytes` whatever the limits: for memory that is already allocated, such as the
    /// allocations of a [`MonitoredGlobalAlloc`], which cannot be refused. The context and its
    /// parents may then use more than their limits, so that the next [`add`](Self::add) is
    /// refused until enough is released.
    pub fn force_add(&self, bytes: i64) {
        let Some(inner) = &self.inner else {
            return;
        };
        if let Some(reservation) = &inner.reservation {
            let used = inner.counter.get_bytes_used();
            let (reserved_before, shared_before) = reservation.split(used);
            let (reserved_after, shared_after) = reservation.split(used + bytes);
            if let Some(shared) = &inner.parent {
                shared.force_add(shared_after - shared_before);
            }
            reservation
                .charged_to
                .force_add(reserved_after - reserved_before);
        } else if let Some(parent) = &inner.parent {
            parent.force_add(bytes);
        }
        inner.counter.add(bytes);
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
            inner.mem_limit.load(Ordering::Relaxed)
        } else {
            u64::MAX
        }
    }

    /// Changes the limit of the context while it is in use. A limit lower than what the context
    /// uses refuses any growth until enough memory is released, and never takes back what was
    /// already granted. No effect on a context created with a reservation, whose limit is the one
    /// of its shared parent.
    pub fn set_mem_limit(&self, mem_limit: u64) {
        if let Some(inner) = &self.inner {
            inner.mem_limit.store(mem_limit, Ordering::Relaxed);
        }
    }

    /// Check if the memory usage exceeds the limit.
    /// Returns `false` if the memory usage exceeds the limit.
    pub fn check_memory_usage(&self) -> bool {
        if let Some(inner) = &self.inner {
            if inner.counter.get_bytes_used() as u64 > inner.mem_limit.load(Ordering::Relaxed) {
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
        if used < 0 {
            // More was released than counted: the parents count less than what is held, and
            // may grant memory that is not there.
            tracing::warn!(
                used,
                "memory context dropped with a negative usage: memory released without being counted"
            );
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lowered_limit_refuses_growth_but_not_release() {
        let root = MemoryContext::root(TrAdderAtomic::new(0), u64::MAX);
        let gate =
            MemoryContext::new_with_mem_limit(Some(root.clone()), TrAdderAtomic::new(0), 100);
        let child = MemoryContext::new(Some(gate.clone()), TrAdderAtomic::new(0));
        assert!(child.add(80));

        // Below what is already used: nothing more is granted, nothing is taken back.
        gate.set_mem_limit(50);
        assert_eq!(gate.mem_limit(), 50);
        assert!(!child.add(1));
        assert_eq!(gate.get_bytes_used(), 80);
        // Releasing always succeeds, even above the limit.
        assert!(child.add(-20));
        assert_eq!(gate.get_bytes_used(), 60);
        assert!(!child.add(1));
        assert!(child.add(-20));
        assert!(child.add(10));
        assert!(!child.add(1));

        // Back to a higher limit: growth is granted again.
        gate.set_mem_limit(100);
        assert!(child.add(40));
        assert_eq!(root.get_bytes_used(), 90);
        drop(child);
        assert_eq!(root.get_bytes_used(), 0);
    }

    #[test]
    fn test_force_add_counts_beyond_the_limits() {
        let root = MemoryContext::root(TrAdderAtomic::new(0), 100);
        let op = MemoryContext::new_with_reservation(
            root.clone(),
            root.clone(),
            30,
            TrAdderAtomic::new(0),
        );
        let other = MemoryContext::new(Some(root.clone()), TrAdderAtomic::new(0));
        assert!(op.add(50));
        op.force_add(80);
        assert_eq!(op.get_bytes_used(), 130);
        assert_eq!(root.get_bytes_used(), 130);
        // Nothing more is granted to anyone until enough is released.
        assert!(!other.add(1));
        assert!(!op.add(1));
        op.force_add(-80);
        assert_eq!(root.get_bytes_used(), 50);
        assert!(other.add(50));
        drop((op, other));
        assert_eq!(root.get_bytes_used(), 0);
    }

    #[test]
    fn test_lowered_limit_applies_to_reserved_contexts_beyond_their_reservation() {
        let root = MemoryContext::root(TrAdderAtomic::new(0), u64::MAX);
        let gate =
            MemoryContext::new_with_mem_limit(Some(root.clone()), TrAdderAtomic::new(0), 100);
        let op = MemoryContext::new_with_reservation(
            gate.clone(),
            root.clone(),
            30,
            TrAdderAtomic::new(0),
        );
        assert!(op.add(70));
        assert_eq!(gate.get_bytes_used(), 40);

        gate.set_mem_limit(0);
        // The reserved part is still there, beyond it nothing is granted.
        assert!(!op.add(1));
        assert!(op.add(-50));
        assert_eq!(gate.get_bytes_used(), 0);
        assert!(op.add(10));
        assert!(!op.add(1));
        assert_eq!(root.get_bytes_used(), 30);
    }
}
