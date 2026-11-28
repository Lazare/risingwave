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

//! The memory of the blocks that the storage iterators of one batch scan hold.
//!
//! A batch scan opens one iterator per vnode, each with one SST iterator per level, and each SST
//! iterator holds its current block plus the blocks it prefetched. These blocks are charged, at
//! their decoded size, to a memory context of the query that runs the scan ([`ScanReadMemory`]):
//! the charge is forced (the blocks are already read) and released when the iterator drops the
//! block. Blocks that the block cache also holds are charged all the same: the cache may evict
//! them at any time while the iterator keeps them alive.
//!
//! The same object decides whether the iterators may prefetch: not when the scan holds more than
//! its budget, not when the query (or the batch memory of the node) is over its limit if asked
//! to, and not while the output of the task is held back by its consumers. In the last case the
//! prefetched blocks are also dropped, through the [`PrefetchSlot`]s that the iterators register.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;
use risingwave_common::memory::MemoryContext;

use crate::hummock::block_stream::BlockStream;

/// How a batch scan reads storage, from the session variables of its query. The defaults keep
/// the behaviour of a scan without settings.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScanReadSettings {
    /// `batch_scan_prefetch_budget_mb`, in bytes: the iterators of the scan stop prefetching (and
    /// drop their prefetched blocks) while the blocks they hold exceed it. 0: no budget.
    pub prefetch_budget_bytes: u64,
    /// `batch_scan_prefetch_within_query_limit`: the iterators of the scan stop prefetching while
    /// the memory of the query, or the batch memory of the node, is over its limit.
    pub prefetch_within_query_limit: bool,
    /// `batch_scan_release_prefetch_after_ms`: once the output of the task has been held back by
    /// its consumers for this long, the iterators drop their prefetched blocks and do not prefetch
    /// until the output moves again. 0: never.
    pub release_prefetch_after_ms: u32,
    /// `batch_scan_max_open_vnode_iterators`: a scan that does not need to keep the order of the
    /// table keeps at most this many vnode iterators open at once. 0: all at once.
    pub max_open_vnode_iterators: u32,
}

/// The memory of the blocks held by the iterators of one scan. Cheap to clone.
#[derive(Clone)]
pub struct ScanReadMemory {
    inner: Arc<ScanReadInner>,
}

struct ScanReadInner {
    /// The memory context charged with the blocks, a child of the memory of the query.
    context: MemoryContext,
    settings: ScanReadSettings,
    /// The bytes charged by the scan now.
    held: AtomicUsize,
    /// The most bytes the scan held at once.
    peak: AtomicUsize,
    /// The output of the task that runs the scan is held back by its consumers.
    held_back: AtomicBool,
    /// Prefetches given up or dropped because of the memory or of a held back output.
    prefetch_fallbacks: AtomicU64,
    /// Prefetched bytes dropped while the output was held back.
    released_bytes: AtomicU64,
    /// The prefetch slots of the iterators, when prefetched blocks are released on hold back.
    slots: Mutex<SlotList>,
}

#[derive(Default)]
struct SlotList {
    slots: Vec<Weak<PrefetchSlot>>,
    /// The length at which the slots of dropped iterators are removed.
    prune_at: usize,
}

impl ScanReadMemory {
    pub fn new(context: MemoryContext, settings: ScanReadSettings) -> Self {
        Self {
            inner: Arc::new(ScanReadInner {
                context,
                settings,
                held: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                held_back: AtomicBool::new(false),
                prefetch_fallbacks: AtomicU64::new(0),
                released_bytes: AtomicU64::new(0),
                slots: Mutex::new(SlotList::default()),
            }),
        }
    }

    pub fn settings(&self) -> &ScanReadSettings {
        &self.inner.settings
    }

    /// Charges `bytes` of blocks, until the returned charge is dropped.
    pub fn charge(&self, bytes: usize) -> ScanReadCharge {
        self.add(bytes);
        ScanReadCharge {
            memory: self.clone(),
            bytes,
        }
    }

    fn add(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let held = self.inner.held.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.inner.peak.fetch_max(held, Ordering::Relaxed);
        // The blocks are already read: the charge cannot be refused.
        self.inner.context.force_add(bytes as i64);
    }

    fn sub(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.inner.held.fetch_sub(bytes, Ordering::Relaxed);
        self.inner.context.force_add(-(bytes as i64));
    }

    /// Whether the iterators of the scan may prefetch blocks now.
    pub fn may_prefetch(&self) -> bool {
        let inner = &self.inner;
        if inner.held_back.load(Ordering::Relaxed) {
            return false;
        }
        let budget = inner.settings.prefetch_budget_bytes;
        if budget > 0 && inner.held.load(Ordering::Relaxed) as u64 >= budget {
            return false;
        }
        if inner.settings.prefetch_within_query_limit && !inner.context.check_memory_usage() {
            return false;
        }
        true
    }

    /// Records that an iterator gave up a prefetch, or dropped its prefetched blocks, because
    /// [`may_prefetch`](Self::may_prefetch) said no.
    pub fn record_fallback(&self) {
        self.inner
            .prefetch_fallbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    /// The bytes of blocks that the iterators of the scan hold now.
    pub fn held_bytes(&self) -> usize {
        self.inner.held.load(Ordering::Relaxed)
    }

    /// The most bytes of blocks that the iterators of the scan held at once.
    pub fn peak_bytes(&self) -> usize {
        self.inner.peak.load(Ordering::Relaxed)
    }

    /// How many prefetches were given up or dropped because of the memory or of a held back
    /// output.
    pub fn prefetch_fallbacks(&self) -> u64 {
        self.inner.prefetch_fallbacks.load(Ordering::Relaxed)
    }

    /// The prefetched bytes dropped while the output was held back.
    pub fn released_bytes(&self) -> u64 {
        self.inner.released_bytes.load(Ordering::Relaxed)
    }

    /// Whether the iterators register their prefetched blocks so that they can be dropped while
    /// the output is held back.
    pub fn releases_prefetch(&self) -> bool {
        self.inner.settings.release_prefetch_after_ms > 0
    }

    /// A slot for the prefetched blocks of an iterator, registered so that
    /// [`hold_back`](Self::hold_back) can drop them. `None` when the scan does not release its
    /// prefetched blocks.
    pub fn new_prefetch_slot(&self) -> Option<Arc<PrefetchSlot>> {
        if !self.releases_prefetch() {
            return None;
        }
        let slot = Arc::new(PrefetchSlot::default());
        let mut list = self.inner.slots.lock();
        if list.slots.len() >= list.prune_at {
            list.slots.retain(|slot| slot.strong_count() > 0);
            list.prune_at = (list.slots.len() * 2).max(64);
        }
        list.slots.push(Arc::downgrade(&slot));
        Some(slot)
    }

    /// The output of the task is held back: drops the prefetched blocks of the iterators and stops
    /// prefetching until [`resume`](Self::resume).
    pub fn hold_back(&self) {
        self.inner.held_back.store(true, Ordering::Relaxed);
        let slots: Vec<_> = {
            let list = self.inner.slots.lock();
            list.slots.iter().filter_map(Weak::upgrade).collect()
        };
        let mut released = 0;
        for slot in slots {
            released += slot.clear();
        }
        if released > 0 {
            self.inner
                .released_bytes
                .fetch_add(released as u64, Ordering::Relaxed);
            self.record_fallback();
        }
    }

    /// The output of the task moves again: the iterators may prefetch again.
    pub fn resume(&self) {
        self.inner.held_back.store(false, Ordering::Relaxed);
    }
}

/// Bytes charged to a [`ScanReadMemory`], released on drop.
pub struct ScanReadCharge {
    memory: ScanReadMemory,
    bytes: usize,
}

impl ScanReadCharge {
    /// Changes the charged bytes to `bytes`.
    pub fn set(&mut self, bytes: usize) {
        if bytes > self.bytes {
            self.memory.add(bytes - self.bytes);
        } else {
            self.memory.sub(self.bytes - bytes);
        }
        self.bytes = bytes;
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for ScanReadCharge {
    fn drop(&mut self) {
        self.memory.sub(self.bytes);
    }
}

/// The prefetched blocks of an SST iterator while it is not using them, so that they can be
/// dropped while the output of the task is held back. The iterator takes them back when it reads.
#[derive(Default)]
pub struct PrefetchSlot {
    stream: Mutex<Option<(Box<dyn BlockStream>, Option<ScanReadCharge>)>>,
}

impl PrefetchSlot {
    pub fn put(&self, stream: Box<dyn BlockStream>, charge: Option<ScanReadCharge>) {
        *self.stream.lock() = Some((stream, charge));
    }

    pub fn take(&self) -> Option<(Box<dyn BlockStream>, Option<ScanReadCharge>)> {
        self.stream.lock().take()
    }

    /// Drops the prefetched blocks. Returns the bytes they were charged.
    fn clear(&self) -> usize {
        let taken = self.stream.lock().take();
        taken
            .and_then(|(_, charge)| charge.map(|charge| charge.bytes()))
            .unwrap_or(0)
    }
}

/// The scans of one batch task, so that the task can hold them back while its consumers do not
/// read its output.
#[derive(Default)]
pub struct TaskScanReads {
    scans: Mutex<Vec<Weak<ScanReadInner>>>,
}

impl TaskScanReads {
    /// Registers a scan of the task. Only scans that release their prefetched blocks are kept.
    pub fn register(&self, scan: &ScanReadMemory) {
        if scan.releases_prefetch() {
            let mut scans = self.scans.lock();
            scans.retain(|scan| scan.strong_count() > 0);
            scans.push(Arc::downgrade(&scan.inner));
        }
    }

    fn live(&self) -> Vec<ScanReadMemory> {
        self.scans
            .lock()
            .iter()
            .filter_map(Weak::upgrade)
            .map(|inner| ScanReadMemory { inner })
            .collect()
    }

    /// How long the output of the task may be held back before its scans drop their prefetched
    /// blocks: the shortest delay of its scans. `None` when no scan releases them.
    pub fn release_delay(&self) -> Option<Duration> {
        self.live()
            .iter()
            .map(|scan| scan.settings().release_prefetch_after_ms)
            .filter(|ms| *ms > 0)
            .min()
            .map(|ms| Duration::from_millis(ms as u64))
    }

    /// The output of the task is held back: see [`ScanReadMemory::hold_back`].
    pub fn hold_back(&self) {
        for scan in self.live() {
            scan.hold_back();
        }
    }

    /// The output of the task moves again.
    pub fn resume(&self) {
        for scan in self.live() {
            scan.resume();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use risingwave_common::metrics::LabelGuardedIntGauge;

    use super::*;
    use crate::hummock::block_stream::PrefetchBlockStream;

    fn context(limit: u64) -> MemoryContext {
        MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), limit)
    }

    #[test]
    fn test_charges_are_released_exactly() {
        let ctx = context(u64::MAX);
        let memory = ScanReadMemory::new(ctx.clone(), ScanReadSettings::default());
        let a = memory.charge(100);
        let mut b = memory.charge(50);
        assert_eq!(ctx.get_bytes_used(), 150);
        assert_eq!(memory.held_bytes(), 150);
        b.set(80);
        assert_eq!(ctx.get_bytes_used(), 180);
        b.set(10);
        assert_eq!(ctx.get_bytes_used(), 110);
        drop(a);
        assert_eq!(ctx.get_bytes_used(), 10);
        drop(b);
        assert_eq!(ctx.get_bytes_used(), 0);
        assert_eq!(memory.held_bytes(), 0);
        assert_eq!(memory.peak_bytes(), 180);
    }

    #[test]
    fn test_charge_is_forced_beyond_the_limit() {
        let ctx = context(10);
        let memory = ScanReadMemory::new(ctx.clone(), ScanReadSettings::default());
        let charge = memory.charge(100);
        assert_eq!(ctx.get_bytes_used(), 100);
        drop(charge);
        assert_eq!(ctx.get_bytes_used(), 0);
    }

    #[test]
    fn test_prefetch_stops_beyond_the_scan_budget() {
        let memory = ScanReadMemory::new(
            context(u64::MAX),
            ScanReadSettings {
                prefetch_budget_bytes: 100,
                ..Default::default()
            },
        );
        assert!(memory.may_prefetch());
        let charge = memory.charge(99);
        assert!(memory.may_prefetch());
        let more = memory.charge(1);
        assert!(!memory.may_prefetch());
        drop(more);
        assert!(memory.may_prefetch());
        drop(charge);
    }

    #[test]
    fn test_prefetch_stops_over_the_query_limit_only_when_asked() {
        let query = context(100);
        let scan_ctx = MemoryContext::new(
            Some(query.clone()),
            LabelGuardedIntGauge::test_int_gauge::<4>(),
        );
        let other = MemoryContext::new(
            Some(query.clone()),
            LabelGuardedIntGauge::test_int_gauge::<4>(),
        );
        let default = ScanReadMemory::new(scan_ctx.clone(), ScanReadSettings::default());
        let within = ScanReadMemory::new(
            scan_ctx,
            ScanReadSettings {
                prefetch_within_query_limit: true,
                ..Default::default()
            },
        );
        assert!(within.may_prefetch());
        // Another operator of the query takes it beyond its limit.
        other.force_add(150);
        assert!(default.may_prefetch());
        assert!(!within.may_prefetch());
        other.force_add(-150);
        assert!(within.may_prefetch());
    }

    #[test]
    fn test_hold_back_drops_prefetched_blocks() {
        let ctx = context(u64::MAX);
        let settings = ScanReadSettings {
            release_prefetch_after_ms: 10,
            ..Default::default()
        };
        let memory = ScanReadMemory::new(ctx.clone(), settings);
        let task = TaskScanReads::default();
        task.register(&memory);
        assert_eq!(task.release_delay(), Some(Duration::from_millis(10)));

        let slot = memory.new_prefetch_slot().unwrap();
        let stream: Box<dyn BlockStream> =
            Box::new(PrefetchBlockStream::new(VecDeque::new(), 0, None));
        slot.put(stream, Some(memory.charge(1000)));
        assert_eq!(ctx.get_bytes_used(), 1000);

        task.hold_back();
        assert!(!memory.may_prefetch());
        assert!(slot.take().is_none());
        assert_eq!(ctx.get_bytes_used(), 0);
        assert_eq!(memory.released_bytes(), 1000);

        task.resume();
        assert!(memory.may_prefetch());
    }

    #[test]
    fn test_no_slots_without_release() {
        let memory = ScanReadMemory::new(context(u64::MAX), ScanReadSettings::default());
        assert!(memory.new_prefetch_slot().is_none());
        let task = TaskScanReads::default();
        task.register(&memory);
        assert_eq!(task.release_delay(), None);
    }
}
