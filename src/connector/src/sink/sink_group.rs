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

//! Ordering between the sinks of a group.
//!
//! Sinks that reference the same connection form a group. A sink of the group declared
//! `sink_order = 'last'` writes the data of an epoch only once every other sink of the group has
//! committed that epoch to the external system.
//!
//! Each sink actor of a group is a member with a *frontier*: all its data of epochs strictly lower
//! than the frontier has been committed. The frontier is advanced in memory from the items read
//! from the log store, so a sink without data advances at every barrier without writing anything
//! and never delays the `last` sinks.
//!
//! The registry is local to the process: ordering is only guaranteed between sink actors running
//! on the same compute node. The guarantee is also at-least-once: after a recovery, a sink may
//! replay epochs that a `last` sink has already written.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use parking_lot::Mutex;
use risingwave_common::hash::ActorId;
use tokio::sync::watch;

use crate::sink::catalog::SinkId;

/// Option injected by the frontend: the group of the sink, derived from its connection.
pub const SINK_GROUP_KEY: &str = "sink_group";
/// Option set by the user to order the sink inside its group.
pub const SINK_ORDER_KEY: &str = "sink_order";
pub const SINK_ORDER_LAST: &str = "last";

const WAIT_WARN_INTERVAL: Duration = Duration::from_secs(30);

type MemberKey = (SinkId, ActorId);

struct MemberState {
    /// Identifies the registration, so that a stale guard does not remove a newer registration of
    /// the same actor.
    incarnation: u64,
    is_last: bool,
    /// All data of epochs strictly lower than this value is committed.
    frontier: u64,
}

struct GroupState {
    members: Mutex<HashMap<MemberKey, MemberState>>,
    /// Bumped whenever a frontier advances or a member leaves.
    version: watch::Sender<u64>,
}

impl GroupState {
    fn new() -> Self {
        Self {
            members: Mutex::new(HashMap::new()),
            version: watch::channel(0).0,
        }
    }

    fn notify(&self) {
        self.version.send_modify(|v| *v += 1);
    }
}

#[derive(Default)]
pub struct SinkGroupRegistry {
    groups: Mutex<HashMap<String, Arc<GroupState>>>,
    next_incarnation: Mutex<u64>,
}

static GLOBAL_SINK_GROUP_REGISTRY: LazyLock<Arc<SinkGroupRegistry>> =
    LazyLock::new(|| Arc::new(SinkGroupRegistry::default()));

impl SinkGroupRegistry {
    pub fn global() -> Arc<Self> {
        GLOBAL_SINK_GROUP_REGISTRY.clone()
    }

    fn group(&self, group: &str) -> Arc<GroupState> {
        self.groups
            .lock()
            .entry(group.to_owned())
            .or_insert_with(|| Arc::new(GroupState::new()))
            .clone()
    }

    fn new_incarnation(&self) -> u64 {
        let mut next = self.next_incarnation.lock();
        *next += 1;
        *next
    }

    /// Registers a sink actor as soon as it is scheduled, before it starts, with an empty
    /// frontier: the `last` sinks of the group wait for it from then on. Must be called before
    /// any sink actor built in the same batch can consume its log.
    ///
    /// The member is removed when the returned guard is dropped.
    pub fn expect_member(
        self: &Arc<Self>,
        group: &str,
        sink_id: SinkId,
        actor_id: ActorId,
        is_last: bool,
    ) -> SinkGroupMemberGuard {
        let state = self.group(group);
        let incarnation = self.new_incarnation();
        state.members.lock().insert(
            (sink_id, actor_id),
            MemberState {
                incarnation,
                is_last,
                frontier: 0,
            },
        );
        state.notify();
        SinkGroupMemberGuard {
            registry: self.clone(),
            group: group.to_owned(),
            key: (sink_id, actor_id),
            incarnation,
        }
    }

    /// Called by the sink writer of the actor. Reuses the registration made by
    /// [`Self::expect_member`], or creates one that lives as long as the returned handle.
    pub fn join(
        self: &Arc<Self>,
        group: &str,
        sink_id: SinkId,
        actor_id: ActorId,
        is_last: bool,
    ) -> SinkGroupMember {
        let state = self.group(group);
        let key = (sink_id, actor_id);
        let exists = {
            let mut members = state.members.lock();
            match members.get_mut(&key) {
                Some(member) => {
                    member.is_last = is_last;
                    true
                }
                None => false,
            }
        };
        let own_guard = (!exists).then(|| self.expect_member(group, sink_id, actor_id, is_last));
        SinkGroupMember {
            state,
            key,
            is_last,
            _own_guard: own_guard,
        }
    }

    fn remove_member(&self, group: &str, key: MemberKey, incarnation: u64) {
        let mut groups = self.groups.lock();
        let Some(state) = groups.get(group) else {
            return;
        };
        let now_empty = {
            let mut members = state.members.lock();
            if members
                .get(&key)
                .is_some_and(|member| member.incarnation == incarnation)
            {
                members.remove(&key);
            }
            members.is_empty()
        };
        state.notify();
        if now_empty {
            groups.remove(group);
        }
    }
}

/// Removes the member from its group when dropped.
pub struct SinkGroupMemberGuard {
    registry: Arc<SinkGroupRegistry>,
    group: String,
    key: MemberKey,
    incarnation: u64,
}

impl Drop for SinkGroupMemberGuard {
    fn drop(&mut self) {
        self.registry
            .remove_member(&self.group, self.key, self.incarnation);
    }
}

/// Handle of a sink actor in its group.
pub struct SinkGroupMember {
    state: Arc<GroupState>,
    key: MemberKey,
    is_last: bool,
    _own_guard: Option<SinkGroupMemberGuard>,
}

impl SinkGroupMember {
    pub fn is_last(&self) -> bool {
        self.is_last
    }

    fn advance(&self, frontier: u64) {
        let advanced = {
            let mut members = self.state.members.lock();
            match members.get_mut(&self.key) {
                Some(member) if member.frontier < frontier => {
                    member.frontier = frontier;
                    true
                }
                _ => false,
            }
        };
        if advanced {
            self.state.notify();
        }
    }

    /// The sink starts handling data of `epoch`: all earlier epochs are committed.
    pub fn start_epoch(&self, epoch: u64) {
        self.advance(epoch);
    }

    /// All the data of `epoch` and of earlier epochs is committed.
    pub fn finish_epoch(&self, epoch: u64) {
        self.advance(epoch.saturating_add(1));
    }

    /// The lowest frontier of the other members that are not `last`, with the member holding it.
    fn others_frontier(&self) -> Option<(u64, MemberKey)> {
        self.state
            .members
            .lock()
            .iter()
            .filter(|(key, member)| **key != self.key && !member.is_last)
            .map(|(key, member)| (member.frontier, *key))
            .min()
    }

    /// Waits until every other sink of the group that is not `last` has committed `epoch`.
    /// Returns immediately for a sink that is not `last`.
    pub async fn wait_others_finished(&self, epoch: u64) {
        if !self.is_last {
            return;
        }
        let mut version = self.state.version.subscribe();
        loop {
            // Mark the current version as seen before checking, so that no update is missed.
            version.borrow_and_update();
            let Some((frontier, blocker)) = self.others_frontier() else {
                return;
            };
            if frontier > epoch {
                return;
            }
            if tokio::time::timeout(WAIT_WARN_INTERVAL, version.changed())
                .await
                .is_err()
            {
                tracing::warn!(
                    sink_id = %self.key.0,
                    actor_id = %self.key.1,
                    epoch,
                    blocking_sink_id = %blocker.0,
                    blocking_actor_id = %blocker.1,
                    blocking_frontier = frontier,
                    "last sink is waiting for another sink of its group"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::task::Poll;

    use futures::FutureExt;

    use super::*;

    fn poll_once<F: Future + Unpin>(fut: &mut F) -> Poll<F::Output> {
        fut.now_or_never().map_or(Poll::Pending, Poll::Ready)
    }

    fn registry() -> Arc<SinkGroupRegistry> {
        Arc::new(SinkGroupRegistry::default())
    }

    #[tokio::test]
    async fn test_last_waits_for_others() {
        let registry = registry();
        let a = registry.join("g", 1.into(), 1.into(), false);
        let b = registry.join("g", 2.into(), 2.into(), false);
        let last = registry.join("g", 3.into(), 3.into(), true);

        let mut wait = Box::pin(last.wait_others_finished(10));
        assert!(poll_once(&mut wait).is_pending());

        a.finish_epoch(10);
        assert!(poll_once(&mut wait).is_pending());

        // `b` reads data of a later epoch: epoch 10 is finished for it too.
        b.start_epoch(12);
        assert!(poll_once(&mut wait).is_ready());

        // `b` is still writing epoch 12.
        let mut wait = Box::pin(last.wait_others_finished(12));
        a.finish_epoch(12);
        assert!(poll_once(&mut wait).is_pending());
        b.finish_epoch(12);
        assert!(poll_once(&mut wait).is_ready());
    }

    #[tokio::test]
    async fn test_idle_sink_only_needs_barriers() {
        let registry = registry();
        let idle = registry.join("g", 1.into(), 1.into(), false);
        let last = registry.join("g", 2.into(), 2.into(), true);

        // An idle sink only sees barriers.
        idle.finish_epoch(5);
        idle.finish_epoch(6);
        last.wait_others_finished(6).now_or_never().unwrap();
        assert!(last.wait_others_finished(7).now_or_never().is_none());
    }

    #[tokio::test]
    async fn test_last_sinks_do_not_wait_for_each_other() {
        let registry = registry();
        let last1 = registry.join("g", 1.into(), 1.into(), true);
        let last2 = registry.join("g", 2.into(), 2.into(), true);
        last1.wait_others_finished(10).now_or_never().unwrap();
        last2.wait_others_finished(10).now_or_never().unwrap();

        // A sink that is not last never waits.
        let other = registry.join("g", 3.into(), 3.into(), false);
        other.wait_others_finished(10).now_or_never().unwrap();
        // Other groups are independent.
        let _other_group = registry.join("h", 4.into(), 4.into(), false);
        assert!(last1.wait_others_finished(10).now_or_never().is_none());
        other.finish_epoch(10);
        last1.wait_others_finished(10).now_or_never().unwrap();
    }

    #[tokio::test]
    async fn test_expected_member_blocks_until_it_progresses_or_leaves() {
        let registry = registry();
        let last = registry.join("g", 1.into(), 1.into(), true);

        // An actor scheduled but not started yet blocks the last sink.
        let guard = registry.expect_member("g", 2.into(), 2.into(), false);
        let mut wait = Box::pin(last.wait_others_finished(10));
        assert!(poll_once(&mut wait).is_pending());

        // Its writer reuses the registration.
        let member = registry.join("g", 2.into(), 2.into(), false);
        assert!(poll_once(&mut wait).is_pending());
        member.finish_epoch(10);
        assert!(poll_once(&mut wait).is_ready());

        // Dropping the writer keeps the registration, dropping the guard removes it.
        let mut wait = Box::pin(last.wait_others_finished(20));
        drop(member);
        assert!(poll_once(&mut wait).is_pending());
        drop(guard);
        assert!(poll_once(&mut wait).is_ready());
    }

    #[tokio::test]
    async fn test_stale_guard_does_not_remove_new_registration() {
        let registry = registry();
        let last = registry.join("g", 1.into(), 1.into(), true);

        let old_guard = registry.expect_member("g", 2.into(), 2.into(), false);
        // The actor is rebuilt (recovery) before the old one is dropped.
        let _new_guard = registry.expect_member("g", 2.into(), 2.into(), false);
        drop(old_guard);
        assert!(last.wait_others_finished(10).now_or_never().is_none());
    }

    #[tokio::test]
    async fn test_group_removed_when_empty() {
        let registry = registry();
        let member = registry.join("g", 1.into(), 1.into(), false);
        assert_eq!(registry.groups.lock().len(), 1);
        drop(member);
        assert!(registry.groups.lock().is_empty());
    }
}
