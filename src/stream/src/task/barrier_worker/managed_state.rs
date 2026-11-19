// Copyright 2025 RisingWave Labs
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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::future::{Future, pending, poll_fn};
use std::mem::replace;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use futures::future::BoxFuture;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use futures::{FutureExt, StreamExt};
use prometheus::HistogramTimer;
use risingwave_common::catalog::TableId;
use risingwave_common::id::{SinkId, SourceId};
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntCounter};
use risingwave_common::util::epoch::EpochPair;
use risingwave_pb::connector_service::SinkMetadata;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_service::barrier_complete_response::{
    IcebergPkIndexSinkMetadata as PbIcebergPkIndexSinkMetadata, PbCdcSourceOffsetUpdated,
    PbCdcTableBackfillProgress, PbCreateMviewProgress, PbListFinishedSource, PbLoadFinishedSource,
};
use risingwave_storage::StateStoreImpl;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;

use crate::error::{StreamError, StreamResult};
use crate::executor::Barrier;
use crate::executor::monitor::StreamingMetrics;
use crate::task::progress::BackfillState;
use crate::task::{
    ActorId, LocalBarrierEvent, LocalBarrierManager, NewOutputRequest, PartialGraphId,
    StreamActorManager, UpDownActorIds,
};

struct IssuedState {
    /// Actor ids remaining to be collected.
    pub remaining_actors: BTreeSet<ActorId>,

    pub barrier_inflight_latency: HistogramTimer,
}

impl Debug for IssuedState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IssuedState")
            .field("remaining_actors", &self.remaining_actors)
            .finish()
    }
}

/// The state machine of local barrier manager.
#[derive(Debug)]
enum ManagedBarrierStateInner {
    /// Meta service has issued a `send_barrier` request. We're collecting barriers now.
    Issued(IssuedState),

    /// The barrier has been collected by all remaining actors
    AllCollected {
        create_mview_progress: Vec<PbCreateMviewProgress>,
        list_finished_source_ids: Vec<PbListFinishedSource>,
        load_finished_source_ids: Vec<PbLoadFinishedSource>,
        cdc_table_backfill_progress: Vec<PbCdcTableBackfillProgress>,
        cdc_source_offset_updated: Vec<PbCdcSourceOffsetUpdated>,
        iceberg_pk_index_sink_metadata: Vec<PbIcebergPkIndexSinkMetadata>,
        truncate_tables: Vec<TableId>,
        refresh_finished_tables: Vec<TableId>,
    },
}

/// Tables that started the epoch opened by the latest barrier.
#[derive(Debug)]
struct PrevBarrierTableIds {
    epoch: EpochPair,
    /// Tables grouped by the set of epochs they started since the last checkpoint. Without
    /// selective barriers there is a single group. With selective barriers, the tables that wake
    /// up at a non-checkpoint barrier form a new group, since they did not start the previous
    /// epochs of the other tables. The uploader syncs each group separately.
    groups: Vec<HashSet<TableId>>,
    /// Whether the latest barrier was a checkpoint (or the initial barrier), i.e. every epoch
    /// before the one it opened is synced.
    synced: bool,
}

#[derive(Debug)]
struct BarrierState {
    barrier: Barrier,
    /// Only be `Some(_)` when `barrier.kind` is `Checkpoint`: the groups of tables to sync, see
    /// [`PrevBarrierTableIds::groups`].
    table_ids: Option<Vec<HashSet<TableId>>>,
    inner: ManagedBarrierStateInner,
}

use risingwave_common::must_match;
use risingwave_pb::id::FragmentId;
use risingwave_pb::stream_service::{InjectBarrierRequest, PbIcebergPkIndexSinkRole};

use crate::executor::exchange::permit;
use crate::task::barrier_worker::await_epoch_completed_future::AwaitEpochCompletedFuture;
use crate::task::barrier_worker::{ScoredStreamError, TakeReceiverRequest};
use crate::task::cdc_progress::CdcTableBackfillState;

pub(super) struct ManagedBarrierStateDebugInfo<'a> {
    running_actors: BTreeSet<ActorId>,
    graph_state: &'a PartialGraphManagedBarrierState,
}

impl Display for ManagedBarrierStateDebugInfo<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "running_actors: ")?;
        for actor_id in &self.running_actors {
            write!(f, "{}, ", actor_id)?;
        }
        {
            writeln!(f, "graph states")?;
            write!(f, "{}", self.graph_state)?;
        }
        Ok(())
    }
}

impl Display for &'_ PartialGraphManagedBarrierState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut prev_epoch = 0u64;
        for (epoch, barrier_state) in &self.epoch_barrier_state_map {
            write!(f, "> Epoch {}: ", epoch)?;
            match &barrier_state.inner {
                ManagedBarrierStateInner::Issued(state) => {
                    write!(
                        f,
                        "Issued [{:?}]. Remaining actors: [",
                        barrier_state.barrier.kind
                    )?;
                    let mut is_prev_epoch_issued = false;
                    if prev_epoch != 0 {
                        let bs = &self.epoch_barrier_state_map[&prev_epoch];
                        if let ManagedBarrierStateInner::Issued(IssuedState {
                            remaining_actors: remaining_actors_prev,
                            ..
                        }) = &bs.inner
                        {
                            // Only show the actors that are not in the previous epoch.
                            is_prev_epoch_issued = true;
                            let mut duplicates = 0usize;
                            for actor_id in &state.remaining_actors {
                                if !remaining_actors_prev.contains(actor_id) {
                                    write!(f, "{}, ", actor_id)?;
                                } else {
                                    duplicates += 1;
                                }
                            }
                            if duplicates > 0 {
                                write!(f, "...and {} actors in prev epoch", duplicates)?;
                            }
                        }
                    }
                    if !is_prev_epoch_issued {
                        for actor_id in &state.remaining_actors {
                            write!(f, "{}, ", actor_id)?;
                        }
                    }
                    write!(f, "]")?;
                }
                ManagedBarrierStateInner::AllCollected { .. } => {
                    write!(f, "AllCollected")?;
                }
            }
            prev_epoch = *epoch;
            writeln!(f)?;
        }

        if !self.create_mview_progress.is_empty() {
            writeln!(f, "Create MView Progress:")?;
            for (epoch, progress) in &self.create_mview_progress {
                write!(f, "> Epoch {}:", epoch)?;
                for (actor_id, (_, state)) in progress {
                    write!(f, ">> Actor {}: {}, ", actor_id, state)?;
                }
            }
        }

        Ok(())
    }
}

enum InflightActorStatus {
    /// The actor has been issued some barriers, but has not collected the first barrier
    IssuedFirst(Vec<Barrier>),
    /// The actor has been issued some barriers, and has collected the first barrier
    Running(u64),
}

impl InflightActorStatus {
    fn max_issued_epoch(&self) -> u64 {
        match self {
            InflightActorStatus::Running(epoch) => *epoch,
            InflightActorStatus::IssuedFirst(issued_barriers) => {
                issued_barriers.last().expect("non-empty").epoch.prev
            }
        }
    }
}

pub(crate) struct InflightActorState {
    actor_id: ActorId,
    barrier_senders: Vec<mpsc::UnboundedSender<Barrier>>,
    /// `prev_epoch`. `push_back` and `pop_front`
    pub(in crate::task) inflight_barriers: VecDeque<u64>,
    status: InflightActorStatus,
    /// Whether the actor has been issued a stop barrier
    is_stopping: bool,

    new_output_request_tx: UnboundedSender<(ActorId, NewOutputRequest)>,
    join_handle: JoinHandle<()>,
    monitor_task_handle: Option<JoinHandle<()>>,
}

impl InflightActorState {
    pub(super) fn start(
        actor_id: ActorId,
        initial_barrier: &Barrier,
        new_output_request_tx: UnboundedSender<(ActorId, NewOutputRequest)>,
        join_handle: JoinHandle<()>,
        monitor_task_handle: Option<JoinHandle<()>>,
    ) -> Self {
        Self {
            actor_id,
            barrier_senders: vec![],
            inflight_barriers: VecDeque::from_iter([initial_barrier.epoch.prev]),
            status: InflightActorStatus::IssuedFirst(vec![initial_barrier.clone()]),
            is_stopping: false,
            new_output_request_tx,
            join_handle,
            monitor_task_handle,
        }
    }

    pub(super) fn issue_barrier(&mut self, barrier: &Barrier, is_stop: bool) -> StreamResult<()> {
        assert!(barrier.epoch.prev > self.status.max_issued_epoch());

        for barrier_sender in &self.barrier_senders {
            barrier_sender.send(barrier.clone()).map_err(|_| {
                StreamError::barrier_send(
                    barrier.clone(),
                    self.actor_id,
                    "failed to send to registered sender",
                )
            })?;
        }

        if let Some(prev_epoch) = self.inflight_barriers.back() {
            assert!(*prev_epoch < barrier.epoch.prev);
        }
        self.inflight_barriers.push_back(barrier.epoch.prev);

        match &mut self.status {
            InflightActorStatus::IssuedFirst(pending_barriers) => {
                pending_barriers.push(barrier.clone());
            }
            InflightActorStatus::Running(prev_epoch) => {
                *prev_epoch = barrier.epoch.prev;
            }
        };

        if is_stop {
            assert!(!self.is_stopping, "stopped actor should not issue barrier");
            self.is_stopping = true;
        }
        Ok(())
    }

    pub(super) fn collect(&mut self, epoch: EpochPair) -> bool {
        let prev_epoch = self.inflight_barriers.pop_front().expect("should exist");
        assert_eq!(prev_epoch, epoch.prev);
        match &self.status {
            InflightActorStatus::IssuedFirst(pending_barriers) => {
                assert_eq!(
                    prev_epoch,
                    pending_barriers.first().expect("non-empty").epoch.prev
                );
                self.status = InflightActorStatus::Running(
                    pending_barriers.last().expect("non-empty").epoch.prev,
                );
            }
            InflightActorStatus::Running(_) => {}
        }

        self.inflight_barriers.is_empty() && self.is_stopping
    }
}

/// Part of [`PartialGraphState`]
pub(crate) struct PartialGraphManagedBarrierState {
    /// Record barrier state for each epoch of concurrent checkpoints.
    ///
    /// The key is `prev_epoch`, and the first value is `curr_epoch`
    epoch_barrier_state_map: BTreeMap<u64, BarrierState>,

    prev_barrier_table_ids: Option<PrevBarrierTableIds>,

    /// Record the progress updates of creating mviews for each epoch of concurrent checkpoints.
    ///
    /// The process of progress reporting is as follows:
    /// 1. updated by [`crate::task::barrier_manager::CreateMviewProgressReporter::update`]
    /// 2. converted to [`ManagedBarrierStateInner`] in [`Self::may_have_collected_all`]
    /// 3. handled by [`Self::pop_barrier_to_complete`]
    /// 4. put in [`crate::task::barrier_worker::BarrierCompleteResult`] and reported to meta.
    pub(crate) create_mview_progress: HashMap<u64, HashMap<ActorId, (FragmentId, BackfillState)>>,

    /// Record the source list finished reports for each epoch of concurrent checkpoints.
    /// Used for refreshable batch source. The map key is epoch and the value is
    /// a list of pb messages reported by actors.
    pub(crate) list_finished_source_ids: HashMap<u64, Vec<PbListFinishedSource>>,

    /// Record the source load finished reports for each epoch of concurrent checkpoints.
    /// Used for refreshable batch source. The map key is epoch and the value is
    /// a list of pb messages reported by actors.
    pub(crate) load_finished_source_ids: HashMap<u64, Vec<PbLoadFinishedSource>>,

    pub(crate) cdc_table_backfill_progress: HashMap<u64, HashMap<ActorId, CdcTableBackfillState>>,

    /// Record CDC source offset updated reports for each epoch of concurrent checkpoints.
    /// Used to track when CDC sources have updated their offset at least once.
    pub(crate) cdc_source_offset_updated: HashMap<u64, Vec<PbCdcSourceOffsetUpdated>>,

    /// Record Iceberg pk-index sink metadata reports per epoch for concurrent checkpoints.
    pub(crate) iceberg_pk_index_sink_metadata: HashMap<u64, Vec<PbIcebergPkIndexSinkMetadata>>,

    /// Record the tables to truncate for each epoch of concurrent checkpoints.
    pub(crate) truncate_tables: HashMap<u64, HashSet<TableId>>,
    /// Record the tables that have finished refresh for each epoch of concurrent checkpoints.
    /// Used for materialized view refresh completion reporting.
    pub(crate) refresh_finished_tables: HashMap<u64, HashSet<TableId>>,

    state_store: StateStoreImpl,

    barrier_inflight_latency: LabelGuardedHistogram,
    barrier_sync_latency: LabelGuardedHistogram,
    barrier_manager_progress: LabelGuardedIntCounter,
}

impl PartialGraphManagedBarrierState {
    pub(super) fn new(
        actor_manager: &StreamActorManager,
        partial_graph_id: PartialGraphId,
    ) -> Self {
        Self::new_inner(
            actor_manager.env.state_store(),
            actor_manager.streaming_metrics.clone(),
            partial_graph_id,
        )
    }

    fn new_inner(
        state_store: StateStoreImpl,
        streaming_metrics: Arc<StreamingMetrics>,
        partial_graph_id: PartialGraphId,
    ) -> Self {
        fn partial_graph_name(partial_graph_id: PartialGraphId) -> String {
            // HACK: The partial graph ID encoding is owned by meta and intentionally not exposed to
            // compute nodes. Decode it locally only for the human-readable metrics label.
            let raw_partial_graph_id = partial_graph_id.as_raw_id();
            let database_id = raw_partial_graph_id >> 32;
            let raw_job_id = raw_partial_graph_id as u32;
            if raw_job_id == u32::MAX {
                format!("database {database_id}")
            } else {
                format!("database {database_id} job {raw_job_id}")
            }
        }

        let partial_graph_name = partial_graph_name(partial_graph_id);
        let barrier_inflight_latency = streaming_metrics
            .barrier_inflight_latency
            .with_guarded_label_values(&[&partial_graph_name]);
        let barrier_sync_latency = streaming_metrics
            .barrier_sync_latency
            .with_guarded_label_values(&[&partial_graph_name]);
        let barrier_manager_progress = streaming_metrics
            .barrier_manager_progress
            .with_guarded_label_values(&[&partial_graph_name]);
        Self {
            epoch_barrier_state_map: Default::default(),
            prev_barrier_table_ids: None,
            create_mview_progress: Default::default(),
            list_finished_source_ids: Default::default(),
            load_finished_source_ids: Default::default(),
            cdc_table_backfill_progress: Default::default(),
            cdc_source_offset_updated: Default::default(),
            iceberg_pk_index_sink_metadata: Default::default(),
            truncate_tables: Default::default(),
            refresh_finished_tables: Default::default(),
            state_store,
            barrier_inflight_latency,
            barrier_sync_latency,
            barrier_manager_progress,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self::new_inner(
            StateStoreImpl::for_test(),
            Arc::new(StreamingMetrics::unused()),
            PartialGraphId::new(0),
        )
    }

    pub(super) fn is_empty(&self) -> bool {
        self.epoch_barrier_state_map.is_empty()
    }
}

pub(crate) struct SuspendedPartialGraphState {
    pub(super) suspend_time: Instant,
    inner: PartialGraphState,
    failure: Option<(Option<ActorId>, StreamError)>,
}

impl SuspendedPartialGraphState {
    fn new(
        state: PartialGraphState,
        failure: Option<(Option<ActorId>, StreamError)>,
        _completing_futures: Option<FuturesOrdered<AwaitEpochCompletedFuture>>, /* discard the completing futures */
    ) -> Self {
        Self {
            suspend_time: Instant::now(),
            inner: state,
            failure,
        }
    }

    async fn reset(mut self) -> ResetPartialGraphOutput {
        let root_err = self.inner.try_find_root_actor_failure(self.failure).await;
        self.inner.abort_and_wait_actors().await;
        ResetPartialGraphOutput { root_err }
    }
}

pub(crate) struct ResetPartialGraphOutput {
    pub(crate) root_err: Option<ScoredStreamError>,
}

pub(in crate::task) enum PartialGraphStatus {
    ReceivedExchangeRequest(Vec<(UpDownActorIds, TakeReceiverRequest)>),
    Running(PartialGraphState),
    Suspended(SuspendedPartialGraphState),
    Resetting,
    /// temporary place holder
    Unspecified,
}

impl PartialGraphStatus {
    pub(crate) async fn abort(&mut self) {
        match self {
            PartialGraphStatus::ReceivedExchangeRequest(pending_requests) => {
                for (_, request) in pending_requests.drain(..) {
                    if let TakeReceiverRequest::Remote { result_sender, .. } = request {
                        let _ = result_sender.send(Err(anyhow!("partial graph aborted").into()));
                    }
                }
            }
            PartialGraphStatus::Running(state) => {
                state.abort_and_wait_actors().await;
            }
            PartialGraphStatus::Suspended(SuspendedPartialGraphState { inner: state, .. }) => {
                state.abort_and_wait_actors().await;
            }
            PartialGraphStatus::Resetting => {}
            PartialGraphStatus::Unspecified => {
                unreachable!()
            }
        }
    }

    pub(crate) fn state_for_request(&mut self) -> Option<&mut PartialGraphState> {
        match self {
            PartialGraphStatus::ReceivedExchangeRequest(_) => {
                unreachable!("should not handle request")
            }
            PartialGraphStatus::Running(state) => Some(state),
            PartialGraphStatus::Suspended(_) => None,
            PartialGraphStatus::Resetting => {
                unreachable!("should not receive further request during cleaning")
            }
            PartialGraphStatus::Unspecified => {
                unreachable!()
            }
        }
    }

    pub(super) fn poll_next_event(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ManagedBarrierStateEvent> {
        match self {
            PartialGraphStatus::ReceivedExchangeRequest(_) => Poll::Pending,
            PartialGraphStatus::Running(state) => state.poll_next_event(cx),
            PartialGraphStatus::Suspended(_) | PartialGraphStatus::Resetting => Poll::Pending,
            PartialGraphStatus::Unspecified => {
                unreachable!()
            }
        }
    }

    pub(super) fn suspend(
        &mut self,
        failed_actor: Option<ActorId>,
        err: StreamError,
        completing_futures: Option<FuturesOrdered<AwaitEpochCompletedFuture>>,
    ) {
        let state = must_match!(replace(self, PartialGraphStatus::Unspecified), PartialGraphStatus::Running(state) => state);
        *self = PartialGraphStatus::Suspended(SuspendedPartialGraphState::new(
            state,
            Some((failed_actor, err)),
            completing_futures,
        ));
    }

    pub(super) fn start_reset(
        &mut self,
        partial_graph_id: PartialGraphId,
        completing_futures: Option<FuturesOrdered<AwaitEpochCompletedFuture>>,
        table_ids_to_clear: &mut HashSet<TableId>,
    ) -> BoxFuture<'static, ResetPartialGraphOutput> {
        match replace(self, PartialGraphStatus::Resetting) {
            PartialGraphStatus::ReceivedExchangeRequest(pending_requests) => {
                for (_, request) in pending_requests {
                    if let TakeReceiverRequest::Remote { result_sender, .. } = request {
                        let _ = result_sender.send(Err(anyhow!("partial graph reset").into()));
                    }
                }
                async move { ResetPartialGraphOutput { root_err: None } }.boxed()
            }
            PartialGraphStatus::Running(state) => {
                assert_eq!(partial_graph_id, state.partial_graph_id);
                info!(
                    %partial_graph_id,
                    "start partial graph reset from Running"
                );
                table_ids_to_clear.extend(state.table_ids.iter().copied());
                SuspendedPartialGraphState::new(state, None, completing_futures)
                    .reset()
                    .boxed()
            }
            PartialGraphStatus::Suspended(state) => {
                assert!(
                    completing_futures.is_none(),
                    "should have been clear when suspended"
                );
                assert_eq!(partial_graph_id, state.inner.partial_graph_id);
                info!(
                    %partial_graph_id,
                    suspend_elapsed = ?state.suspend_time.elapsed(),
                    "start partial graph reset after suspended"
                );
                table_ids_to_clear.extend(state.inner.table_ids.iter().copied());
                state.reset().boxed()
            }
            PartialGraphStatus::Resetting => {
                unreachable!("should not reset for twice");
            }
            PartialGraphStatus::Unspecified => {
                unreachable!()
            }
        }
    }
}

#[derive(Default)]
pub(in crate::task) struct ManagedBarrierState {
    pub(super) partial_graphs: HashMap<PartialGraphId, PartialGraphStatus>,
    pub(super) resetting_graphs:
        FuturesUnordered<JoinHandle<Vec<(PartialGraphId, ResetPartialGraphOutput)>>>,
}

pub(super) enum ManagedBarrierStateEvent {
    BarrierCollected {
        partial_graph_id: PartialGraphId,
        barrier: Barrier,
    },
    ActorError {
        partial_graph_id: PartialGraphId,
        actor_id: ActorId,
        err: StreamError,
    },
    PartialGraphsReset(Vec<(PartialGraphId, ResetPartialGraphOutput)>),
    RegisterLocalUpstreamOutput {
        actor_id: ActorId,
        upstream_actor_id: ActorId,
        upstream_partial_graph_id: PartialGraphId,
        tx: permit::Sender,
    },
    /// Selective barriers: a fragment holds input data and waits for a barrier.
    PendingInput {
        partial_graph_id: PartialGraphId,
        fragment_id: FragmentId,
    },
}

impl ManagedBarrierState {
    pub(super) fn next_event(&mut self) -> impl Future<Output = ManagedBarrierStateEvent> + '_ {
        poll_fn(|cx| {
            for graph in self.partial_graphs.values_mut() {
                if let Poll::Ready(event) = graph.poll_next_event(cx) {
                    return Poll::Ready(event);
                }
            }
            if let Poll::Ready(Some(result)) = self.resetting_graphs.poll_next_unpin(cx) {
                let outputs = result.expect("failed to join resetting future");
                for (partial_graph_id, _) in &outputs {
                    let PartialGraphStatus::Resetting = self
                        .partial_graphs
                        .remove(partial_graph_id)
                        .expect("should exist")
                    else {
                        panic!("should be resetting")
                    };
                }
                return Poll::Ready(ManagedBarrierStateEvent::PartialGraphsReset(outputs));
            }
            Poll::Pending
        })
    }
}

/// Per-partial-graph barrier state manager. Handles barriers for one specific partial graph.
/// Part of [`ManagedBarrierState`] in [`super::LocalBarrierWorker`].
///
/// See [`crate::task`] for architecture overview.
pub(crate) struct PartialGraphState {
    partial_graph_id: PartialGraphId,
    pub(crate) actor_states: HashMap<ActorId, InflightActorState>,
    pub(super) actor_pending_new_output_requests:
        HashMap<ActorId, Vec<(ActorId, NewOutputRequest)>>,

    pub(crate) graph_state: PartialGraphManagedBarrierState,

    table_ids: HashSet<TableId>,

    actor_manager: Arc<StreamActorManager>,

    pub(super) local_barrier_manager: LocalBarrierManager,

    barrier_event_rx: UnboundedReceiver<LocalBarrierEvent>,
    pub(super) actor_failure_rx: UnboundedReceiver<(ActorId, StreamError)>,
}

impl PartialGraphState {
    /// Create a barrier manager state. This will be called only once.
    pub(super) fn new(
        partial_graph_id: PartialGraphId,
        term_id: String,
        actor_manager: Arc<StreamActorManager>,
    ) -> Self {
        let (local_barrier_manager, barrier_event_rx, actor_failure_rx) =
            LocalBarrierManager::new(term_id, actor_manager.env.clone());
        Self {
            partial_graph_id,
            actor_states: Default::default(),
            actor_pending_new_output_requests: Default::default(),
            graph_state: PartialGraphManagedBarrierState::new(&actor_manager, partial_graph_id),
            table_ids: Default::default(),
            actor_manager,
            local_barrier_manager,
            barrier_event_rx,
            actor_failure_rx,
        }
    }

    pub(super) fn to_debug_info(&self) -> ManagedBarrierStateDebugInfo<'_> {
        ManagedBarrierStateDebugInfo {
            running_actors: self.actor_states.keys().cloned().collect(),
            graph_state: &self.graph_state,
        }
    }

    async fn abort_and_wait_actors(&mut self) {
        for (actor_id, state) in &self.actor_states {
            tracing::debug!("force stopping actor {}", actor_id);
            state.join_handle.abort();
            if let Some(monitor_task_handle) = &state.monitor_task_handle {
                monitor_task_handle.abort();
            }
        }

        for (actor_id, state) in self.actor_states.drain() {
            tracing::debug!("join actor {}", actor_id);
            let result = state.join_handle.await;
            assert!(result.is_ok() || result.unwrap_err().is_cancelled());
        }
    }
}

impl InflightActorState {
    pub(super) fn register_barrier_sender(
        &mut self,
        tx: mpsc::UnboundedSender<Barrier>,
    ) -> StreamResult<()> {
        match &self.status {
            InflightActorStatus::IssuedFirst(pending_barriers) => {
                for barrier in pending_barriers {
                    tx.send(barrier.clone()).map_err(|_| {
                        StreamError::barrier_send(
                            barrier.clone(),
                            self.actor_id,
                            "failed to send pending barriers to newly registered sender",
                        )
                    })?;
                }
                self.barrier_senders.push(tx);
            }
            InflightActorStatus::Running(_) => {
                unreachable!("should not register barrier sender when entering Running status")
            }
        }
        Ok(())
    }
}

impl PartialGraphState {
    pub(super) fn register_barrier_sender(
        &mut self,
        actor_id: ActorId,
        tx: mpsc::UnboundedSender<Barrier>,
    ) -> StreamResult<()> {
        self.actor_states
            .get_mut(&actor_id)
            .expect("should exist")
            .register_barrier_sender(tx)
    }
}

impl PartialGraphState {
    pub(super) fn transform_to_issued(
        &mut self,
        barrier: &Barrier,
        request: InjectBarrierRequest,
    ) -> StreamResult<()> {
        assert_eq!(self.partial_graph_id, request.partial_graph_id);
        let actor_to_stop = barrier.all_stop_actors();
        let is_stop_actor = |actor_id| {
            actor_to_stop
                .map(|actors| actors.contains(&actor_id))
                .unwrap_or(false)
        };

        let table_ids = HashSet::from_iter(request.table_ids_to_sync);
        self.table_ids.extend(table_ids.iter().cloned());

        self.graph_state.transform_to_issued(
            barrier,
            request.actor_ids_to_collect.iter().copied(),
            table_ids,
        );

        let mut new_actors = HashSet::new();
        for (node, fragment_id, actor) in
            request
                .actors_to_build
                .into_iter()
                .flat_map(|fragment_actors| {
                    let node = Arc::new(fragment_actors.node.unwrap());
                    fragment_actors
                        .actors
                        .into_iter()
                        .map(move |actor| (node.clone(), fragment_actors.fragment_id, actor))
                })
        {
            let actor_id = actor.actor_id;
            assert!(!is_stop_actor(actor_id));
            assert!(new_actors.insert(actor_id));
            assert!(request.actor_ids_to_collect.contains(&actor_id));
            let (new_output_request_tx, new_output_request_rx) = unbounded_channel();
            if let Some(pending_requests) = self.actor_pending_new_output_requests.remove(&actor_id)
            {
                for request in pending_requests {
                    let _ = new_output_request_tx.send(request);
                }
            }
            let (join_handle, monitor_join_handle) = self.actor_manager.spawn_actor(
                actor,
                fragment_id,
                node,
                self.local_barrier_manager.clone(),
                new_output_request_rx,
            );
            assert!(
                self.actor_states
                    .try_insert(
                        actor_id,
                        InflightActorState::start(
                            actor_id,
                            barrier,
                            new_output_request_tx,
                            join_handle,
                            monitor_join_handle
                        )
                    )
                    .is_ok()
            );
        }

        // Spawn a trivial join handle to be compatible with the unit test. In the unit tests that involve local barrier manager,
        // actors are spawned in the local test logic, but we assume that there is an entry for each spawned actor in ·actor_states`,
        // so under cfg!(test) we add a dummy entry for each new actor.
        if cfg!(test) {
            for &actor_id in &request.actor_ids_to_collect {
                if !self.actor_states.contains_key(&actor_id) {
                    let (tx, rx) = unbounded_channel();
                    let join_handle = self.actor_manager.runtime.spawn(async move {
                        // The rx is spawned so that tx.send() will not fail.
                        let _ = rx;
                        pending().await
                    });
                    assert!(
                        self.actor_states
                            .try_insert(
                                actor_id,
                                InflightActorState::start(actor_id, barrier, tx, join_handle, None,)
                            )
                            .is_ok()
                    );
                    new_actors.insert(actor_id);
                }
            }
        }

        // Note: it's important to issue barrier to actor after issuing to graph to ensure that
        // we call `start_epoch` on the graph before the actors receive the barrier
        for &actor_id in &request.actor_ids_to_collect {
            if new_actors.contains(&actor_id) {
                continue;
            }
            self.actor_states
                .get_mut(&actor_id)
                .unwrap_or_else(|| {
                    panic!(
                        "should exist: {} {:?}",
                        actor_id, request.actor_ids_to_collect
                    );
                })
                .issue_barrier(barrier, is_stop_actor(actor_id))?;
        }

        Ok(())
    }

    pub(super) fn new_actor_output_request(
        &mut self,
        actor_id: ActorId,
        upstream_actor_id: ActorId,
        request: TakeReceiverRequest,
    ) {
        let request = match request {
            TakeReceiverRequest::Remote {
                result_sender,
                upstream_fragment_id,
            } => {
                let upstream_fragment_id_str = upstream_fragment_id.to_string();
                let fragment_channel_buffered_bytes = self
                    .actor_manager
                    .streaming_metrics
                    .fragment_channel_buffered_bytes
                    .with_guarded_label_values(&[&upstream_fragment_id_str]);
                let (tx, rx) = permit::channel_from_config_with_metrics(
                    self.local_barrier_manager.env.global_config(),
                    permit::ChannelMetrics {
                        sender_actor_channel_buffered_bytes: fragment_channel_buffered_bytes
                            .clone(),
                        receiver_actor_channel_buffered_bytes: fragment_channel_buffered_bytes,
                    },
                );
                let _ = result_sender.send(Ok(rx));
                NewOutputRequest::Remote(tx)
            }
            TakeReceiverRequest::Local(tx) => NewOutputRequest::Local(tx),
        };
        if let Some(actor) = self.actor_states.get_mut(&upstream_actor_id) {
            let _ = actor.new_output_request_tx.send((actor_id, request));
        } else {
            self.actor_pending_new_output_requests
                .entry(upstream_actor_id)
                .or_default()
                .push((actor_id, request));
        }
    }

    /// Handles [`LocalBarrierEvent`] from [`crate::task::barrier_manager::LocalBarrierManager`].
    pub(super) fn poll_next_event(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ManagedBarrierStateEvent> {
        if let Poll::Ready(option) = self.actor_failure_rx.poll_recv(cx) {
            let (actor_id, err) = option.expect("non-empty when tx in local_barrier_manager");
            return Poll::Ready(ManagedBarrierStateEvent::ActorError {
                actor_id,
                err,
                partial_graph_id: self.partial_graph_id,
            });
        }
        // yield some pending collected epochs
        {
            if let Some(barrier) = self.graph_state.may_have_collected_all() {
                return Poll::Ready(ManagedBarrierStateEvent::BarrierCollected {
                    barrier,
                    partial_graph_id: self.partial_graph_id,
                });
            }
        }
        while let Poll::Ready(event) = self.barrier_event_rx.poll_recv(cx) {
            match event.expect("non-empty when tx in local_barrier_manager") {
                LocalBarrierEvent::ReportActorCollected { actor_id, epoch } => {
                    if let Some(barrier) = self.collect(actor_id, epoch) {
                        return Poll::Ready(ManagedBarrierStateEvent::BarrierCollected {
                            barrier,
                            partial_graph_id: self.partial_graph_id,
                        });
                    }
                }
                LocalBarrierEvent::ReportCreateProgress {
                    epoch,
                    fragment_id,
                    actor,
                    state,
                } => {
                    self.update_create_mview_progress(epoch, fragment_id, actor, state);
                }
                LocalBarrierEvent::ReportSourceListFinished {
                    epoch,
                    actor_id,
                    table_id,
                    associated_source_id,
                } => {
                    self.report_source_list_finished(
                        epoch,
                        actor_id,
                        table_id,
                        associated_source_id,
                    );
                }
                LocalBarrierEvent::ReportSourceLoadFinished {
                    epoch,
                    actor_id,
                    table_id,
                    associated_source_id,
                } => {
                    self.report_source_load_finished(
                        epoch,
                        actor_id,
                        table_id,
                        associated_source_id,
                    );
                }
                LocalBarrierEvent::RefreshFinished {
                    epoch,
                    actor_id,
                    table_id,
                    staging_table_id,
                } => {
                    self.report_refresh_finished(epoch, actor_id, table_id, staging_table_id);
                }
                LocalBarrierEvent::RegisterBarrierSender {
                    actor_id,
                    barrier_sender,
                } => {
                    if let Err(err) = self.register_barrier_sender(actor_id, barrier_sender) {
                        return Poll::Ready(ManagedBarrierStateEvent::ActorError {
                            actor_id,
                            err,
                            partial_graph_id: self.partial_graph_id,
                        });
                    }
                }
                LocalBarrierEvent::RegisterLocalUpstreamOutput {
                    actor_id,
                    upstream_actor_id,
                    upstream_partial_graph_id,
                    tx,
                } => {
                    return Poll::Ready(ManagedBarrierStateEvent::RegisterLocalUpstreamOutput {
                        actor_id,
                        upstream_actor_id,
                        upstream_partial_graph_id,
                        tx,
                    });
                }
                LocalBarrierEvent::ReportCdcTableBackfillProgress {
                    actor_id,
                    epoch,
                    state,
                } => {
                    self.update_cdc_table_backfill_progress(epoch, actor_id, state);
                }
                LocalBarrierEvent::ReportCdcSourceOffsetUpdated {
                    epoch,
                    actor_id,
                    source_id,
                } => {
                    self.report_cdc_source_offset_updated(epoch, actor_id, source_id);
                }
                LocalBarrierEvent::ReportIcebergPkIndexSinkMetadata {
                    epoch,
                    sink_id,
                    actor_id,
                    role,
                    metadata,
                } => {
                    self.report_iceberg_pk_index_sink_metadata(
                        epoch, sink_id, actor_id, role, metadata,
                    );
                }
                LocalBarrierEvent::ReportPendingInput { fragment_id } => {
                    return Poll::Ready(ManagedBarrierStateEvent::PendingInput {
                        partial_graph_id: self.partial_graph_id,
                        fragment_id,
                    });
                }
            }
        }

        debug_assert!(self.graph_state.may_have_collected_all().is_none());
        Poll::Pending
    }
}

impl PartialGraphState {
    #[must_use]
    pub(super) fn collect(&mut self, actor_id: ActorId, epoch: EpochPair) -> Option<Barrier> {
        let is_finished = self
            .actor_states
            .get_mut(&actor_id)
            .expect("should exist")
            .collect(epoch);
        if is_finished {
            let state = self.actor_states.remove(&actor_id).expect("should exist");
            if let Some(monitor_task_handle) = state.monitor_task_handle {
                monitor_task_handle.abort();
            }
        }
        self.graph_state.collect(actor_id, epoch);
        self.graph_state.may_have_collected_all()
    }

    pub(super) fn pop_barrier_to_complete(&mut self, prev_epoch: u64) -> BarrierToComplete {
        self.graph_state.pop_barrier_to_complete(prev_epoch)
    }

    /// Collect actor errors for a while and find the one that might be the root cause.
    ///
    /// Returns `None` if there's no actor error received.
    async fn try_find_root_actor_failure(
        &mut self,
        first_failure: Option<(Option<ActorId>, StreamError)>,
    ) -> Option<ScoredStreamError> {
        let mut later_errs = vec![];
        // fetch more actor errors within a timeout
        let _ = tokio::time::timeout(Duration::from_secs(3), async {
            let mut uncollected_actors: HashSet<_> = self.actor_states.keys().cloned().collect();
            if let Some((Some(failed_actor), _)) = &first_failure {
                uncollected_actors.remove(failed_actor);
            }
            while !uncollected_actors.is_empty()
                && let Some((actor_id, error)) = self.actor_failure_rx.recv().await
            {
                uncollected_actors.remove(&actor_id);
                later_errs.push(error);
            }
        })
        .await;

        first_failure
            .into_iter()
            .map(|(_, err)| err)
            .chain(later_errs)
            .map(|e| e.with_score())
            .max_by_key(|e| e.score)
    }

    /// Report that a source has finished listing for a specific epoch
    pub(super) fn report_source_list_finished(
        &mut self,
        epoch: EpochPair,
        actor_id: ActorId,
        table_id: TableId,
        associated_source_id: SourceId,
    ) {
        // Find the correct partial graph state by matching the actor's partial graph id
        if let Some(actor_state) = self.actor_states.get(&actor_id)
            && actor_state.inflight_barriers.contains(&epoch.prev)
        {
            self.graph_state
                .list_finished_source_ids
                .entry(epoch.curr)
                .or_default()
                .push(PbListFinishedSource {
                    reporter_actor_id: actor_id,
                    table_id,
                    associated_source_id,
                });
        } else {
            warn!(
                ?epoch,
                %actor_id, %table_id, %associated_source_id, "ignore source list finished"
            );
        }
    }

    /// Report that a source has finished loading for a specific epoch
    pub(super) fn report_source_load_finished(
        &mut self,
        epoch: EpochPair,
        actor_id: ActorId,
        table_id: TableId,
        associated_source_id: SourceId,
    ) {
        // Find the correct partial graph state by matching the actor's partial graph id
        if let Some(actor_state) = self.actor_states.get(&actor_id)
            && actor_state.inflight_barriers.contains(&epoch.prev)
        {
            self.graph_state
                .load_finished_source_ids
                .entry(epoch.curr)
                .or_default()
                .push(PbLoadFinishedSource {
                    reporter_actor_id: actor_id,
                    table_id,
                    associated_source_id,
                });
        } else {
            warn!(
                ?epoch,
                %actor_id, %table_id, %associated_source_id, "ignore source load finished"
            );
        }
    }

    /// Report that a CDC source has updated its offset at least once
    pub(super) fn report_cdc_source_offset_updated(
        &mut self,
        epoch: EpochPair,
        actor_id: ActorId,
        source_id: SourceId,
    ) {
        if let Some(actor_state) = self.actor_states.get(&actor_id)
            && actor_state.inflight_barriers.contains(&epoch.prev)
        {
            self.graph_state
                .cdc_source_offset_updated
                .entry(epoch.curr)
                .or_default()
                .push(PbCdcSourceOffsetUpdated {
                    reporter_actor_id: actor_id,
                    source_id,
                });
        } else {
            warn!(
                ?epoch,
                %actor_id, %source_id, "ignore cdc source offset updated"
            );
        }
    }

    /// Record a Iceberg pk-index sink metadata report.
    pub(super) fn report_iceberg_pk_index_sink_metadata(
        &mut self,
        epoch: EpochPair,
        sink_id: SinkId,
        actor_id: ActorId,
        role: PbIcebergPkIndexSinkRole,
        metadata: Option<SinkMetadata>,
    ) {
        if let Some(actor_state) = self.actor_states.get(&actor_id)
            && actor_state.inflight_barriers.contains(&epoch.prev)
        {
            self.graph_state
                .iceberg_pk_index_sink_metadata
                .entry(epoch.curr)
                .or_default()
                .push(PbIcebergPkIndexSinkMetadata {
                    sink_id,
                    reporter_actor_id: actor_id,
                    prev_epoch: epoch.prev,
                    role: role as i32,
                    metadata,
                });
        } else {
            tracing::warn!(
                ?epoch,
                %actor_id,
                %sink_id,
                ?role,
                "ignore iceberg v3 sink metadata report from non-inflight actor"
            );
        }
    }

    /// Report that a table has finished refreshing for a specific epoch
    pub(super) fn report_refresh_finished(
        &mut self,
        epoch: EpochPair,
        actor_id: ActorId,
        table_id: TableId,
        staging_table_id: TableId,
    ) {
        // Find the correct partial graph state by matching the actor's partial graph id
        let Some(actor_state) = self.actor_states.get(&actor_id) else {
            warn!(
                ?epoch,
                %actor_id, %table_id, "ignore refresh finished table: actor_state not found"
            );
            return;
        };
        if !actor_state.inflight_barriers.contains(&epoch.prev) {
            warn!(
                ?epoch,
                %actor_id,
                %table_id,
                inflight_barriers = ?actor_state.inflight_barriers,
                "ignore refresh finished table: partial_graph_id not found in inflight_barriers"
            );
            return;
        };
        self.graph_state
            .refresh_finished_tables
            .entry(epoch.curr)
            .or_default()
            .insert(table_id);
        self.graph_state
            .truncate_tables
            .entry(epoch.curr)
            .or_default()
            .insert(staging_table_id);
    }
}

impl PartialGraphManagedBarrierState {
    /// Check whether any `Issued` barrier has been collected by all actors and, if so,
    /// transition it to `AllCollected`.
    fn may_have_collected_all(&mut self) -> Option<Barrier> {
        for barrier_state in self.epoch_barrier_state_map.values_mut() {
            match &barrier_state.inner {
                ManagedBarrierStateInner::Issued(IssuedState {
                    remaining_actors, ..
                }) if remaining_actors.is_empty() => {}
                ManagedBarrierStateInner::AllCollected { .. } => {
                    continue;
                }
                ManagedBarrierStateInner::Issued(_) => {
                    break;
                }
            }

            self.barrier_manager_progress.inc();

            let create_mview_progress = self
                .create_mview_progress
                .remove(&barrier_state.barrier.epoch.curr)
                .unwrap_or_default()
                .into_iter()
                .map(|(actor, (fragment_id, state))| state.to_pb(fragment_id, actor))
                .collect();

            let list_finished_source_ids = self
                .list_finished_source_ids
                .remove(&barrier_state.barrier.epoch.curr)
                .unwrap_or_default();

            let load_finished_source_ids = self
                .load_finished_source_ids
                .remove(&barrier_state.barrier.epoch.curr)
                .unwrap_or_default();

            let cdc_table_backfill_progress = self
                .cdc_table_backfill_progress
                .remove(&barrier_state.barrier.epoch.curr)
                .unwrap_or_default()
                .into_iter()
                .map(|(actor, state)| state.to_pb(actor, barrier_state.barrier.epoch.curr))
                .collect();

            let cdc_source_offset_updated = self
                .cdc_source_offset_updated
                .remove(&barrier_state.barrier.epoch.curr)
                .unwrap_or_default();

            let iceberg_pk_index_sink_metadata = self
                .iceberg_pk_index_sink_metadata
                .remove(&barrier_state.barrier.epoch.curr)
                .unwrap_or_default();

            let truncate_tables = self
                .truncate_tables
                .remove(&barrier_state.barrier.epoch.curr)
                .unwrap_or_default()
                .into_iter()
                .collect();

            let refresh_finished_tables = self
                .refresh_finished_tables
                .remove(&barrier_state.barrier.epoch.curr)
                .unwrap_or_default()
                .into_iter()
                .collect();
            let prev_state = replace(
                &mut barrier_state.inner,
                ManagedBarrierStateInner::AllCollected {
                    create_mview_progress,
                    list_finished_source_ids,
                    load_finished_source_ids,
                    truncate_tables,
                    refresh_finished_tables,
                    cdc_table_backfill_progress,
                    cdc_source_offset_updated,
                    iceberg_pk_index_sink_metadata,
                },
            );

            must_match!(prev_state, ManagedBarrierStateInner::Issued(IssuedState {
                barrier_inflight_latency: timer,
                ..
            }) => {
                timer.observe_duration();
            });

            return Some(barrier_state.barrier.clone());
        }
        None
    }

    fn pop_barrier_to_complete(&mut self, prev_epoch: u64) -> BarrierToComplete {
        let (popped_prev_epoch, barrier_state) = self
            .epoch_barrier_state_map
            .pop_first()
            .expect("should exist");

        assert_eq!(prev_epoch, popped_prev_epoch);

        let (
            create_mview_progress,
            list_finished_source_ids,
            load_finished_source_ids,
            cdc_table_backfill_progress,
            cdc_source_offset_updated,
            iceberg_pk_index_sink_metadata,
            truncate_tables,
            refresh_finished_tables,
        ) = must_match!(barrier_state.inner, ManagedBarrierStateInner::AllCollected {
            create_mview_progress,
            list_finished_source_ids,
            load_finished_source_ids,
            truncate_tables,
            refresh_finished_tables,
            cdc_table_backfill_progress,
            cdc_source_offset_updated,
            iceberg_pk_index_sink_metadata,
        } => {
            (create_mview_progress, list_finished_source_ids, load_finished_source_ids, cdc_table_backfill_progress, cdc_source_offset_updated, iceberg_pk_index_sink_metadata, truncate_tables, refresh_finished_tables)
        });
        BarrierToComplete {
            barrier: barrier_state.barrier,
            table_ids: barrier_state.table_ids,
            create_mview_progress,
            list_finished_source_ids,
            load_finished_source_ids,
            truncate_tables,
            refresh_finished_tables,
            cdc_table_backfill_progress,
            cdc_source_offset_updated,
            iceberg_pk_index_sink_metadata,
        }
    }

    pub(super) fn barrier_sync_latency(&self) -> LabelGuardedHistogram {
        self.barrier_sync_latency.clone()
    }
}

pub(crate) struct BarrierToComplete {
    pub barrier: Barrier,
    /// Groups of tables to sync, each synced separately by the uploader.
    pub table_ids: Option<Vec<HashSet<TableId>>>,
    pub create_mview_progress: Vec<PbCreateMviewProgress>,
    pub list_finished_source_ids: Vec<PbListFinishedSource>,
    pub load_finished_source_ids: Vec<PbLoadFinishedSource>,
    pub truncate_tables: Vec<TableId>,
    pub refresh_finished_tables: Vec<TableId>,
    pub cdc_table_backfill_progress: Vec<PbCdcTableBackfillProgress>,
    pub cdc_source_offset_updated: Vec<PbCdcSourceOffsetUpdated>,
    pub iceberg_pk_index_sink_metadata: Vec<PbIcebergPkIndexSinkMetadata>,
}

impl PartialGraphManagedBarrierState {
    /// Collect a `barrier` from the actor with `actor_id`.
    pub(super) fn collect(&mut self, actor_id: impl Into<ActorId>, epoch: EpochPair) {
        let actor_id = actor_id.into();
        tracing::debug!(
            target: "events::stream::barrier::manager::collect",
            ?epoch, %actor_id, state = ?self.epoch_barrier_state_map,
            "collect_barrier",
        );

        match self.epoch_barrier_state_map.get_mut(&epoch.prev) {
            None => {
                // If the barrier's state is stashed, this occurs exclusively in scenarios where the barrier has not been
                // injected by the barrier manager, or the barrier message is blocked at the `RemoteInput` side waiting for injection.
                // Given these conditions, it's inconceivable for an actor to attempt collect at this point.
                panic!(
                    "cannot collect new actor barrier {:?} at current state: None",
                    epoch,
                )
            }
            Some(&mut BarrierState {
                ref barrier,
                inner:
                    ManagedBarrierStateInner::Issued(IssuedState {
                        ref mut remaining_actors,
                        ..
                    }),
                ..
            }) => {
                let exist = remaining_actors.remove(&actor_id);
                assert!(
                    exist,
                    "the actor doesn't exist. actor_id: {:?}, curr_epoch: {:?}",
                    actor_id, epoch.curr
                );
                assert_eq!(barrier.epoch.curr, epoch.curr);
            }
            Some(BarrierState { inner, .. }) => {
                panic!(
                    "cannot collect new actor barrier {:?} at current state: {:?}",
                    epoch, inner
                )
            }
        }
    }

    /// When the meta service issues a `send_barrier` request, call this function to transform to
    /// `Issued` and start to collect or to notify.
    pub(super) fn transform_to_issued(
        &mut self,
        barrier: &Barrier,
        actor_ids_to_collect: impl IntoIterator<Item = ActorId>,
        table_ids: HashSet<TableId>,
    ) {
        let timer = self.barrier_inflight_latency.start_timer();

        let hummock = self.state_store.as_hummock();
        let is_checkpoint = barrier.kind.is_checkpoint();
        // Tables fall asleep only at a periodic barrier that leaves fragments asleep. A barrier
        // with a mutation (e.g. dropping jobs) is delivered to every fragment, and the tables it
        // removes have sealed the previous epoch and must be synced.
        let may_sleep = barrier
            .selective
            .as_ref()
            .is_some_and(|info| !info.sleeping_fragments.is_empty() && barrier.mutation.is_none());

        // Groups of tables that start the new epoch. The uploader requires the tables synced
        // together to have started the same epochs since their last sync, so each group is
        // started separately.
        let (table_ids_to_sync, curr_groups) = match barrier.kind {
            BarrierKind::Unspecified => {
                unreachable!()
            }
            BarrierKind::Initial => {
                assert!(
                    self.prev_barrier_table_ids.is_none(),
                    "non empty table_ids at initial barrier: {:?}",
                    self.prev_barrier_table_ids
                );
                info!(epoch = ?barrier.epoch, "initialize at Initial barrier");
                (None, vec![table_ids])
            }
            BarrierKind::Barrier | BarrierKind::Checkpoint => {
                match self.prev_barrier_table_ids.take() {
                    Some(prev) if prev.epoch.curr == barrier.epoch.prev => {
                        let mut groups = prev.groups;
                        if barrier.selective.is_some() {
                            if may_sleep {
                                // Tables that started the previous epoch but do not receive
                                // this barrier fall asleep: their actors will not seal the
                                // previous epoch, which is empty for them. Abandon it and do
                                // not sync them.
                                let asleep: HashSet<TableId> = groups
                                    .iter()
                                    .flatten()
                                    .filter(|table_id| !table_ids.contains(*table_id))
                                    .copied()
                                    .collect();
                                if !asleep.is_empty() {
                                    // Meta lets a table fall asleep only right after a
                                    // checkpoint, when everything the table wrote is synced.
                                    assert!(
                                        prev.synced,
                                        "tables {:?} fall asleep at {:?} with unsynced epochs",
                                        asleep, barrier.epoch
                                    );
                                    for group in &mut groups {
                                        group.retain(|table_id| !asleep.contains(table_id));
                                    }
                                    if let Some(hummock) = hummock {
                                        hummock.abandon_epoch(prev.epoch.curr, asleep);
                                    }
                                }
                            }
                        } else if !is_checkpoint {
                            assert!(groups.len() <= 1, "{:?}", groups);
                            assert_eq!(
                                groups.first().cloned().unwrap_or_default(),
                                table_ids,
                                "table ids changed at non-checkpoint barrier {:?}",
                                barrier.epoch
                            );
                        }
                        if is_checkpoint {
                            // Everything started so far is synced by this barrier: the tables
                            // start the new epoch as a single group again.
                            (Some(groups), vec![table_ids])
                        } else {
                            // Tables waking up at this barrier did not start the previous
                            // epochs of the other tables: they form a new group.
                            let waking: HashSet<TableId> = table_ids
                                .iter()
                                .filter(|table_id| {
                                    !groups.iter().any(|group| group.contains(*table_id))
                                })
                                .copied()
                                .collect();
                            groups.push(waking);
                            (None, groups)
                        }
                    }
                    _ if is_checkpoint => {
                        debug!(epoch = ?barrier.epoch, "reinitialize at Checkpoint barrier");
                        (Some(vec![]), vec![table_ids])
                    }
                    None => {
                        info!(epoch = ?barrier.epoch, "initialize at non-checkpoint barrier");
                        (None, vec![table_ids])
                    }
                    Some(prev) => {
                        panic!(
                            "non-checkpoint barrier {:?} does not follow the previous barrier {:?}",
                            barrier.epoch, prev.epoch
                        )
                    }
                }
            }
        };
        let curr_groups: Vec<_> = curr_groups
            .into_iter()
            .filter(|group| !group.is_empty())
            .collect();
        if let Some(hummock) = hummock {
            for group in &curr_groups {
                hummock.start_epoch(barrier.epoch.curr, group.clone());
            }
        }
        self.prev_barrier_table_ids = Some(PrevBarrierTableIds {
            epoch: barrier.epoch,
            groups: curr_groups,
            synced: is_checkpoint || matches!(barrier.kind, BarrierKind::Initial),
        });
        let table_ids = table_ids_to_sync.map(|groups| {
            groups
                .into_iter()
                .filter(|group| !group.is_empty())
                .collect()
        });

        if let Some(&mut BarrierState { ref inner, .. }) =
            self.epoch_barrier_state_map.get_mut(&barrier.epoch.prev)
        {
            {
                panic!(
                    "barrier epochs{:?} state has already been `Issued`. Current state: {:?}",
                    barrier.epoch, inner
                );
            }
        };

        self.epoch_barrier_state_map.insert(
            barrier.epoch.prev,
            BarrierState {
                barrier: barrier.clone(),
                inner: ManagedBarrierStateInner::Issued(IssuedState {
                    remaining_actors: BTreeSet::from_iter(actor_ids_to_collect),
                    barrier_inflight_latency: timer,
                }),
                table_ids,
            },
        );
    }

    #[cfg(test)]
    async fn pop_next_completed_epoch(&mut self) -> u64 {
        if let Some(barrier) = self.may_have_collected_all() {
            self.pop_barrier_to_complete(barrier.epoch.prev);
            return barrier.epoch.prev;
        }
        pending().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use risingwave_common::catalog::TableId;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_pb::id::FragmentId;
    use risingwave_pb::stream_plan::barrier::BarrierKind;

    use crate::executor::{Barrier, SelectiveBarrierInfo};
    use crate::task::barrier_worker::managed_state::PartialGraphManagedBarrierState;

    #[tokio::test]
    async fn test_managed_state_add_actor() {
        let mut managed_barrier_state = PartialGraphManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(test_epoch(1));
        let barrier2 = Barrier::new_test_barrier(test_epoch(2));
        let barrier3 = Barrier::new_test_barrier(test_epoch(3));
        let actor_ids_to_collect1 = HashSet::from([1.into(), 2.into()]);
        let actor_ids_to_collect2 = HashSet::from([1.into(), 2.into()]);
        let actor_ids_to_collect3 = HashSet::from([1.into(), 2.into(), 3.into()]);
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1, HashSet::new());
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2, HashSet::new());
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3, HashSet::new());
        managed_barrier_state.collect(1, barrier1.epoch);
        managed_barrier_state.collect(2, barrier1.epoch);
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(0)
        );
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &test_epoch(1)
        );
        managed_barrier_state.collect(1, barrier2.epoch);
        managed_barrier_state.collect(1, barrier3.epoch);
        managed_barrier_state.collect(2, barrier2.epoch);
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(1)
        );
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &test_epoch(2)
        );
        managed_barrier_state.collect(2, barrier3.epoch);
        managed_barrier_state.collect(3, barrier3.epoch);
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(2)
        );
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }

    /// A selective barrier that leaves some fragment asleep.
    fn selective_barrier(epoch: u64, is_checkpoint: bool) -> Barrier {
        let mut barrier = Barrier::new_test_barrier(test_epoch(epoch));
        if !is_checkpoint {
            barrier.kind = BarrierKind::Barrier;
        }
        barrier.selective = Some(Arc::new(SelectiveBarrierInfo {
            sleeping_fragments: HashSet::from([FragmentId::new(1)]),
            ..Default::default()
        }));
        barrier
    }

    fn tables(ids: &[u32]) -> HashSet<TableId> {
        ids.iter().map(|id| TableId::new(*id)).collect()
    }

    /// Selective barriers with `checkpoint_frequency > 1`: tables that wake up between two
    /// checkpoints are synced as separate groups.
    #[tokio::test]
    async fn test_selective_table_groups_between_checkpoints() {
        let mut state = PartialGraphManagedBarrierState::for_test();
        let mut issue = |epoch: u64, is_checkpoint: bool, table_ids: &[u32]| {
            let barrier = selective_barrier(epoch, is_checkpoint);
            state.transform_to_issued(&barrier, [], tables(table_ids));
            let barrier_state = state
                .epoch_barrier_state_map
                .remove(&barrier.epoch.prev)
                .unwrap();
            let groups = state
                .prev_barrier_table_ids
                .as_ref()
                .unwrap()
                .groups
                .clone();
            (barrier_state.table_ids, groups)
        };

        // Checkpoint: nothing to sync at the first barrier.
        let (to_sync, groups) = issue(1, true, &[1, 2, 3]);
        assert_eq!(to_sync, Some(vec![]));
        assert_eq!(groups, vec![tables(&[1, 2, 3])]);
        // Right after the checkpoint, tables 2 and 3 fall asleep.
        let (to_sync, groups) = issue(2, false, &[1]);
        assert_eq!(to_sync, None);
        assert_eq!(groups, vec![tables(&[1])]);
        // Table 2 wakes up at a non-checkpoint barrier: it forms a new group.
        let (to_sync, groups) = issue(3, false, &[1, 2]);
        assert_eq!(to_sync, None);
        assert_eq!(groups, vec![tables(&[1]), tables(&[2])]);
        let (to_sync, groups) = issue(4, false, &[1, 2]);
        assert_eq!(to_sync, None);
        assert_eq!(groups, vec![tables(&[1]), tables(&[2])]);
        // Checkpoint: the two groups are synced, table 3 wakes up and is not synced. All tables
        // start the new epoch as one group.
        let (to_sync, groups) = issue(5, true, &[1, 2, 3]);
        assert_eq!(to_sync, Some(vec![tables(&[1]), tables(&[2])]));
        assert_eq!(groups, vec![tables(&[1, 2, 3])]);
        // Checkpoint right after a checkpoint: table 1 falls asleep and is not synced.
        let (to_sync, groups) = issue(6, true, &[2, 3]);
        assert_eq!(to_sync, Some(vec![tables(&[2, 3])]));
        assert_eq!(groups, vec![tables(&[2, 3])]);
    }

    /// A table must not fall asleep while it may hold unsynced data.
    #[tokio::test]
    #[should_panic(expected = "fall asleep")]
    async fn test_selective_table_cannot_sleep_between_checkpoints() {
        let mut state = PartialGraphManagedBarrierState::for_test();
        state.transform_to_issued(&selective_barrier(1, true), [], tables(&[1, 2]));
        state.transform_to_issued(&selective_barrier(2, false), [], tables(&[1, 2]));
        state.transform_to_issued(&selective_barrier(3, false), [], tables(&[1]));
    }

    #[tokio::test]
    async fn test_managed_state_stop_actor() {
        let mut managed_barrier_state = PartialGraphManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(test_epoch(1));
        let barrier2 = Barrier::new_test_barrier(test_epoch(2));
        let barrier3 = Barrier::new_test_barrier(test_epoch(3));
        let actor_ids_to_collect1 = HashSet::from([1.into(), 2.into(), 3.into(), 4.into()]);
        let actor_ids_to_collect2 = HashSet::from([1.into(), 2.into(), 3.into()]);
        let actor_ids_to_collect3 = HashSet::from([1.into(), 2.into()]);
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1, HashSet::new());
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2, HashSet::new());
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3, HashSet::new());

        managed_barrier_state.collect(1, barrier1.epoch);
        managed_barrier_state.collect(1, barrier2.epoch);
        managed_barrier_state.collect(1, barrier3.epoch);
        managed_barrier_state.collect(2, barrier1.epoch);
        managed_barrier_state.collect(2, barrier2.epoch);
        managed_barrier_state.collect(2, barrier3.epoch);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &0
        );
        managed_barrier_state.collect(3, barrier1.epoch);
        managed_barrier_state.collect(3, barrier2.epoch);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &0
        );
        managed_barrier_state.collect(4, barrier1.epoch);
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(0)
        );
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(1)
        );
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(2)
        );
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }
}
