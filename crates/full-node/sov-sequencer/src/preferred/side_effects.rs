use std::collections::VecDeque;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use sov_blob_sender::BlobExecutionStatus;
use sov_full_node_configs::sequencer::PostgresConfig;
use sov_modules_api::{ConcurrentStateCheckpoint, Runtime, Spec, StateCheckpoint};
use sov_rollup_interface::node::da::DaService;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{debug, enabled, error, info, warn, Level};

use super::executor_events::ExecutorEvent;
use crate::metrics::PreferredSequencerExecutorEventMetrics;
use crate::preferred::db::postgres::PostgresBackend;
use crate::preferred::db::{BatchToStore, DbBackend, SequencerRole};
use crate::preferred::executor_events::AcceptedTxEventContents;
use crate::preferred::transaction_subscriptions::TxResultWriter;
use crate::preferred::{
    exit_rollup, LedgerDb, PreferredBlobSender, PreferredSequencerDb, ReadBatch, ReadBlob,
    RecoveryStrategy, TxStatusManager, RECOVERY_ERROR_MESSAGE_ON_NONE_STRATEGY,
};

/// Inputs that [`SideEffectsTask`] stashes at construction so it can lazily
/// build the leader-only state (Postgres backend + blob sender) when an
/// in-process role transition arrives via [`RoleTransitionRequest::Promote`].
/// Cloned per request.
pub(super) struct LeaderConstructionDeps<Da: DaService> {
    pub da: Da,
    pub ledger_db: LedgerDb,
    pub storage_path: PathBuf,
    pub tx_status_manager: TxStatusManager<Da::Spec>,
    pub blob_processing_timeout: Duration,
    pub blob_status_channel: broadcast::Sender<BlobExecutionStatus<Da::Spec>>,
    pub postgres_config: Option<PostgresConfig>,
    pub bind_addr: SocketAddr,
}

/// Request envelope sent from the
/// [`SynchronizedSequencerState`](crate::preferred::sync_sequencer_state::SynchronizedSequencerState)
/// actor to the [`SideEffectsTask`] when an in-process role transition is
/// occurring.
pub(super) enum RoleTransitionRequest {
    /// Build leader-only state (Postgres backend + blob sender) and swap it
    /// into `self.db.backend` / `self.blob_sender`. Confirm via the embedded
    /// oneshot.
    Promote {
        confirm: oneshot::Sender<anyhow::Result<()>>,
    },
    /// Drop leader-only state. Sets `db.backend = None` and the blob sender's
    /// `inner` to `None`; the dropped `BlobSender` task gets its cancellation
    /// signal naturally when its owning struct drops.
    Demote {
        confirm: oneshot::Sender<anyhow::Result<()>>,
    },
}

/// A task that runs in the background and handles side effects of accepted transactions.
pub(super) struct SideEffectsTask<S, Rt, Da>
where
    S: Spec,
    Rt: Runtime<S>,
    Da: DaService<Spec = S::Da>,
{
    pub checkpoint_sender: watch::Sender<std::sync::Arc<ConcurrentStateCheckpoint<S>>>,
    pub blob_sender: PreferredBlobSender<Da>,
    pub db: PreferredSequencerDb,
    pub api_ledger_db: LedgerDb,
    pub executor_events_receiver: mpsc::Receiver<ExecutorEvent<S, Rt>>,
    pub shutdown_sender: watch::Sender<()>,
    pub transaction_cache: TxResultWriter<S, Rt>,
    /// Lazy-construction inputs for leader-side state. The actor sends a
    /// [`RoleTransitionRequest`] through `role_transition_rx`; the handler
    /// uses these inputs to build a fresh `PostgresBackend` + `BlobSender`.
    pub leader_construction_deps: LeaderConstructionDeps<Da>,
    /// In-process role transition requests from the sequencer state actor
    /// (see `sync_sequencer_state::SynchronizedSequencerState::process_promote_to_leader`).
    pub role_transition_rx: mpsc::Receiver<RoleTransitionRequest>,
}

impl<S, Rt, Da> SideEffectsTask<S, Rt, Da>
where
    S: Spec,
    Rt: Runtime<S>,
    Da: DaService<Spec = S::Da>,
{
    #[cfg(not(debug_assertions))]
    async fn maybe_delay_api_state_update_for_tests() {}

    #[cfg(debug_assertions)]
    async fn maybe_delay_api_state_update_for_tests() {
        const ENV_VAR: &str = "SOV_TEST_DELAY_FORCE_UPDATE_API_STATE_MS";
        let Ok(raw_ms) = std::env::var(ENV_VAR) else {
            return;
        };
        let Ok(ms) = raw_ms.parse::<u64>() else {
            warn!(%ENV_VAR, %raw_ms, "Invalid delay value, expected u64 milliseconds");
            return;
        };
        if ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        }
    }

    /// Syncs [`ApiState`]s with the latest [`StateCheckpoint`].
    #[tracing::instrument(skip_all, level = "trace")]
    fn update_api_state(&self, checkpoint: StateCheckpoint<S>) {
        // Preferred sequencer intentionally treats the latest available slot as finalized
        // for API state (soft-confirmation semantics). This differs from the standard
        // sequencer which passes the node's true finalized slot explicitly.
        let concurrent_checkpoint = ConcurrentStateCheckpoint::from_state_checkpoint(checkpoint);
        if self
            .checkpoint_sender
            .send(Arc::new(concurrent_checkpoint))
            .is_err()
        {
            debug!("Could not send checkpoint because the receiver has been dropped; this probably means the rollup is shutting down");
        }
    }

    #[tracing::instrument(skip_all, level = "trace")]
    async fn update_api_ledger(
        &self,
        ledger_reader: rockbound::cache::delta_reader::DeltaReader,
        slot_number: crate::SlotNumber,
        latest_finalized_slot_number: crate::SlotNumber,
        next_tx_number: u64,
    ) {
        let start = std::time::Instant::now();
        tracing::trace!(
            slot_number = %slot_number,
            latest_finalized_slot_number = %latest_finalized_slot_number,
            "Starting LedgerAPI storage update"
        );
        self.api_ledger_db.replace_reader(ledger_reader);
        tracing::trace!(
            time = ?start.elapsed(),
            slot_number = %slot_number,
            latest_finalized_slot_number = %latest_finalized_slot_number,
            "LedgerDb reader is replaced, sending notifications for the slot"
        );
        self.api_ledger_db.send_notifications_for_slot(slot_number);
        tracing::trace!(
            time = ?start.elapsed(),
            slot_number = %slot_number,
            latest_finalized_slot_number = %latest_finalized_slot_number,
            "LedgerAPI storage updated, notification has been sent"
        );
        self.transaction_cache.prune(next_tx_number).await;
    }

    #[tracing::instrument(skip_all, level = "trace")]
    async fn close_and_publish_current_batch(
        &mut self,
        checkpoint: StateCheckpoint<S>,
        batch: ReadBatch,
        info_to_store: BatchToStore,
    ) -> Result<()> {
        self.db.terminate_batch(info_to_store).await?;
        self.update_api_state(checkpoint);

        // Publish the batch.
        self.blob_sender
            .add_txs(batch.blob_id, batch.tx_hashes.clone())
            .await;
        self.blob_sender.publish_batch(batch).await?;

        Ok(())
    }

    async fn trigger_recovery(
        &mut self,
        batches_to_flush: Vec<ReadBlob>,
        recovery_strategy: RecoveryStrategy,
    ) -> Result<()> {
        if !batches_to_flush.is_empty() {
            match recovery_strategy {
                RecoveryStrategy::TryToSave => {
                    // Flush our batches to try to save them if we can
                    warn!(num_batches_to_replay = batches_to_flush.len(), "TryToSave recovery strategy has been configured. The currently pending soft confirmations will be flushed to the node. This may save some of the transactions, but if any are no longer valid, the sequencer will be penalised.");
                    self.blob_sender
                        .publish_blobs_for_recovery(batches_to_flush)
                        .await?;
                }
                RecoveryStrategy::None => {
                    // Shut down
                    error!(RECOVERY_ERROR_MESSAGE_ON_NONE_STRATEGY);
                    exit_rollup(&self.shutdown_sender).await;
                }
            }
        } else {
            warn!("Recovery: sequencer will now fast-forward the visible slot number, and resume normal operations when ready. There were no pending soft confirmations, so users will not be affected except for the downtime.");
        }
        Ok(())
    }

    /// Drains at least one event from the queue, batching operations when possible.
    async fn handle_executor_event(
        &mut self,
        event_queue: &mut VecDeque<ExecutorEvent<S, Rt>>,
    ) -> Result<()> {
        let queue_size_before = event_queue.len();
        let next_event = event_queue
            .pop_front()
            .expect("Tried to pop from empty event queue. This is a bug, please report it");
        let event_type: &'static str = (&next_event).into();
        let start_time = std::time::Instant::now();
        match next_event {
            ExecutorEvent::AcceptedTx(contents) => {
                let sequence_number = contents.sequence_number;
                let tx_idx_within_batch = contents.tx_idx_within_batch;
                let txs_to_insert = drain_consecutive_accepted_txs(contents, event_queue);
                if enabled!(Level::DEBUG) {
                    for tx in txs_to_insert.iter() {
                        debug!(tx_hash = %tx.accepted_tx.tx_hash, "Transaction was accepted by the sequencer");
                    }
                }
                let txs = txs_to_insert
                    .iter()
                    .map(|contents| {
                        (
                            contents.accepted_tx.tx.clone(),
                            contents.accepted_tx.tx_hash,
                        )
                    })
                    .collect();
                self.db
                    .bulk_insert_txs(txs, sequence_number, tx_idx_within_batch)
                    .await?;

                let checkpoint_ref = self.checkpoint_sender.borrow().clone();

                let mut oneshot_and_txs = Vec::with_capacity(txs_to_insert.len());
                for contents in txs_to_insert {
                    // Apply all updates in a single batch
                    checkpoint_ref.apply_tx_changes(contents.tx_changes);
                    oneshot_and_txs.push((contents.oneshot_sender, contents.accepted_tx));
                }
                // Send a notification that the checkpoint has been updated. The inner value is already concurrency safe, this just ensures that anyone
                // relying on change notifications get one. Note, however, that change notifications are not in sync with the actual changes.
                self.checkpoint_sender.send_modify(|_| {});
                // Send tx confirmations after API state is updated, then broadcast to WebSocket.
                // HTTP callers receive their response before WebSocket subscribers are notified.
                // We yield after sending to the oneshot to give the HTTP handler a chance to
                // process the response before we broadcast to WebSocket subscribers.
                for (oneshot, tx) in oneshot_and_txs {
                    let _ = oneshot.send(tx.clone());
                    self.transaction_cache.insert(tx).await;
                }
            }
            ExecutorEvent::CloseBatch {
                batch,
                checkpoint,
                forced_txs,
            } => {
                let info_to_store = BatchToStore {
                    blob_id: batch.blob_id,
                    sequence_number: batch.sequence_number,
                    visible_slot_number_after_increase: batch.visible_slot_number_after_increase,
                    visible_slots_to_advance: batch.visible_slots_to_advance,
                };
                self.close_and_publish_current_batch(checkpoint, batch, info_to_store)
                    .await?;
                for tx in forced_txs {
                    self.transaction_cache.insert(tx).await;
                }
            }
            ExecutorEvent::StartBatch {
                visible_slot_number_after_increase,
                visible_slots_to_advance,
                sequence_number,
                new_checkpoint,
                blob_id,
            } => {
                self.db
                    .start_batch(
                        visible_slot_number_after_increase,
                        visible_slots_to_advance,
                        sequence_number,
                        blob_id,
                    )
                    .await?;
                self.update_api_state(new_checkpoint);
            }
            ExecutorEvent::TriggerRecovery {
                blobs_to_flush,
                recovery_strategy,
                batch_to_close,
            } => {
                if let Some(batch) = batch_to_close {
                    let info_to_store = BatchToStore {
                        blob_id: batch.blob_id,
                        sequence_number: batch.sequence_number,
                        visible_slot_number_after_increase: batch
                            .visible_slot_number_after_increase,
                        visible_slots_to_advance: batch.visible_slots_to_advance,
                    };
                    self.db.terminate_batch(info_to_store).await?; // This batch will be included in the list to publish, so we only terminate and don't explicitly publish it
                }
                self.trigger_recovery(blobs_to_flush, recovery_strategy)
                    .await?;
            }
            ExecutorEvent::PublishProofBlob(blob_id, data, sequence_number) => {
                self.db
                    .insert_proof_blob(blob_id, data.clone(), sequence_number)
                    .await?;
                self.blob_sender
                    .publish_proof(data, sequence_number, blob_id)
                    .await?;
            }
            ExecutorEvent::ForceUpdateApiState(new_checkpoint) => {
                Self::maybe_delay_api_state_update_for_tests().await;
                self.update_api_state(new_checkpoint);
            }
            ExecutorEvent::UpdateApiLedger {
                ledger_reader,
                slot_number,
                latest_finalized_slot_number,
                next_tx_number,
            } => {
                self.update_api_ledger(
                    ledger_reader,
                    slot_number,
                    latest_finalized_slot_number,
                    next_tx_number,
                )
                .await;
            }
            ExecutorEvent::PruneDb(sequence_number) => {
                self.db.prune_db(sequence_number).await?;
            }
            ExecutorEvent::UpdateStateForRecovery(checkpoint) => {
                Self::maybe_delay_api_state_update_for_tests().await;
                self.update_api_state(checkpoint);
            }
            ExecutorEvent::FlushTransactionsCache {
                next_tx_number,
                oneshot_sender,
            } => {
                self.transaction_cache
                    .clean_and_overwrite_next_tx_number(next_tx_number)
                    .await;
                let _ = oneshot_sender.send(());
            }
        }
        let queue_size_after = event_queue.len();
        let batch_size = queue_size_before - queue_size_after;
        let duration = start_time.elapsed();
        sov_metrics::track_metrics(|t| {
            t.submit(PreferredSequencerExecutorEventMetrics {
                event_type,
                duration,
                batch_size,
            });
        });
        Ok(())
    }

    /// In-process role-transition handler (chain#435 Bug 3 Phase 3). Builds or
    /// drops the leader-only side-effects (Postgres backend, blob sender) in
    /// response to a Promote / Demote request from the
    /// [`SequencerStateUpdator`](crate::preferred::sync_sequencer_state::SequencerStateUpdator)
    /// actor. The actor flips `Inner::seq_role` atomically only after this
    /// handler confirms; otherwise a freshly-promoted "leader" would have a
    /// stale `db.backend = None` and silently fail every write.
    async fn handle_role_transition(&mut self, req: RoleTransitionRequest) {
        match req {
            RoleTransitionRequest::Promote { confirm } => {
                let outcome = self.do_promote().await;
                if let Err(ref err) = outcome {
                    error!(?err, "in-process Promote handler failed in SideEffectsTask");
                }
                let _ = confirm.send(outcome);
            }
            RoleTransitionRequest::Demote { confirm } => {
                let outcome = self.do_demote().await;
                let _ = confirm.send(outcome);
            }
        }
    }

    async fn do_promote(&mut self) -> anyhow::Result<()> {
        let deps = &self.leader_construction_deps;
        let postgres_config = deps
            .postgres_config
            .clone()
            .ok_or_else(|| anyhow::anyhow!(
                "Promote requested but no postgres_config is configured; this node was \
                 not initialized as a DbElected sequencer."
            ))?;

        // 1. Build a fresh Postgres backend. Migrations in `connect` are
        // idempotent (sqlx::migrate! is gated by _sqlx_migrations table state),
        // so this is safe even when other nodes have run them already.
        info!("Promote: connecting Postgres backend...");
        let backend = PostgresBackend::connect(&postgres_config, deps.bind_addr).await?;

        // 2. Read the completed-blob snapshot so the blob sender's recovery
        // pass has the same starting point as a leader-from-startup would.
        let snapshot = match backend.current_data().await {
            Ok(s) => s,
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "Promote: failed to read SnapshotData from new backend: {err:?}"
                ));
            }
        };
        let all_completed_blobs = snapshot.completed_blobs.clone();

        // 3. Build the blob sender's leader-side inner. Constructed with the
        // same args as `PreferredBlobSender::new`'s `BatchProducer` branch,
        // routed through `activate_leader_state` so the existing
        // `nb_of_concurrent_*` atomic counters are preserved.
        info!(
            num_blobs_for_recovery = all_completed_blobs.len(),
            "Promote: activating leader-side blob sender..."
        );
        let blob_sender_handle = self
            .blob_sender
            .activate_leader_state(
                deps.da.clone(),
                deps.ledger_db.clone(),
                all_completed_blobs,
                deps.storage_path.clone().into_boxed_path(),
                deps.tx_status_manager.clone(),
                self.shutdown_sender.clone(),
                deps.blob_processing_timeout,
                deps.blob_status_channel.clone(),
            )
            .await?;

        // 4. Hot-swap the backend into the sequencer DB.
        self.db.set_backend(Some(Box::new(backend)));

        // 5. The BlobSender task we just spawned isn't tracked in the standard
        // background_handles vec (those were collected at startup). It lives
        // until the global shutdown_sender fires. That's fine for our use case:
        // shutdown propagates through the same channel.
        let _ = blob_sender_handle;

        info!("Promote: leader-side state activated.");
        Ok(())
    }

    async fn do_demote(&mut self) -> anyhow::Result<()> {
        info!("Demote: tearing down leader-side state...");
        self.db.set_backend(None);
        self.blob_sender.deactivate_leader_state();
        info!("Demote: leader-side state torn down.");
        Ok(())
    }

    pub(crate) fn spawn(mut self) -> JoinHandle<()> {
        // We use a queue so that we can batch insert txs.
        let max_queue_size = self.executor_events_receiver.max_capacity();
        let mut event_queue = VecDeque::with_capacity(max_queue_size);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Bias toward role transitions so a Promote / Demote is
                    // handled before the next batch of executor events. This
                    // matters because the events queue can run hot and starve
                    // the actor's confirm-await for tens of seconds otherwise.
                    biased;

                    Some(req) = self.role_transition_rx.recv() => {
                        self.handle_role_transition(req).await;
                    }

                    event_opt = self.executor_events_receiver.recv() => {
                        let Some(event) = event_opt else {
                            // Sender closed — the rollup is shutting down.
                            break;
                        };
                        event_queue.push_back(event);
                        // Drain more events without blocking.
                        while event_queue.len() < max_queue_size {
                            if let Ok(event) = self.executor_events_receiver.try_recv() {
                                event_queue.push_back(event);
                            } else {
                                break;
                            }
                        }
                        while !event_queue.is_empty() {
                            if let Err(e) = self.handle_executor_event(&mut event_queue).await {
                                tracing::error!(error = ?e, "Error handling executor event");
                                let _ = self.shutdown_sender.send(());
                                break;
                            }
                        }
                    }
                }
            }
        })
    }
}

fn drain_consecutive_accepted_txs<S: Spec, Rt: Runtime<S>>(
    first_tx: AcceptedTxEventContents<S, Rt>,
    event_queue: &mut VecDeque<ExecutorEvent<S, Rt>>,
) -> Vec<AcceptedTxEventContents<S, Rt>> {
    let mut txs_to_insert = vec![first_tx];
    while let Some(next_event) = event_queue.pop_front() {
        if let ExecutorEvent::AcceptedTx(accepted) = next_event {
            txs_to_insert.push(accepted);
        } else {
            // Otherwise, put the event back and return.
            event_queue.push_front(next_event);
            break;
        }
    }
    txs_to_insert
}

#[cfg(test)]
mod tests {
    use sov_modules_api::{
        ApiTxEffect, FullyBakedTx, Gas, SuccessfulTxContents, TxChangeSet, TxHash,
    };
    use sov_test_utils::{generate_optimistic_runtime, TestSpec as S};
    use tokio::sync::oneshot;

    use crate::preferred::{AcceptedTx, Confirmation};

    generate_optimistic_runtime!(TestRuntime <= );

    use super::*;

    fn create_accepted_tx_event(number: u64) -> ExecutorEvent<S, TestRuntime<S>> {
        let tx = FullyBakedTx::new(vec![]);
        let tx_hash = TxHash::new([number as u8; 32]);
        let tx_changes = TxChangeSet {
            writes: vec![],
            reads: Default::default(),
        };
        let (sender, _) = oneshot::channel();
        let confirmation = Confirmation {
            events: vec![],
            receipt: ApiTxEffect::Successful {
                data: SuccessfulTxContents {
                    gas_used: <<S as Spec>::Gas>::zero(),
                },
            },
            tx_number: number,
            timestamp_nanos: None,
        };
        ExecutorEvent::AcceptedTx(AcceptedTxEventContents {
            accepted_tx: AcceptedTx {
                tx,
                tx_hash,
                confirmation,
            },
            tx_changes,
            oneshot_sender: sender,
            sequence_number: 0,
            tx_idx_within_batch: number,
        })
    }

    fn extract_contents(
        event: ExecutorEvent<S, TestRuntime<S>>,
    ) -> AcceptedTxEventContents<S, TestRuntime<S>> {
        {
            match event {
                ExecutorEvent::AcceptedTx(contents) => contents,
                _ => panic!("Expected AcceptedTx event"),
            }
        }
    }

    #[tokio::test]
    async fn test_drain_consecutive_accepted_txs() {
        let events = (1..1001)
            .map(create_accepted_tx_event)
            .collect::<VecDeque<_>>();
        // Test 1: draining from an empty queue
        {
            let first_event = create_accepted_tx_event(0);
            let drained_txs =
                drain_consecutive_accepted_txs(extract_contents(first_event), &mut VecDeque::new());
            // We should get back the event that we passed and no others. The queue should still be empty.
            assert_eq!(drained_txs.len(), 1);
        }

        // Test draining from a queue where the first event is not AcceptedTx
        {
            let first_event = create_accepted_tx_event(0);
            let mut event_queue = vec![ExecutorEvent::PruneDb(0)].into();
            let drained_txs =
                drain_consecutive_accepted_txs(extract_contents(first_event), &mut event_queue);
            // We should get back the event that we passed and no others. The queue should be untouched
            assert_eq!(drained_txs.len(), 1);
            assert_eq!(event_queue.len(), 1);
        }

        // Test draining from a queue where the first event is not AcceptedTx and there are other events in the queue
        {
            let first_event = create_accepted_tx_event(0);
            let second_event = create_accepted_tx_event(1);
            let mut event_queue = vec![ExecutorEvent::PruneDb(0), second_event].into();
            let drained_txs =
                drain_consecutive_accepted_txs(extract_contents(first_event), &mut event_queue);
            // We should get back the event that we passed and no others. The queue should be untouched
            assert_eq!(drained_txs.len(), 1);
            assert_eq!(event_queue.len(), 2);
        }

        // test a large queue size
        {
            let first_event = create_accepted_tx_event(0);
            let mut event_queue = events;
            // Put a non-accepted tx in the middle of the queue
            event_queue.insert(500, ExecutorEvent::PruneDb(0));
            let drained_txs =
                drain_consecutive_accepted_txs(extract_contents(first_event), &mut event_queue);
            // We should drain events at index 0..499 (so 500 of them) plus the "first" event
            assert_eq!(drained_txs.len(), 501);
            assert_eq!(event_queue.len(), 501); // There should be 501 events in the queue - one prune and the remaining 500 accept txs
            for i in 0..501 {
                assert_eq!(
                    drained_txs[i as usize].accepted_tx.confirmation.tx_number,
                    i
                );
            }

            // Drain the prune event
            event_queue.pop_front();

            // Drain the last half of the events and check correctness
            let drained_txs = drain_consecutive_accepted_txs(
                extract_contents(create_accepted_tx_event(0)),
                &mut event_queue,
            );
            assert_eq!(drained_txs.len(), 501);
            assert_eq!(event_queue.len(), 0);
            let first_event_received = drained_txs.first().unwrap();
            assert_eq!(first_event_received.accepted_tx.confirmation.tx_number, 0);
            for i in 1..501 {
                assert_eq!(
                    drained_txs[i as usize].accepted_tx.confirmation.tx_number,
                    i + 500
                );
            }
        }
    }
}
