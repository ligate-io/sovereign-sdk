//! Heartbeat and leadership tasks for sequencer nodes.
//!
//! This module provides periodic background tasks that maintain node presence
//! in the cluster and handle leadership transitions:
//!
//! - **Leader nodes** run a heartbeat to maintain leadership; on loss the task
//!   sends a [`Message::DemoteToReplica`](crate::preferred::sync_sequencer_state::Message)
//!   and (pre-Bug-3 only) used to call `exit_rollup`.
//! - **DbElected replicas** run a heartbeat while competing for leadership; on
//!   win the task sends a [`Message::PromoteToLeader`] and (post-Bug-3)
//!   transitions in-process from the replica heartbeat loop into the leader
//!   heartbeat loop. Pre-Bug-3, it signalled the global shutdown channel and
//!   relied on systemd to restart the whole process as a leader.
//! - **Static replicas** run a heartbeat for registration only, never competing
//!   for leadership.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use sov_full_node_configs::sequencer::{ConfiguredNodeRole, PostgresConfig};
use sov_rollup_interface::node::{future_or_shutdown, FutureOrShutdownOutput};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use super::postgres::PostgresBackend;
use super::SequencerRole;

/// Object-safe handle for requesting in-process role transitions from the
/// [`SequencerStateUpdator`](crate::preferred::sync_sequencer_state::SequencerStateUpdator).
/// Implemented for `Arc<SequencerStateUpdator<S, Rt>>` in `sync_sequencer_state/updator.rs`.
///
/// The trait exists so `HeartBeatTask` (which currently has no `S: Spec, Rt: Runtime<S>`
/// generics) can carry a handle to the actor without becoming generic itself.
#[async_trait::async_trait]
pub(crate) trait RoleTransitionRequester: Send + Sync + 'static {
    /// Requests an in-process Replica → Leader transition. Blocks until the
    /// `SequencerStateUpdator` confirms or returns an error.
    async fn promote_to_leader(&self) -> Result<()>;
    /// Requests an in-process Leader → Replica transition. Blocks until the
    /// `SequencerStateUpdator` confirms or returns an error.
    async fn demote_to_replica(&self) -> Result<()>;
}

/// Manages periodic heartbeat and optional leadership election for a sequencer node.
///
/// This task maintains the node's presence in the cluster by periodically updating
/// its registration in the database. Depending on the spawn method used, it may
/// also compete for leadership.
pub struct HeartBeatTask {
    backend: PostgresBackend,
    node_id: String,
    shutdown_sender: watch::Sender<()>,
    shutdown_receiver: watch::Receiver<()>,
    postgres_config: PostgresConfig,
    heartbeat_interval: Duration,
    /// Handle for requesting in-process role transitions from the sequencer's
    /// state actor. `None` for the registration-only / static-replica path
    /// (which can never trigger a transition).
    transitioner: Option<Arc<dyn RoleTransitionRequester>>,
}

impl HeartBeatTask {
    pub async fn new(
        postgres_config: PostgresConfig,
        shutdown_sender: watch::Sender<()>,
        bind_addr: SocketAddr,
        heartbeat_interval: Duration,
        transitioner: Option<Arc<dyn RoleTransitionRequester>>,
    ) -> Result<Self> {
        let backend = PostgresBackend::connect(&postgres_config, bind_addr).await?;
        let shutdown_receiver = shutdown_sender.subscribe();

        Ok(Self {
            backend,
            node_id: postgres_config.node_id.clone(),
            shutdown_sender,
            shutdown_receiver,
            postgres_config,
            heartbeat_interval,
            transitioner,
        })
    }

    /// Spawns the heartbeat task. The returned `JoinHandle` represents the entire
    /// task's lifecycle, including any number of in-process role transitions
    /// (Replica → Leader, Leader → Replica) that occur after the initial spawn.
    ///
    /// Pre-Bug-3, this dispatched to one of three terminal spawn methods, one of
    /// which (`spawn_replica_heartbeat_task`) ended the task by signalling the
    /// global shutdown channel. Post-Bug-3, the task is a single async block that
    /// loops between leader / replica heartbeat phases via the `transitioner`
    /// trait.
    pub async fn spawn(self, seq_role: SequencerRole) -> JoinHandle<()> {
        // Static replicas (ConfiguredNodeRole::Replica) and DA-only replicas
        // never compete for leadership. Keep the registration-only path
        // unchanged; no transitions possible.
        let configured = self.postgres_config.node_role;
        let is_db_elected = configured == ConfiguredNodeRole::DbElected;
        if seq_role == SequencerRole::DaOnlyReplica {
            return self.spawn_node_registration_task();
        }
        if seq_role == SequencerRole::PgSyncReplica && !is_db_elected {
            assert_eq!(configured, ConfiguredNodeRole::Replica);
            return self.spawn_node_registration_task();
        }

        tokio::spawn(async move {
            let mut role = seq_role;
            loop {
                match role {
                    SequencerRole::BatchProducer => match self.run_leader_loop().await {
                        LoopOutcome::Shutdown => return,
                        LoopOutcome::TransitionRequested => {
                            // Leader lost its lock (or DB call failed). Ask the
                            // actor to demote us in-process. If we have no
                            // transitioner OR demotion fails, fall back to the
                            // pre-Bug-3 behavior of exiting the rollup.
                            if let Some(transitioner) = self.transitioner.as_ref() {
                                match transitioner.demote_to_replica().await {
                                    Ok(()) => {
                                        info!(
                                            node_id = %self.node_id,
                                            "in-process demotion succeeded; continuing as replica."
                                        );
                                        role = SequencerRole::PgSyncReplica;
                                        continue;
                                    }
                                    Err(err) => {
                                        error!(
                                            node_id = %self.node_id,
                                            ?err,
                                            "in-process demotion failed; falling back to process exit."
                                        );
                                    }
                                }
                            } else {
                                error!(
                                    node_id = %self.node_id,
                                    "leadership lost but no transitioner is wired in; \
                                     falling back to pre-Bug-3 process-exit behavior."
                                );
                            }
                            crate::preferred::exit_rollup(&self.shutdown_sender).await;
                            return;
                        }
                    },
                    SequencerRole::PgSyncReplica => match self.run_replica_loop().await {
                        LoopOutcome::Shutdown => return,
                        LoopOutcome::TransitionRequested => {
                            if let Some(transitioner) = self.transitioner.as_ref() {
                                match transitioner.promote_to_leader().await {
                                    Ok(()) => {
                                        info!(
                                            node_id = %self.node_id,
                                            "in-process promotion succeeded; continuing as leader."
                                        );
                                        role = SequencerRole::BatchProducer;
                                        continue;
                                    }
                                    Err(err) => {
                                        // Promotion is recoverable: if the actor
                                        // rejected our request (e.g. mid-shutdown,
                                        // or Phase 3+ side-effects failure), back
                                        // off and try the lock again next tick.
                                        // The Postgres lock is still held by us;
                                        // another replica won't take over while
                                        // we hold it.
                                        warn!(
                                            node_id = %self.node_id,
                                            ?err,
                                            "in-process promotion failed; will retry on next heartbeat tick."
                                        );
                                        continue;
                                    }
                                }
                            } else {
                                // Pre-Bug-3 behavior path (no transitioner wired): signal shutdown
                                // so systemd restarts us as a leader. Should be unreachable in
                                // post-Bug-3 deployments.
                                error!(
                                    node_id = %self.node_id,
                                    "Replica acquired leadership but no transitioner is wired in; \
                                     falling back to pre-Bug-3 'exit to restart as leader' behavior."
                                );
                                let _ = self.shutdown_sender.send(());
                                return;
                            }
                        }
                    },
                    SequencerRole::DaOnlyReplica => {
                        // Reached only if someone calls a transition that flips to DaOnly,
                        // which currently nothing does. Treat as a registration loop.
                        self.run_registration_loop().await;
                        return;
                    }
                }
            }
        })
    }

    // Sends a heartbeat that competes for leadership.
    // Returns `true` if this node is the current leader.
    async fn try_acquire_leadership(&self) -> Result<bool> {
        match self
            .backend
            .heartbeat(Some(self.postgres_config.leader_election))
            .await?
        {
            Some(leader) => Ok(leader.node_id == self.node_id),
            None => Ok(false),
        }
    }

    // Sends a heartbeat that only updates node registration (no leadership competition).
    async fn register_node(&self) -> Result<()> {
        self.backend.heartbeat(None).await?;
        Ok(())
    }

    /// Runs the leader heartbeat loop until shutdown or leadership loss.
    /// Caller in `spawn` reacts to the returned [`LoopOutcome`] by either exiting
    /// the task (`Shutdown`) or requesting an in-process role transition
    /// (`TransitionRequested`).
    async fn run_leader_loop(&self) -> LoopOutcome {
        info!(
            node_id = %self.node_id,
            address = %self.backend.node_address,
            "Starting leader heartbeat loop"
        );
        let mut interval = tokio::time::interval(self.heartbeat_interval);

        loop {
            match future_or_shutdown(interval.tick(), &self.shutdown_receiver).await {
                FutureOrShutdownOutput::Shutdown => {
                    info!("Shutdown signal received, stopping leader heartbeat loop");
                    return LoopOutcome::Shutdown;
                }
                FutureOrShutdownOutput::Output(_) => match self.try_acquire_leadership().await {
                    Ok(true) => {
                        tracing::trace!("Leadership heartbeat successful.");
                    }
                    Ok(false) => {
                        error!(
                            node_id = %self.node_id,
                            "Leadership lost! Another node has taken over. Requesting in-process demotion."
                        );
                        return LoopOutcome::TransitionRequested;
                    }
                    Err(e) => {
                        error!(
                            node_id = %self.node_id,
                            error = ?e,
                            "Heartbeat error! Unable to communicate with database. Requesting in-process demotion."
                        );
                        return LoopOutcome::TransitionRequested;
                    }
                },
            }
        }
    }

    /// Runs the replica election loop until shutdown or leadership acquisition.
    /// Caller in `spawn` reacts to the returned [`LoopOutcome`] by either exiting
    /// the task (`Shutdown`) or requesting an in-process role transition
    /// (`TransitionRequested`).
    async fn run_replica_loop(&self) -> LoopOutcome {
        info!(
            node_id = %self.node_id,
            address = %self.backend.node_address,
            "Starting replica election loop"
        );
        let mut interval = tokio::time::interval(self.heartbeat_interval);

        loop {
            match future_or_shutdown(interval.tick(), &self.shutdown_receiver).await {
                FutureOrShutdownOutput::Shutdown => {
                    info!("Shutdown signal received, stopping replica election loop.");
                    return LoopOutcome::Shutdown;
                }
                FutureOrShutdownOutput::Output(_) => match self.try_acquire_leadership().await {
                    Ok(true) => {
                        info!(
                            node_id = %self.node_id,
                            "Replica acquired leadership! Requesting in-process promotion."
                        );
                        return LoopOutcome::TransitionRequested;
                    }
                    Ok(false) => {
                        tracing::trace!(
                            "Leadership acquisition failed, another node is leader."
                        );
                    }
                    Err(e) => {
                        warn!(
                            node_id = %self.node_id,
                            error = ?e,
                            "Election attempt failed, will retry."
                        );
                    }
                },
            }
        }
    }

    /// Runs the registration-only loop (for static replicas / DA-only replicas).
    /// Never returns until shutdown; this role does not participate in elections.
    async fn run_registration_loop(&self) {
        info!(
            node_id = %self.node_id,
            address = %self.backend.node_address,
            "Starting replica registration task."
        );
        let mut interval = tokio::time::interval(self.heartbeat_interval);

        loop {
            match future_or_shutdown(interval.tick(), &self.shutdown_receiver).await {
                FutureOrShutdownOutput::Shutdown => {
                    info!("Shutdown signal received, stopping registration task.");
                    return;
                }
                FutureOrShutdownOutput::Output(_) => match self.register_node().await {
                    Ok(_) => {}
                    Err(e) => {
                        warn!(
                            node_id = %self.node_id,
                            error = ?e,
                            "Node registration attempt failed, will retry."
                        );
                    }
                },
            }
        }
    }

    // Backwards-compat wrapper kept for the static-replica path. The new
    // `spawn` dispatches to `run_registration_loop` directly via the role
    // table; this helper exists so the small-replica path stays a single
    // `tokio::spawn` call.
    fn spawn_node_registration_task(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            self.run_registration_loop().await;
        })
    }
}

/// Result of running one phase of the heartbeat loop. The outer `spawn` task
/// uses this to decide whether to exit cleanly (`Shutdown`) or transition to
/// the symmetric phase (`TransitionRequested`).
#[derive(Debug)]
enum LoopOutcome {
    /// Global shutdown signal received; the task should exit.
    Shutdown,
    /// The election state changed (replica won the lock, or leader lost it).
    /// The outer task should request the in-process role transition via the
    /// `transitioner` handle and then re-enter the symmetric loop.
    TransitionRequested,
}
