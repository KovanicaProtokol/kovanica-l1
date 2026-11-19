//! Stratum V1 TCP gateway listener, client connection handler, and upstream coordinator.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::RwLock;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::config::{StratumConfig, DEFAULT_PAYOUT_ADDRESS};
use crate::difficulty::{check_network_solution, check_share_hash_meets_difficulty};
use crate::job::{Job, JobManager};
use crate::protocol::{StratumNotification, StratumRequest, StratumResponse};
use crate::rpc::NodeRpcClient;
use crate::session::{parse_and_validate_username, SessionRegistry, WorkerSession};
use crate::{KOVANICA_STRATUM_ALGORITHM, KOVANICA_STRATUM_CAPABILITY};

/// Channel message to send to a connected client TCP stream.
type ClientSender = mpsc::UnboundedSender<String>;

/// Production Stratum V1 mining pool gateway.
pub struct StratumGateway {
    config: StratumConfig,
    rpc_client: NodeRpcClient,
    job_manager: Arc<JobManager>,
    session_registry: Arc<SessionRegistry>,
    active_subscribers: Arc<RwLock<HashMap<String, ClientSender>>>,
    shutdown: Arc<AtomicBool>,
}

impl StratumGateway {
    /// Create a new gateway from configuration.
    pub fn new(config: StratumConfig) -> Self {
        let rpc_client = NodeRpcClient::new(&config.rpc_url);
        Self {
            config,
            rpc_client,
            job_manager: Arc::new(JobManager::new()),
            session_registry: Arc::new(SessionRegistry::new()),
            active_subscribers: Arc::new(RwLock::new(HashMap::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Access the underlying configuration.
    pub fn config(&self) -> &StratumConfig {
        &self.config
    }

    /// Access the job manager.
    pub fn job_manager(&self) -> &Arc<JobManager> {
        &self.job_manager
    }

    /// Access the session registry.
    pub fn session_registry(&self) -> &Arc<SessionRegistry> {
        &self.session_registry
    }

    /// Signal shutdown.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Broadcast a new job notification to all active subscribed miners.
    pub fn broadcast_notify(&self, job: &Job) {
        let notif = StratumNotification::notify(
            &job.job_id,
            &job.prevhash,
            &job.coinb1_hex,
            "",
            &[],
            &format!("{:08x}", job.round),
            &format!("{:08x}", job.target_bits),
            &format!("{:08x}", job.header.timestamp),
            true,
        );
        let wire_msg = notif.to_wire_line();

        let mut subscribers = self.active_subscribers.write();
        let mut dead_sessions = Vec::new();

        for (session_id, tx) in subscribers.iter() {
            if let Err(e) = tx.send(wire_msg.clone()) {
                debug!(%session_id, %e, "miner channel closed, dropping subscriber");
                dead_sessions.push(session_id.clone());
            }
        }

        for dead in dead_sessions {
            subscribers.remove(&dead);
        }
    }

    /// Start the Stratum TCP listener and upstream background polling loop.
    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(self.config.listen_addr)
            .await
            .with_context(|| {
                format!("failed to bind TCP listener on {}", self.config.listen_addr)
            })?;

        self.run_with_listener(listener).await
    }

    /// Run the Stratum TCP listener and upstream polling loop on an existing listener.
    pub async fn run_with_listener(&self, listener: TcpListener) -> Result<()> {
        let local_addr = listener.local_addr().unwrap_or(self.config.listen_addr);

        info!(
            listen_addr = %local_addr,
            rpc_url = %self.config.rpc_url,
            chain_id = self.config.chain_id,
            difficulty = self.config.difficulty,
            "Stratum V1 pool gateway started"
        );

        // Spawn upstream template poller
        let poller_shutdown = Arc::clone(&self.shutdown);
        let poller_rpc = self.rpc_client.clone();
        let poller_jobs = Arc::clone(&self.job_manager);
        let poller_subscribers = Arc::clone(&self.active_subscribers);
        let poller_payout = self.config.payout_address.clone();
        let poll_interval = self.config.poll_interval;

        tokio::spawn(async move {
            let mut last_round = 0u64;
            let mut last_target_bits = 0u32;
            let fallback_payout =
                poller_payout.unwrap_or_else(|| DEFAULT_PAYOUT_ADDRESS.to_string());

            while !poller_shutdown.load(Ordering::Relaxed) {
                match poller_rpc.get_work(&fallback_payout).await {
                    Ok(template) => {
                        let target_bits = template.parse_target_bits().unwrap_or(0);
                        if template.round != last_round || target_bits != last_target_bits {
                            match template.to_candidate_header() {
                                Ok(header) => {
                                    last_round = template.round;
                                    last_target_bits = target_bits;

                                    let job = poller_jobs.add_template(header);
                                    info!(
                                        job_id = %job.job_id,
                                        round = job.round,
                                        target_bits = format!("0x{:08x}", job.target_bits),
                                        "new work template received from node; broadcasting to workers"
                                    );

                                    // Broadcast to active miners
                                    let notif = StratumNotification::notify(
                                        &job.job_id,
                                        &job.prevhash,
                                        &job.coinb1_hex,
                                        "",
                                        &[],
                                        &format!("{:08x}", job.round),
                                        &format!("{:08x}", job.target_bits),
                                        &format!("{:08x}", job.header.timestamp),
                                        true,
                                    );
                                    let wire_msg = notif.to_wire_line();

                                    let mut subscribers = poller_subscribers.write();
                                    let mut dead = Vec::new();
                                    for (sess_id, tx) in subscribers.iter() {
                                        if tx.send(wire_msg.clone()).is_err() {
                                            dead.push(sess_id.clone());
                                        }
                                    }
                                    for d in dead {
                                        subscribers.remove(&d);
                                    }
                                }
                                Err(e) => {
                                    warn!(%e, "failed to convert work template to candidate header");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        debug!(%e, "upstream kovanica_getWork polling failed");
                    }
                }

                tokio::time::sleep(poll_interval).await;
            }
        });

        // Accept loop
        while !self.shutdown.load(Ordering::Relaxed) {
            match listener.accept().await {
                Ok((socket, remote_addr)) => {
                    debug!(%remote_addr, "accepted new Stratum TCP client connection");
                    let session = self.session_registry.create_session(self.config.difficulty);
                    let gateway_jobs = Arc::clone(&self.job_manager);
                    let gateway_subscribers = Arc::clone(&self.active_subscribers);
                    let gateway_rpc = self.rpc_client.clone();

                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(
                            socket,
                            remote_addr,
                            session,
                            gateway_jobs,
                            gateway_subscribers,
                            gateway_rpc,
                        )
                        .await
                        {
                            debug!(%remote_addr, %e, "Stratum client connection terminated");
                        }
                    });
                }
                Err(e) => {
                    if self.shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    warn!(%e, "error accepting TCP connection");
                }
            }
        }

        Ok(())
    }

    /// Process a single miner's TCP session.
    pub async fn handle_connection(
        socket: TcpStream,
        remote_addr: SocketAddr,
        session: Arc<WorkerSession>,
        job_manager: Arc<JobManager>,
        active_subscribers: Arc<RwLock<HashMap<String, ClientSender>>>,
        rpc_client: NodeRpcClient,
    ) -> Result<()> {
        let (reader, mut writer) = socket.into_split();
        let mut buf_reader = BufReader::new(reader);

        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<String>();

        // Writer task
        let writer_handle = tokio::spawn(async move {
            while let Some(msg) = outbound_rx.recv().await {
                if let Err(e) = writer.write_all(msg.as_bytes()).await {
                    debug!(%e, "failed to write to miner socket");
                    break;
                }
                let _ = writer.flush().await;
            }
        });

        let mut line = String::new();
        while buf_reader.read_line(&mut line).await? > 0 {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                line.clear();
                continue;
            }

            debug!(%remote_addr, request = %trimmed, "inbound Stratum line");
            let req = match StratumRequest::parse_line(trimmed) {
                Ok(r) => r,
                Err(e) => {
                    warn!(%remote_addr, %e, "malformed JSON-RPC request");
                    let resp =
                        StratumResponse::invalid_params(Value::Null, "Malformed JSON-RPC request");
                    let _ = outbound_tx.send(resp.to_wire_line());
                    line.clear();
                    continue;
                }
            };

            let req_id = req.id.clone();
            match req.method.as_str() {
                // -------------------------------------------------------------
                // mining.subscribe
                // -------------------------------------------------------------
                "mining.subscribe" => {
                    // Register as active subscriber for broadcasts
                    {
                        let mut subs = active_subscribers.write();
                        subs.insert(session.session_id.clone(), outbound_tx.clone());
                    }

                    let sub_response = StratumResponse::ok(
                        req_id,
                        json!([
                            [
                                ["mining.set_difficulty", session.session_id.clone()],
                                ["mining.notify", session.session_id.clone()]
                            ],
                            session.extranonce1.clone(),
                            session.extranonce2_size,
                            {
                                "algorithm": KOVANICA_STRATUM_ALGORITHM,
                                "profile": KOVANICA_STRATUM_CAPABILITY,
                                "version": 1
                            }
                        ]),
                    );
                    let _ = outbound_tx.send(sub_response.to_wire_line());
                }

                // -------------------------------------------------------------
                // mining.authorize
                // -------------------------------------------------------------
                "mining.authorize" => {
                    let params = req.params.as_array();
                    let username = params
                        .and_then(|arr| arr.first())
                        .and_then(Value::as_str)
                        .unwrap_or_default();

                    match parse_and_validate_username(username) {
                        Ok(authorized) => {
                            info!(
                                %remote_addr,
                                session_id = %session.session_id,
                                worker = %authorized.worker_name,
                                payout = %authorized.payout_address,
                                "worker authorized"
                            );
                            session.authorize(authorized);

                            // 1. Authorize response
                            let auth_resp = StratumResponse::ok(req_id, Value::Bool(true));
                            let _ = outbound_tx.send(auth_resp.to_wire_line());

                            // 2. Set difficulty notification
                            let diff = session.get_difficulty();
                            let diff_notif = StratumNotification::set_difficulty(diff);
                            let _ = outbound_tx.send(diff_notif.to_wire_line());

                            // 3. Immediately send active job notification if available
                            if let Some(job) = job_manager.get_current_job() {
                                let job_notif = StratumNotification::notify(
                                    &job.job_id,
                                    &job.prevhash,
                                    &job.coinb1_hex,
                                    "",
                                    &[],
                                    &format!("{:08x}", job.round),
                                    &format!("{:08x}", job.target_bits),
                                    &format!("{:08x}", job.header.timestamp),
                                    true,
                                );
                                let _ = outbound_tx.send(job_notif.to_wire_line());
                            }
                        }
                        Err(err_msg) => {
                            warn!(%remote_addr, username, %err_msg, "worker authorization failed");
                            let auth_err = StratumResponse::unauthorized(req_id, &err_msg);
                            let _ = outbound_tx.send(auth_err.to_wire_line());
                        }
                    }
                }

                // -------------------------------------------------------------
                // mining.submit
                // -------------------------------------------------------------
                "mining.submit" => {
                    if !session.is_authorized() {
                        let err_resp = StratumResponse::unauthorized(
                            req_id,
                            "Unauthorized: please call mining.authorize first",
                        );
                        let _ = outbound_tx.send(err_resp.to_wire_line());
                        line.clear();
                        continue;
                    }

                    let params = req.params.as_array().cloned().unwrap_or_default();
                    if params.len() < 4 {
                        let err_resp = StratumResponse::invalid_params(
                            req_id,
                            "mining.submit requires at least [worker, job_id, extranonce2, nonce]",
                        );
                        let _ = outbound_tx.send(err_resp.to_wire_line());
                        line.clear();
                        continue;
                    }

                    let _worker_name = params[0].as_str().unwrap_or_default();
                    let job_id = params[1].as_str().unwrap_or_default();
                    let extranonce2 = params[2].as_str().unwrap_or_default();

                    // Param layout can be 4 or 5 args:
                    // 4 args: [worker, job_id, extranonce2, nonce]
                    // 5 args: [worker, job_id, extranonce2, ntime, nonce]
                    let nonce_str = if params.len() >= 5 {
                        params[4].as_str().unwrap_or_default()
                    } else {
                        params[3].as_str().unwrap_or_default()
                    };

                    let job = match job_manager.get_job(job_id) {
                        Some(j) => j,
                        None => {
                            session.record_rejected_share();
                            let _ = outbound_tx
                                .send(StratumResponse::job_not_found(req_id).to_wire_line());
                            line.clear();
                            continue;
                        }
                    };

                    // Parse nonce
                    let nonce = if nonce_str.len() <= 8 {
                        let nonce_u32 =
                            match u32::from_str_radix(nonce_str.trim_start_matches("0x"), 16) {
                                Ok(n) => n,
                                Err(_) => {
                                    session.record_rejected_share();
                                    let _ = outbound_tx.send(
                                        StratumResponse::invalid_params(
                                            req_id,
                                            "invalid hex nonce",
                                        )
                                        .to_wire_line(),
                                    );
                                    line.clear();
                                    continue;
                                }
                            };
                        let en2_u32 = u32::from_str_radix(extranonce2.trim_start_matches("0x"), 16)
                            .unwrap_or(0);
                        ((en2_u32 as u64) << 32) | (nonce_u32 as u64)
                    } else {
                        match u64::from_str_radix(nonce_str.trim_start_matches("0x"), 16) {
                            Ok(n) => n,
                            Err(_) => {
                                session.record_rejected_share();
                                let _ = outbound_tx.send(
                                    StratumResponse::invalid_params(req_id, "invalid hex nonce")
                                        .to_wire_line(),
                                );
                                line.clear();
                                continue;
                            }
                        }
                    };

                    // Check duplicate share
                    if session.check_and_record_share(job_id, extranonce2, nonce) {
                        session.record_rejected_share();
                        let _ = outbound_tx
                            .send(StratumResponse::duplicate_share(req_id).to_wire_line());
                        line.clear();
                        continue;
                    }

                    // Reconstruct header with session payout
                    let payout = session.payout_address();
                    let mut candidate_header = job.reconstruct_header(nonce, payout);
                    let pow_hash = candidate_header.compute_pow_hash();

                    // Check share difficulty target
                    let current_diff = session.get_difficulty();
                    let meets_share = check_share_hash_meets_difficulty(&pow_hash, current_diff);

                    if !meets_share {
                        session.record_rejected_share();
                        debug!(
                            %remote_addr,
                            job_id,
                            nonce,
                            pow_hash = %pow_hash,
                            difficulty = current_diff,
                            "low difficulty share rejected"
                        );
                        let _ = outbound_tx
                            .send(StratumResponse::low_difficulty_share(req_id).to_wire_line());
                        line.clear();
                        continue;
                    }

                    // Valid share!
                    session.record_accepted_share();
                    let _ = outbound_tx
                        .send(StratumResponse::ok(req_id, Value::Bool(true)).to_wire_line());

                    // Check if solution also meets full network target
                    if check_network_solution(&pow_hash, candidate_header.target_bits) {
                        session.record_block_found();
                        candidate_header.pow_hash = Some(pow_hash);

                        info!(
                            %remote_addr,
                            round = candidate_header.round,
                            nonce = candidate_header.nonce,
                            pow_hash = %pow_hash,
                            target_bits = format!("0x{:08x}", candidate_header.target_bits),
                            "VALID NETWORK BLOCK FOUND! Submitting upstream to kovanica_submitWork"
                        );

                        let upstream_rpc = rpc_client.clone();
                        tokio::spawn(async move {
                            match upstream_rpc.submit_work(&candidate_header).await {
                                Ok(true) => {
                                    info!(
                                        round = candidate_header.round,
                                        "block ACCEPTED by upstream node!"
                                    );
                                }
                                Ok(false) => {
                                    warn!(
                                        round = candidate_header.round,
                                        "block REJECTED by upstream node"
                                    );
                                }
                                Err(e) => {
                                    error!(
                                        round = candidate_header.round,
                                        %e,
                                        "upstream kovanica_submitWork RPC failed"
                                    );
                                }
                            }
                        });
                    }
                }

                // -------------------------------------------------------------
                // mining.extranonce.subscribe
                // -------------------------------------------------------------
                "mining.extranonce.subscribe" => {
                    let _ = outbound_tx
                        .send(StratumResponse::ok(req_id, Value::Bool(true)).to_wire_line());
                }

                // -------------------------------------------------------------
                // mining.suggest_difficulty / mining.suggest_target
                // -------------------------------------------------------------
                "mining.suggest_difficulty" => {
                    if let Some(diff) = req
                        .params
                        .as_array()
                        .and_then(|a| a.first())
                        .and_then(Value::as_f64)
                    {
                        if diff > 0.0 {
                            session.set_difficulty(diff);
                            let notif = StratumNotification::set_difficulty(diff);
                            let _ = outbound_tx.send(notif.to_wire_line());
                        }
                    }
                    let _ = outbound_tx
                        .send(StratumResponse::ok(req_id, Value::Bool(true)).to_wire_line());
                }

                // -------------------------------------------------------------
                // mining.ping
                // -------------------------------------------------------------
                "mining.ping" => {
                    let _ =
                        outbound_tx.send(StratumResponse::ok(req_id, json!("pong")).to_wire_line());
                }

                other => {
                    debug!(method = other, "unrecognized Stratum method");
                    let _ = outbound_tx.send(
                        StratumResponse::error(
                            req_id,
                            20,
                            &format!("Method '{other}' not supported"),
                        )
                        .to_wire_line(),
                    );
                }
            }

            line.clear();
        }

        // Clean up subscriber on disconnect
        {
            let mut subs = active_subscribers.write();
            subs.remove(&session.session_id);
        }
        writer_handle.abort();

        Ok(())
    }
}
