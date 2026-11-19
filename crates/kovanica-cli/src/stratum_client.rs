//! Stratum V1 mining client for connecting to a Kovanica Stratum mining pool.
//!
//! Complies with the Stratum V1 line-delimited JSON specification in `docs/MINING.md`:
//! - Connects over TCP to a Stratum pool bridge (`mining.subscribe`, `mining.authorize`).
//! - Handles difficulty notifications (`mining.set_difficulty`) and new work jobs (`mining.notify`).
//! - Spawns multithreaded CPU workers utilizing pre-allocated 2 MiB `KovanicaHasher` contexts.
//! - Submits shares satisfying pool share difficulty via `mining.submit`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use kovanica_crypto::KovanicaHasher;
use kovanica_types::block::compact_to_target;
use kovanica_types::Hash;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::miner::{format_hashrate, MiningStats};

/// PoW algorithm required by the Kovanica Stratum V1 job profile.
const KOVANICA_STRATUM_ALGORITHM: &str = "kovanicahash";
/// Capability token advertised by Kovanica-compatible gateways during `mining.subscribe`.
const KOVANICA_STRATUM_CAPABILITY: &str = "kovanicahash-v1";

/// Stratum share submission message sent from worker threads.
#[derive(Debug, Clone)]
pub struct StratumShare {
    pub job_id: String,
    pub nonce: u64,
    pub ntime: u64,
    pub extranonce2: String,
    pub pow_hash: Hash,
    pub meets_network_target: bool,
}

/// Active work job distributed to mining worker threads.
#[derive(Debug, Clone)]
pub struct ActiveStratumJob {
    pub version: u64,
    pub job_id: String,
    pub preimage: Vec<u8>,
    pub share_target: [u8; 32],
    pub network_target: [u8; 32],
    pub ntime: u64,
    pub extranonce2: String,
}

/// Controller for Stratum V1 pool mining.
pub struct StratumMiner {
    endpoint: String,
    address: String,
    worker: String,
    password: String,
    chain_id: u64,
    threads: usize,
    stats: Arc<MiningStats>,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<tokio::sync::Notify>,
}

impl StratumMiner {
    /// Create a new Stratum miner controller.
    pub fn new(
        endpoint: String,
        address: String,
        worker: String,
        password: String,
        chain_id: u64,
        threads: usize,
    ) -> Self {
        let threads = if threads == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            threads
        };

        Self {
            endpoint,
            address,
            worker,
            password,
            chain_id,
            threads,
            stats: Arc::new(MiningStats::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Access mining stats.
    pub fn stats(&self) -> Arc<MiningStats> {
        self.stats.clone()
    }

    /// Shutdown handle.
    pub fn shutdown_handle(&self) -> Arc<AtomicBool> {
        self.shutdown.clone()
    }

    /// Stop mining gracefully.
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();
    }

    /// Clean endpoint string by stripping URL schemes.
    fn clean_endpoint(endpoint: &str) -> &str {
        endpoint
            .trim()
            .strip_prefix("stratum+tcp://")
            .or_else(|| endpoint.trim().strip_prefix("tcp://"))
            .unwrap_or(endpoint.trim())
    }

    /// Connect, subscribe, authorize, and run the Stratum client mining loop.
    pub async fn run(&self) -> Result<()> {
        let start_time = Instant::now();
        let target_endpoint = Self::clean_endpoint(&self.endpoint);

        info!(
            endpoint = target_endpoint,
            address = self.address,
            worker = self.worker,
            threads = self.threads,
            chain_id = self.chain_id,
            "connecting to Stratum V1 mining pool"
        );

        let stream = TcpStream::connect(target_endpoint)
            .await
            .with_context(|| format!("failed to connect to Stratum pool at {target_endpoint}"))?;

        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        // 1. Subscribe
        let subscribe_req = json!({
            "id": 1,
            "method": "mining.subscribe",
            "params": ["kovanica-miner/0.4.0", KOVANICA_STRATUM_CAPABILITY]
        });
        write_half
            .write_all(format!("{}\n", subscribe_req).as_bytes())
            .await
            .context("failed to write mining.subscribe request")?;
        write_half.flush().await?;

        let mut line = String::new();
        reader
            .read_line(&mut line)
            .await
            .context("failed to read mining.subscribe response")?;
        let sub_resp: Value = serde_json::from_str(&line)
            .with_context(|| format!("invalid JSON in subscribe response: {line}"))?;

        if !sub_resp.get("error").is_none_or(|v| v.is_null()) {
            anyhow::bail!("Stratum pool rejected subscribe: {:?}", sub_resp["error"]);
        }
        Self::verify_subscribe_capability(&sub_resp)?;

        let mut extranonce1 = String::new();
        let mut extranonce2_size = 4usize;
        if let Some(arr) = sub_resp.get("result").and_then(|r| r.as_array()) {
            if arr.len() >= 2 {
                if let Some(en1) = arr[1].as_str() {
                    extranonce1 = en1.to_string();
                }
            }
            if arr.len() >= 3 {
                if let Some(en2_sz) = arr[2].as_u64() {
                    extranonce2_size = en2_sz as usize;
                }
            }
        }

        info!(
            extranonce1 = extranonce1,
            extranonce2_size = extranonce2_size,
            "Stratum subscription established"
        );

        // 2. Authorize
        let auth_username = format!("{}.{}", self.address, self.worker);
        let auth_req = json!({
            "id": 2,
            "method": "mining.authorize",
            "params": [auth_username, self.password]
        });
        write_half
            .write_all(format!("{}\n", auth_req).as_bytes())
            .await
            .context("failed to write mining.authorize request")?;
        write_half.flush().await?;

        line.clear();
        reader
            .read_line(&mut line)
            .await
            .context("failed to read mining.authorize response")?;
        let auth_resp: Value = serde_json::from_str(&line)
            .with_context(|| format!("invalid JSON in authorize response: {line}"))?;

        let authorized = auth_resp
            .get("result")
            .and_then(|r| r.as_bool())
            .unwrap_or(false);
        if !authorized {
            anyhow::bail!(
                "Stratum pool authorization failed for user {}: {:?}",
                auth_username,
                auth_resp.get("error")
            );
        }

        info!(
            username = auth_username,
            "Stratum worker authorized successfully"
        );

        // 3. Initialize Shared State & Worker Channels
        let active_job: Arc<RwLock<Option<ActiveStratumJob>>> = Arc::new(RwLock::new(None));
        let job_version = Arc::new(AtomicU64::new(0));
        let current_share_target: Arc<RwLock<[u8; 32]>> = Arc::new(RwLock::new([0xff; 32])); // Default easy share target until set_difficulty
        let current_network_target: Arc<RwLock<[u8; 32]>> = Arc::new(RwLock::new([
            0x00, 0x00, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff,
        ]));

        let (share_tx, mut share_rx) = mpsc::unbounded_channel::<StratumShare>();

        // Spawn Worker Threads
        let mut worker_handles = Vec::with_capacity(self.threads);
        for thread_idx in 0..self.threads {
            let active_job_clone = active_job.clone();
            let job_version_clone = job_version.clone();
            let shutdown_clone = self.shutdown.clone();
            let stats_clone = self.stats.clone();
            let share_tx_clone = share_tx.clone();
            let total_threads = self.threads;

            let handle = std::thread::Builder::new()
                .name(format!("stratum-miner-{thread_idx}"))
                .spawn(move || {
                    let mut hasher = KovanicaHasher::new();
                    let mut local_version = 0u64;
                    let mut current_job: Option<ActiveStratumJob> = None;
                    let mut preimage_buf: Vec<u8> = Vec::new();
                    let mut nonce: u64 = thread_idx as u64;
                    let stride = total_threads as u64;

                    while !shutdown_clone.load(Ordering::Relaxed) {
                        let global_version = job_version_clone.load(Ordering::Relaxed);
                        if global_version != local_version {
                            let guard = active_job_clone.read().unwrap();
                            if let Some(job) = guard.as_ref() {
                                if job.version == global_version {
                                    preimage_buf = job.preimage.clone();
                                    current_job = Some(job.clone());
                                    local_version = global_version;
                                    nonce = thread_idx as u64;
                                }
                            }
                        }

                        let Some(job) = &current_job else {
                            std::thread::sleep(Duration::from_millis(20));
                            continue;
                        };

                        if preimage_buf.len() < 8 {
                            std::thread::sleep(Duration::from_millis(20));
                            continue;
                        }

                        // Hash in batches of 64 nonces. The gateway reconstructs the header
                        // nonce as (extranonce2 << 32) | nonce, so hash that exact value.
                        for _ in 0..64 {
                            let full_nonce = Self::share_nonce(&job.extranonce2, nonce);
                            let len = preimage_buf.len();
                            preimage_buf[len - 8..].copy_from_slice(&full_nonce.to_le_bytes());
                            let pow_hash = hasher.hash(&preimage_buf);

                            // Check share target (256-bit big-endian comparison)
                            if pow_hash.0 <= job.share_target {
                                let meets_network = pow_hash.0 <= job.network_target;
                                let _ = share_tx_clone.send(StratumShare {
                                    job_id: job.job_id.clone(),
                                    nonce: full_nonce,
                                    ntime: job.ntime,
                                    extranonce2: job.extranonce2.clone(),
                                    pow_hash,
                                    meets_network_target: meets_network,
                                });
                            }

                            nonce = nonce.wrapping_add(stride);
                        }
                        stats_clone.total_hashes.fetch_add(64, Ordering::Relaxed);
                    }
                })
                .context("failed to spawn stratum worker thread")?;
            worker_handles.push(handle);
        }

        // Periodic hashrate and stats reporting interval
        let mut stats_interval = tokio::time::interval(Duration::from_secs(3));
        stats_interval.tick().await; // consume first tick

        let mut next_req_id = 10u64;
        let mut in_flight_submits = std::collections::HashSet::new();

        println!("============================================================");
        println!("       Kovanica Stratum V1 Mining Client Connected          ");
        println!("============================================================");
        println!(" Pool Endpoint:      {}", target_endpoint);
        println!(" Worker Identity:    {}", auth_username);
        println!(" CPU Worker Threads: {}", self.threads);
        println!(" Scratchpad Memory:  {} MiB", self.threads * 2);
        println!("============================================================");

        // Main event loop: TCP read lines, submit shares, handle stats
        loop {
            tokio::select! {
                _ = self.shutdown_notify.notified() => {
                    info!("shutdown received in stratum miner loop");
                    break;
                }

                // Handle solved shares submitted from worker threads
                Some(share) = share_rx.recv() => {
                    self.stats.blocks_found.fetch_add(1, Ordering::Relaxed);
                    let req_id = next_req_id;
                    next_req_id += 1;
                    in_flight_submits.insert(req_id);

                    if share.meets_network_target {
                        info!(
                            job_id = share.job_id,
                            nonce = format!("0x{:016x}", share.nonce),
                            pow_hash = format!("{}", share.pow_hash),
                            "FOUND CANDIDATE NETWORK BLOCK SOLUTION"
                        );
                    }

                    let submit_req = json!({
                        "id": req_id,
                        "method": "mining.submit",
                        "params": [
                            auth_username,
                            share.job_id,
                            share.extranonce2,
                            format!("{:08x}", share.ntime),
                            format!("{:016x}", share.nonce)
                        ]
                    });

                    if let Err(e) = write_half.write_all(format!("{}\n", submit_req).as_bytes()).await {
                        error!(error = %e, "failed to send share to Stratum pool");
                        break;
                    }
                    let _ = write_half.flush().await;
                }

                // Read incoming lines from Stratum pool server
                read_res = reader.read_line(&mut line) => {
                    match read_res {
                        Ok(0) => {
                            warn!("Stratum pool closed TCP connection");
                            break;
                        }
                        Ok(_) => {
                            let msg_str = line.trim();
                            if !msg_str.is_empty() {
                                if let Ok(val) = serde_json::from_str::<Value>(msg_str) {
                                    // Handle Response or Notification
                                    if let Some(id) = val.get("id").and_then(|v| v.as_u64()) {
                                        if in_flight_submits.remove(&id) {
                                            let accepted = val.get("result").and_then(|r| r.as_bool()).unwrap_or(false);
                                            if accepted {
                                                self.stats.blocks_accepted.fetch_add(1, Ordering::Relaxed);
                                                info!(id = id, "Stratum share accepted by pool");
                                            } else {
                                                self.stats.blocks_rejected.fetch_add(1, Ordering::Relaxed);
                                                warn!(id = id, error = ?val.get("error"), "Stratum share rejected by pool");
                                            }
                                        }
                                    } else if let Some(method) = val.get("method").and_then(|m| m.as_str()) {
                                        match method {
                                            "mining.set_difficulty" => {
                                                if let Some(params) = val.get("params").and_then(|p| p.as_array()) {
                                                    if let Some(diff) = params.first().and_then(|d| d.as_f64()) {
                                                        info!(difficulty = diff, "received share difficulty adjustment");
                                                        let new_target = Self::difficulty_to_target(diff);
                                                        *current_share_target.write().unwrap() = new_target;
                                                    }
                                                }
                                            }
                                            "mining.notify" => {
                                                if let Some(params) = val.get("params").and_then(|p| p.as_array()) {
                                                    // params[6] is nbits in Stratum V1; derive the
                                                    // real network target from the job template.
                                                    let network_target = Self::parse_hex_u32(params.get(6))
                                                        .map(compact_to_target)
                                                        .unwrap_or_else(|| *current_network_target.read().unwrap());
                                                    if let Some(job) = Self::parse_notify(
                                                        params,
                                                        &current_share_target.read().unwrap(),
                                                        &network_target,
                                                        job_version.load(Ordering::Relaxed) + 1,
                                                    ) {
                                                        info!(job_id = job.job_id, "received new Stratum mining job");
                                                        job_version.fetch_add(1, Ordering::SeqCst);
                                                        *active_job.write().unwrap() = Some(job);
                                                    }
                                                }
                                            }
                                            other => {
                                                info!(method = other, "received unhandled Stratum method");
                                            }
                                        }
                                    }
                                }
                            }
                            line.clear();
                        }
                        Err(e) => {
                            error!(error = %e, "read error from Stratum pool TCP stream");
                            break;
                        }
                    }
                }

                // Periodic telemetry display
                _ = stats_interval.tick() => {
                    let elapsed = start_time.elapsed().as_secs_f64();
                    let total_hashes = self.stats.total_hashes();
                    let rate_fmt = format_hashrate(total_hashes, elapsed);
                    let accepted = self.stats.blocks_accepted();
                    let rejected = self.stats.blocks_rejected();
                    let found = self.stats.blocks_found();

                    println!(
                        "[{:.1}s] Hashrate: {} | Shares: {} accepted, {} rejected ({} submitted)",
                        elapsed, rate_fmt, accepted, rejected, found
                    );
                }
            }
        }

        // Graceful cleanup
        self.stop();
        for handle in worker_handles {
            let _ = handle.join();
        }

        let elapsed = start_time.elapsed().as_secs_f64();
        let total = self.stats.total_hashes();
        info!(
            elapsed_secs = elapsed,
            total_hashes = total,
            shares_accepted = self.stats.blocks_accepted(),
            shares_rejected = self.stats.blocks_rejected(),
            "Stratum miner stopped"
        );

        Ok(())
    }

    /// Verify that the gateway advertises the KovanicaHash job profile.
    ///
    /// Legacy gateways that omit the capability field are accepted with a warning so that
    /// miners from the built-in-profile release remain compatible.
    fn verify_subscribe_capability(resp: &Value) -> Result<()> {
        let Some(result) = resp.get("result").and_then(Value::as_array) else {
            return Ok(());
        };
        let Some(capability) = result.get(3) else {
            warn!(
                "gateway did not advertise a Stratum capability; assuming legacy Kovanica profile"
            );
            return Ok(());
        };

        let algorithm = capability
            .get("algorithm")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let profile = capability
            .get("profile")
            .and_then(Value::as_str)
            .unwrap_or_default();

        if algorithm == KOVANICA_STRATUM_ALGORITHM && profile == KOVANICA_STRATUM_CAPABILITY {
            return Ok(());
        }

        anyhow::bail!(
            "Stratum gateway does not support the required {KOVANICA_STRATUM_CAPABILITY} profile (algorithm={algorithm}, profile={profile})"
        )
    }

    /// Convert Stratum difficulty factor into a 256-bit big-endian target array.
    ///
    /// This matches `kovanica_stratum::difficulty::target_from_difficulty`: difficulty 1 is the
    /// canonical compact target `0x1d00ffff`, and larger difficulties scale it down exactly.
    pub fn difficulty_to_target(diff: f64) -> [u8; 32] {
        const DIFFICULTY_1_TARGET_BITS: u32 = 0x1d00ffff;
        if diff <= 0.0 {
            return [0xff; 32];
        }
        if (diff - 1.0).abs() < 1e-9 {
            return compact_to_target(DIFFICULTY_1_TARGET_BITS);
        }

        let scaled = (65535.0 / diff) * 65536.0 * 18446744073709551616.0;
        if scaled >= u128::MAX as f64 {
            return [0xff; 32];
        }
        let mut target = [0u8; 32];
        target[..16].copy_from_slice(&(scaled as u128).to_be_bytes());
        target
    }

    /// Parse a compact-hex JSON value into a `u32`.
    fn parse_hex_u32(value: Option<&Value>) -> Option<u32> {
        match value? {
            Value::String(s) => {
                let clean = s.strip_prefix("0x").unwrap_or(s);
                u32::from_str_radix(clean, 16).ok()
            }
            other => other.as_u64().and_then(|n| u32::try_from(n).ok()),
        }
    }

    /// Parse a compact-hex JSON value into a `u64`.
    fn parse_hex_u64(value: Option<&Value>) -> Option<u64> {
        match value? {
            Value::String(s) => {
                let clean = s.strip_prefix("0x").unwrap_or(s);
                u64::from_str_radix(clean, 16).ok()
            }
            other => other.as_u64(),
        }
    }

    /// Combine worker extranonce2 and rotating nonce into the 64-bit header nonce used by the
    /// Kovanica Stratum gateway.
    pub fn share_nonce(extranonce2: &str, nonce: u64) -> u64 {
        let en2 = u32::from_str_radix(extranonce2.trim_start_matches("0x"), 16).unwrap_or(0);
        ((en2 as u64) << 32) | nonce
    }

    /// Parse `mining.notify` parameters into an `ActiveStratumJob`.
    pub fn parse_notify(
        params: &[Value],
        share_target: &[u8; 32],
        network_target: &[u8; 32],
        version: u64,
    ) -> Option<ActiveStratumJob> {
        if params.is_empty() {
            return None;
        }

        let job_id = params[0].as_str()?.to_string();
        let ntime = Self::parse_hex_u64(params.get(7)).unwrap_or(0);

        // The Kovanica gateway puts the canonical header preimage prefix (everything except the
        // trailing 8-byte nonce) in `coinb1`. Decode that first so the client hashes exactly the
        // same bytes the gateway reconstructs when it validates a share.
        let kovanica_prefix = params.get(2).and_then(Value::as_str).and_then(|hex_str| {
            let clean = hex_str.strip_prefix("0x").unwrap_or(hex_str);
            hex::decode(clean).ok()
        });

        let (preimage, extranonce2) =
            if let Some(mut prefix) = kovanica_prefix.filter(|p| p.len() > 32) {
                prefix.extend_from_slice(&0u64.to_le_bytes());
                (prefix, "00000000".to_string())
            } else if params.len() >= 8 {
                // Standard Stratum V1 parameters (kept for compatibility with non-Kovanica pools).
                // [job_id, prevhash, coinb1, coinb2, merkle_branch, version, nbits, ntime, clean_jobs]
                let prevhash = params[1].as_str().unwrap_or_default();
                let clean_prev = prevhash.strip_prefix("0x").unwrap_or(prevhash);
                let mut buf = hex::decode(clean_prev).unwrap_or_else(|_| vec![0u8; 32]);
                buf.resize(80, 0u8);
                (buf, "00000000".to_string())
            } else {
                // Minimal test fallback: construct a candidate template with a nonce slot.
                let mut dummy = vec![0x42u8; 72];
                dummy.extend_from_slice(&0u64.to_le_bytes());
                (dummy, "00000000".to_string())
            };

        Some(ActiveStratumJob {
            version,
            job_id,
            preimage,
            share_target: *share_target,
            network_target: *network_target,
            ntime,
            extranonce2,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn difficulty_one_matches_gateway_target() {
        assert_eq!(
            StratumMiner::difficulty_to_target(1.0),
            compact_to_target(0x1d00ffff)
        );
    }

    #[test]
    fn share_nonce_folds_extranonce2_into_high_half() {
        assert_eq!(
            StratumMiner::share_nonce("00000001", 0x00000042),
            0x0000000100000042
        );
        assert_eq!(
            StratumMiner::share_nonce("0x00000002", 0x00000042),
            0x0000000200000042
        );
    }

    #[test]
    fn parse_notify_uses_kovanica_preimage_prefix_from_coinb1() {
        let prefix = vec![0x42u8; 96];
        let params = json!([
            "job_1",
            "0000000000000000000000000000000000000000000000000000000000000000",
            hex::encode(&prefix),
            "",
            [],
            "00000001",
            "207fffff",
            "673a5f20",
            true
        ]);
        let share_target = [0xffu8; 32];
        let network_target = [0x7fu8; 32];

        let job = StratumMiner::parse_notify(
            params.as_array().unwrap(),
            &share_target,
            &network_target,
            7,
        )
        .expect("must parse Kovanica notify");

        assert_eq!(job.job_id, "job_1");
        assert_eq!(job.version, 7);
        assert_eq!(job.preimage.len(), prefix.len() + 8);
        assert_eq!(&job.preimage[..prefix.len()], &prefix[..]);
        assert_eq!(&job.preimage[prefix.len()..], &[0u8; 8]);
        assert_eq!(job.ntime, 0x673a5f20);
        assert_eq!(job.network_target, network_target);
    }

    #[test]
    fn subscribe_capability_accepts_kovanica_profile() {
        let resp = json!({
            "id": 1,
            "result": [
                [],
                "00000001",
                4,
                {
                    "algorithm": "kovanicahash",
                    "profile": "kovanicahash-v1",
                    "version": 1
                }
            ],
            "error": null
        });
        assert!(StratumMiner::verify_subscribe_capability(&resp).is_ok());
    }

    #[test]
    fn subscribe_capability_rejects_incompatible_profile() {
        let resp = json!({
            "id": 1,
            "result": [
                [],
                "00000001",
                4,
                {
                    "algorithm": "sha256d",
                    "profile": "bitcoin-v1",
                    "version": 1
                }
            ],
            "error": null
        });
        assert!(StratumMiner::verify_subscribe_capability(&resp).is_err());
    }

    #[test]
    fn subscribe_capability_allows_legacy_gateway() {
        let resp = json!({
            "id": 1,
            "result": [[], "00000001", 4],
            "error": null
        });
        assert!(StratumMiner::verify_subscribe_capability(&resp).is_ok());
    }
}
