//! End-to-end integration tests for `kovanica-stratum`.
//!
//! Simulates real TCP client handshakes against the `StratumGateway`:
//! - `mining.subscribe` -> Extranonce1 and extranonce2_size allocation.
//! - `mining.authorize` -> PRO-21 address validation and difficulty notification.
//! - `mining.notify` -> Job broadcasting to active subscribed miners.
//! - `mining.submit` -> Share validation, duplicate detection, and low difficulty rejection.
//! - Error cases: unauthorized submissions, malformed JSON, and unknown jobs.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kovanica_crypto::{generate_keypair, KovanicaHasher};
use kovanica_stratum::config::StratumConfig;
use kovanica_stratum::gateway::StratumGateway;
use kovanica_stratum::{KOVANICA_STRATUM_ALGORITHM, KOVANICA_STRATUM_CAPABILITY};
use kovanica_types::block::{check_pow_hash_meets_target, BlockHeader};
use kovanica_types::signing::chain_id::HYBRID_TESTNET;
use kovanica_types::{Address, Hash};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

/// Helper to start a test gateway on an ephemeral local port.
async fn start_test_gateway(initial_difficulty: f64) -> (Arc<StratumGateway>, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("must bind ephemeral listener");
    let local_addr = listener.local_addr().expect("must get local addr");

    let config = StratumConfig {
        listen_addr: local_addr,
        difficulty: initial_difficulty,
        chain_id: HYBRID_TESTNET,
        // Point the poller at a closed port so tests never depend on a live node or receive an
        // unsolicited `mining.notify` before the test injects its own job.
        rpc_url: "http://127.0.0.1:1/rpc".to_string(),
        // Set a very long poll interval so the test doesn't log unneeded upstream errors
        poll_interval: Duration::from_secs(3600),
        ..Default::default()
    };

    let gateway = Arc::new(StratumGateway::new(config));
    let gw_clone = Arc::clone(&gateway);

    tokio::spawn(async move {
        let _ = gw_clone.run_with_listener(listener).await;
    });

    // Brief yield to ensure server loop is ready
    tokio::time::sleep(Duration::from_millis(50)).await;

    (gateway, local_addr)
}

/// Helper client connection with buffered I/O.
struct TestClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl TestClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr)
            .await
            .expect("client must connect to stratum gateway");
        let (read_half, write_half) = stream.into_split();
        Self {
            reader: BufReader::new(read_half),
            writer: write_half,
        }
    }

    async fn send_line(&mut self, line: &str) {
        self.writer
            .write_all(format!("{}\n", line.trim()).as_bytes())
            .await
            .expect("must write to server");
        self.writer.flush().await.expect("must flush to server");
    }

    async fn read_json(&mut self) -> Value {
        let mut line = String::new();
        timeout(Duration::from_secs(3), self.reader.read_line(&mut line))
            .await
            .expect("timed out reading from server")
            .expect("I/O error reading line");
        serde_json::from_str(&line).expect("must parse valid JSON response")
    }
}

/// Create a dummy candidate block header for job testing.
fn create_test_header(round: u64, target_bits: u32) -> BlockHeader {
    let (_sk, pk) = generate_keypair();
    let payout = Address(pk.0);

    BlockHeader::new(
        HYBRID_TESTNET,
        round,
        0,
        vec![],
        Hash([0x42; 32]),
        1731800000000,
        target_bits,
        0,
    )
    .with_payout(payout)
}

/// Find a nonce that satisfies the template's real network target, so the gateway will forward
/// the solved header upstream instead of only accepting it as a share.
fn find_network_solution(header: &BlockHeader, extranonce2: u32) -> u64 {
    let mut candidate = header.clone();
    let mut hasher = KovanicaHasher::new();
    for nonce in 0..100_000u64 {
        let full_nonce = ((extranonce2 as u64) << 32) | nonce;
        candidate.nonce = full_nonce;
        let pow_hash = hasher.hash(&candidate.preimage());
        if check_pow_hash_meets_target(&pow_hash, candidate.target_bits) {
            return full_nonce;
        }
    }
    panic!("no network solution found in nonce range");
}

#[tokio::test]
async fn test_full_stratum_handshake_and_share_submission() {
    // Start gateway with 0.0 difficulty so any nonce succeeds for fast deterministic testing
    let (gateway, addr) = start_test_gateway(0.0).await;
    let mut client = TestClient::connect(addr).await;

    // 1. mining.subscribe
    let subscribe_req = json!({
        "id": 1,
        "method": "mining.subscribe",
        "params": ["TestMiner/1.0"]
    });
    client.send_line(&subscribe_req.to_string()).await;

    let sub_resp = client.read_json().await;
    assert_eq!(sub_resp["id"], 1);
    assert!(sub_resp["error"].is_null());

    let result = sub_resp["result"].as_array().expect("result array");
    assert_eq!(result.len(), 4);
    let extranonce1 = result[1].as_str().expect("extranonce1 hex");
    let en2_size = result[2].as_u64().expect("extranonce2 size");
    assert!(!extranonce1.is_empty());
    assert_eq!(en2_size, 4);
    assert_eq!(result[3]["algorithm"], KOVANICA_STRATUM_ALGORITHM);
    assert_eq!(result[3]["profile"], KOVANICA_STRATUM_CAPABILITY);
    assert_eq!(result[3]["version"], 1);

    // 2. mining.authorize with invalid address syntax (should fail)
    let invalid_auth_req = json!({
        "id": 2,
        "method": "mining.authorize",
        "params": ["invalid_address_format.worker1", "x"]
    });
    client.send_line(&invalid_auth_req.to_string()).await;

    let invalid_auth_resp = client.read_json().await;
    assert_eq!(invalid_auth_resp["id"], 2);
    assert!(!invalid_auth_resp["error"].is_null());
    assert_eq!(invalid_auth_resp["error"][0], 24);

    // 3. mining.authorize with canonical PRO-21 address
    let (_sk, pk) = generate_keypair();
    let valid_addr = Address(pk.0);
    let valid_addr_str = valid_addr.encode();
    let username = format!("{}.rig_01", valid_addr_str);

    let auth_req = json!({
        "id": 3,
        "method": "mining.authorize",
        "params": [username, "x"]
    });
    client.send_line(&auth_req.to_string()).await;

    // Expect auth response (result: true)
    let auth_resp = client.read_json().await;
    assert_eq!(auth_resp["id"], 3);
    assert_eq!(auth_resp["result"], true);
    assert!(auth_resp["error"].is_null());

    // Expect mining.set_difficulty notification
    let set_diff_msg = client.read_json().await;
    assert_eq!(set_diff_msg["method"], "mining.set_difficulty");
    let diff_val = set_diff_msg["params"][0].as_f64().expect("difficulty f64");
    assert!((diff_val - 0.0).abs() < 1e-6);

    // 4. Inject candidate block template and broadcast
    let test_header = create_test_header(100, 0x207fffff);
    let job = gateway.job_manager().add_template(test_header);
    gateway.broadcast_notify(&job);

    // Client should receive mining.notify
    let notify_msg = client.read_json().await;
    assert_eq!(notify_msg["method"], "mining.notify");
    let notify_params = notify_msg["params"].as_array().expect("notify params");
    assert_eq!(notify_params.len(), 9);
    let broadcast_job_id = notify_params[0].as_str().expect("job_id");
    assert_eq!(broadcast_job_id, job.job_id);

    // 5. Submit valid share
    let extranonce2 = "00000001";
    let nonce = "00000042";
    let submit_req = json!({
        "id": 4,
        "method": "mining.submit",
        "params": [username, broadcast_job_id, extranonce2, nonce]
    });
    client.send_line(&submit_req.to_string()).await;

    let submit_resp = client.read_json().await;
    assert_eq!(submit_resp["id"], 4);
    assert_eq!(submit_resp["result"], true);
    assert!(submit_resp["error"].is_null());

    // 6. Submit duplicate share (should be rejected with code 22)
    let dup_submit_req = json!({
        "id": 5,
        "method": "mining.submit",
        "params": [username, broadcast_job_id, extranonce2, nonce]
    });
    client.send_line(&dup_submit_req.to_string()).await;

    let dup_resp = client.read_json().await;
    assert_eq!(dup_resp["id"], 5);
    assert!(!dup_resp["error"].is_null());
    assert_eq!(dup_resp["error"][0], 22); // Duplicate share

    // 7. Submit to unknown job (should be rejected with code 21)
    let unknown_job_req = json!({
        "id": 6,
        "method": "mining.submit",
        "params": [username, "job_deadbeef", extranonce2, "00000099"]
    });
    client.send_line(&unknown_job_req.to_string()).await;

    let unknown_resp = client.read_json().await;
    assert_eq!(unknown_resp["id"], 6);
    assert!(!unknown_resp["error"].is_null());
    assert_eq!(unknown_resp["error"][0], 21); // Job not found

    gateway.shutdown();
}

#[tokio::test]
async fn test_stratum_unauthorized_submit_rejected() {
    let (gateway, addr) = start_test_gateway(1.0).await;
    let mut client = TestClient::connect(addr).await;

    // Attempt to submit without authorization
    let submit_req = json!({
        "id": 1,
        "method": "mining.submit",
        "params": ["worker", "job_1", "00000001", "00000001"]
    });
    client.send_line(&submit_req.to_string()).await;

    let resp = client.read_json().await;
    assert_eq!(resp["id"], 1);
    assert!(!resp["error"].is_null());
    assert_eq!(resp["error"][0], 24); // Unauthorized

    gateway.shutdown();
}

#[tokio::test]
async fn test_stratum_low_difficulty_share_rejected() {
    // Start gateway with astronomical difficulty so shares are rejected
    let (gateway, addr) = start_test_gateway(1_000_000_000.0).await;
    let mut client = TestClient::connect(addr).await;

    // Subscribe
    client
        .send_line(&json!({"id": 1, "method": "mining.subscribe", "params": []}).to_string())
        .await;
    let _ = client.read_json().await;

    // Authorize
    let (_sk, pk) = generate_keypair();
    let addr_str = Address(pk.0).encode();
    client
        .send_line(
            &json!({"id": 2, "method": "mining.authorize", "params": [addr_str, "x"]}).to_string(),
        )
        .await;
    let _ = client.read_json().await; // auth result
    let _ = client.read_json().await; // set_difficulty

    // Inject job
    let test_header = create_test_header(1, 0x1d00ffff);
    let job = gateway.job_manager().add_template(test_header);

    // Submit arbitrary share -> target at 1B difficulty requires ~30 leading zero bits
    let submit_req = json!({
        "id": 3,
        "method": "mining.submit",
        "params": [addr_str, job.job_id, "00000001", "00000001"]
    });
    client.send_line(&submit_req.to_string()).await;

    let submit_resp = client.read_json().await;
    assert_eq!(submit_resp["id"], 3);
    assert!(!submit_resp["error"].is_null());
    assert_eq!(submit_resp["error"][0], 23); // Low difficulty share

    gateway.shutdown();
}

#[tokio::test]
async fn test_multiple_subscribers_unique_extranonce1() {
    let (gateway, addr) = start_test_gateway(1.0).await;

    let mut client1 = TestClient::connect(addr).await;
    let mut client2 = TestClient::connect(addr).await;

    client1
        .send_line(&json!({"id": 1, "method": "mining.subscribe", "params": []}).to_string())
        .await;
    let resp1 = client1.read_json().await;
    let en1_1 = resp1["result"][1]
        .as_str()
        .expect("extranonce1 for client 1");

    client2
        .send_line(&json!({"id": 1, "method": "mining.subscribe", "params": []}).to_string())
        .await;
    let resp2 = client2.read_json().await;
    let en1_2 = resp2["result"][1]
        .as_str()
        .expect("extranonce1 for client 2");

    assert_ne!(
        en1_1, en1_2,
        "each subscriber must receive unique extranonce1"
    );

    gateway.shutdown();
}

/// Minimal JSON-RPC node stub that serves one work template and records submitted headers.
async fn spawn_mock_node(header: BlockHeader) -> (String, Arc<Mutex<Vec<BlockHeader>>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mock node must bind ephemeral listener");
    let addr = listener.local_addr().expect("mock node local addr");
    let submitted = Arc::new(Mutex::new(Vec::new()));
    let submitted_srv = Arc::clone(&submitted);

    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let header = header.clone();
            let submitted = Arc::clone(&submitted_srv);

            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut tmp = [0u8; 4096];
                let header_end = loop {
                    let Ok(n) = socket.read(&mut tmp).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                };

                let header_text = String::from_utf8_lossy(&buf[..header_end]).to_string();
                let content_len = header_text
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        if key.eq_ignore_ascii_case("content-length") {
                            value.trim().parse::<usize>().ok()
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                while buf.len() < header_end + content_len {
                    let Ok(n) = socket.read(&mut tmp).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }

                let req: Value =
                    match serde_json::from_slice(&buf[header_end..header_end + content_len]) {
                        Ok(v) => v,
                        Err(_) => return,
                    };
                let method = req["method"].as_str().unwrap_or("");

                let result = match method {
                    "kovanica_getWork" => {
                        let mut work =
                            serde_json::to_value(&header).expect("header must serialize");
                        let obj = work
                            .as_object_mut()
                            .expect("header must serialize to object");
                        obj.insert("chain_id".into(), json!(HYBRID_TESTNET));
                        obj.insert("round".into(), json!(header.round));
                        obj.insert("author".into(), json!(header.author));
                        obj.insert("parents".into(), json!([]));
                        obj.insert("parent_refs".into(), json!([]));
                        obj.insert(
                            "merkle_root".into(),
                            json!(hex::encode(header.merkle_root.0)),
                        );
                        obj.insert("timestamp".into(), json!(header.timestamp));
                        obj.insert(
                            "target_bits".into(),
                            json!(format!("0x{:08x}", header.target_bits)),
                        );
                        obj.insert(
                            "payout_address".into(),
                            json!(header.payout.expect("test header has payout").encode()),
                        );
                        obj.insert("header".into(), serde_json::to_value(&header).unwrap());
                        work
                    }
                    "kovanica_submitWork" => {
                        if let Ok(solved) =
                            serde_json::from_value::<BlockHeader>(req["params"]["header"].clone())
                        {
                            submitted
                                .lock()
                                .expect("lock submitted headers")
                                .push(solved);
                        }
                        json!({ "accepted": true })
                    }
                    _ => Value::Null,
                };

                let resp = json!({ "jsonrpc": "2.0", "id": 1, "result": result });
                let body = serde_json::to_vec(&resp).expect("response must serialize");
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(&body).await;
            });
        }
    });

    (format!("http://{addr}/rpc"), submitted)
}

#[tokio::test]
async fn test_gateway_and_builtin_client_hash_and_submit_same_template() {
    let (_sk, pk) = generate_keypair();
    let worker_payout = Address(pk.0);
    let header = create_test_header(4242, 0x207fffff).with_payout(worker_payout);
    let (rpc_url, submitted) = spawn_mock_node(header.clone()).await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("must bind ephemeral listener");
    let addr = listener.local_addr().expect("must get local addr");
    let config = StratumConfig {
        listen_addr: addr,
        rpc_url,
        difficulty: 0.0,
        poll_interval: Duration::from_millis(20),
        ..Default::default()
    };
    let gateway = Arc::new(StratumGateway::new(config));
    let gw_clone = Arc::clone(&gateway);
    tokio::spawn(async move {
        let _ = gw_clone.run_with_listener(listener).await;
    });

    let mut client = TestClient::connect(addr).await;
    client
        .send_line(
            &json!({"id": 1, "method": "mining.subscribe", "params": ["kovanica-miner/test"]})
                .to_string(),
        )
        .await;
    let sub_resp = client.read_json().await;
    let sub_result = sub_resp["result"].as_array().expect("subscribe result");
    assert_eq!(sub_result[3]["profile"], KOVANICA_STRATUM_CAPABILITY);

    let username = format!("{}.rig_01", worker_payout.encode());
    client
        .send_line(
            &json!({"id": 2, "method": "mining.authorize", "params": [username.clone(), "x"]})
                .to_string(),
        )
        .await;
    let _ = client.read_json().await; // auth result
    let _ = client.read_json().await; // set_difficulty

    // Wait for the poller to deliver a job from the mock node.
    let notify = loop {
        let mut line = String::new();
        timeout(Duration::from_secs(5), client.reader.read_line(&mut line))
            .await
            .expect("timed out waiting for mining.notify")
            .expect("notify I/O");
        let value: Value = serde_json::from_str(&line).expect("notify must be JSON");
        if value["method"].as_str() == Some("mining.notify") {
            break value;
        }
    };

    let params = notify["params"].as_array().expect("notify params");
    let job_id = params[0].as_str().expect("job id");
    let coinb1 = params[2].as_str().expect("coinb1 preimage prefix");
    let prefix = hex::decode(coinb1).expect("coinb1 must be hex");
    assert!(
        prefix.len() > 32,
        "coinb1 must carry the Kovanica preimage prefix"
    );

    // This mirrors the fixed built-in client: append an 8-byte nonce slot and submit the
    // (extranonce2 << 32) | nonce value that the gateway will write back into the header.
    let extranonce2 = "00000001";
    let full_nonce = find_network_solution(&header, 1);
    client
        .send_line(
            &json!({
                "id": 3,
                "method": "mining.submit",
                "params": [username, job_id, extranonce2, "00000000", format!("{full_nonce:016x}")]
            })
            .to_string(),
        )
        .await;
    let submit_resp = client.read_json().await;
    assert_eq!(submit_resp["result"], true, "share must be accepted");

    // Duplicate share and stale/unknown job must be rejected with the documented codes.
    let duplicate_params = json!([
        username,
        job_id,
        extranonce2,
        "00000000",
        format!("{full_nonce:016x}")
    ]);
    client
        .send_line(
            &json!({"id": 4, "method": "mining.submit", "params": duplicate_params}).to_string(),
        )
        .await;
    let duplicate_resp = client.read_json().await;
    assert_eq!(
        duplicate_resp["error"][0], 22,
        "duplicate share must be rejected"
    );

    let stale_params = json!([
        username,
        "job_stale",
        extranonce2,
        "00000000",
        format!("{full_nonce:016x}")
    ]);
    client
        .send_line(&json!({"id": 5, "method": "mining.submit", "params": stale_params}).to_string())
        .await;
    let stale_resp = client.read_json().await;
    assert_eq!(stale_resp["error"][0], 21, "unknown job must be rejected");

    // The upstream node must receive the reconstructed header with the worker payout and the
    // same full nonce the miner hashed.
    let recorded = timeout(Duration::from_secs(5), async {
        loop {
            let found = submitted
                .lock()
                .expect("lock submitted headers")
                .first()
                .cloned();
            if let Some(header) = found {
                return header;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("upstream submitWork must be called");
    assert_eq!(recorded.nonce, full_nonce);
    assert_eq!(recorded.payout, Some(worker_payout));
    assert_eq!(recorded.target_bits, header.target_bits);

    // The simulator must have produced a real KovanicaHash solution over the reconstructed
    // preimage, not a SHA-256 or otherwise fabricated share.
    let mut hasher = KovanicaHasher::new();
    let pow_hash = hasher.hash(&recorded.preimage());
    assert_eq!(recorded.pow_hash, Some(pow_hash));
    assert!(
        check_pow_hash_meets_target(&pow_hash, recorded.target_bits),
        "submitted header must satisfy the real network target"
    );

    gateway.shutdown();
}
