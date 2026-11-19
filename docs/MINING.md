# Kovanica Proof-of-Work Mining Manual

This document is the authoritative operator and developer guide for mining on the **Kovanica** Layer-1 blockchain (native ticker **KVNC**). It details the consensus invariants for Hybrid PoW + GHOSTDAG on **kovanica-testnet**, the memory-hard **KovanicaHash** algorithm, local benchmarking via `kovanica miner bench`, solo mining operations, mining pool and Stratum protocol integration, JSON-RPC APIs, and VPS sizing guidelines.

**Release:** `v0.3.0-node`. **Network tag:** `2-24cdb5fadd98b207`. External peers bootstrap at `seed1.kovanica.online:8000` (DNS name only). See [Testnet Seed Nodes](TESTNET-SEED-NODES.md).

---

## Table of Contents

1. [Consensus Overview & Invariants](#1-consensus-overview--invariants)
2. [KovanicaHash PoW Technical Specification](#2-kovanicahash-pow-technical-specification)
3. [Local Hashrate Benchmarking (`kovanica miner bench`)](#3-local-hashrate-benchmarking-kovanica-miner-bench)
4. [Solo Mining Operations](#4-solo-mining-operations)
5. [Stratum Mining & Pool Integration](#5-stratum-mining--pool-integration)
6. [Mining JSON-RPC Interfaces](#6-mining-json-rpc-interfaces)
7. [Hardware Guidelines & Troubleshooting](#7-hardware-guidelines--troubleshooting)
8. [Document References](#8-document-references)

---

## 1. Consensus Overview & Invariants

Kovanica utilizes a **Hybrid PoW + GHOSTDAG** consensus architecture for decentralized block production and transaction ordering.

- **Chain ID:** `2` (`TESTNET` / `hybrid-testnet`).
- **Consensus Model:** Hybrid Proof-of-Work block authoring combined with GHOSTDAG DAG ordering. **There is strictly NO stake required or weighted for consensus** (architectural decision confirmed 2026-11-05).
- **PoW Algorithm:** **KovanicaHash** (BLAKE3 + 2 MiB pseudo-random memory mix scratchpad).
- **Target Block Time:** 4 seconds (`TARGET_BLOCK_MS = 4000`).
- **GHOSTDAG Parameter $K$:** $K = 5$ (`GHOSTDAG_K = 5`).
- **Block Reward:** **2.5 KVNC** per committed block author, credited directly to the miner's payout address.
- **Subsidy Halving / Decay:** Initial subsidy era of 8,200,000 rewarded blocks with $\times 3/4$ decay each subsequent era, drawing from an 82,000,000 KVNC mining subsidy budget (`NoStake82M`; there is no genesis validator allocation).
- **Payout Address Format:** Canonical PRO-21 base32 address (`kvnc` + 57 base32 chars + `dag` = 64 characters). Legacy hex addresses (`0x...` or 64 hex characters) are rejected on the hybrid networks (Chain IDs 1 and 2); use the canonical form.

For further architectural context, see the [Hybrid PoW + GHOSTDAG ADR](adr/0003-hybrid-pow-ghostdag.md), the [Block Header Specification](dev/POW-HEADER-SPEC.md), and the [Tokenomics Specification](TOKENOMICS.md).

---

## 2. KovanicaHash PoW Technical Specification

KovanicaHash is a memory-hard proof-of-work algorithm engineered to resist specialized ASIC/FPGA monopolization by enforcing memory-latency and bandwidth dependency while remaining comfortable on commodity multi-core CPUs and low-cost (2–4 GB RAM) VPS instances.

```
       +------------------------------------+
       |  Canonical Block Header Preimage   |
       |  (132+ bytes, nonce at tail 8 B)   |
       +-----------------+------------------+
                         |
                         v
       +------------------------------------+
       |  1. Seed Phase (BLAKE3 Keyed)      |
       |  Domain: "KVNC-POW-SEED-v1"        |
       +-----------------+------------------+
                         | 32-byte seed
                         v
       +------------------------------------+
       |  2. Memory Mix Phase (2 MiB)       |
       |  - BLAKE3 XOF initializes 65,536   |
       |    chunks of 32 bytes (2,097,152 B)|
       |  - 1,024 Pseudo-Random Rounds      |
       |  - Non-linear state wandering      |
       |  - Word rotations & write-backs    |
       +-----------------+------------------+
                         | mixed state + memory samples
                         v
       +------------------------------------+
       |  3. Finalize Phase (BLAKE3 Keyed)  |
       |  Domain: "KVNC-POW-FINL-v1"        |
       +-----------------+------------------+
                         |
                         v
                Final 32-byte Hash
            (check <= target_bits threshold)
```

### Execution Phases

1. **Seed Phase:**
   The canonical block header preimage (serialized per [PoW Header Spec](dev/POW-HEADER-SPEC.md) with domain tag `KVNC-POW-HDR-v1`) is hashed via BLAKE3 in keyed mode using domain `KVNC-POW-SEED-v1` into a 32-byte seed.
2. **Memory Mix Phase (2 MiB):**
   - A 2 MiB (`2,097,152` bytes) scratchpad allocated once per worker thread context is initialized from the seed using BLAKE3 in extendable output (XOF) mode.
   - The state traverses 1,024 pseudo-random mixing rounds. In each round, state words select chunks from the 2 MiB scratchpad via non-linear indexing, apply bitwise rotations (`rotate_left(19)` and `rotate_left(41)`), compute wrapping additions, and write back mutated states into memory.
3. **Finalize Phase:**
   Keyed BLAKE3 compression under domain `KVNC-POW-FINL-v1` ingests the initial seed, mutated mix state, and scratchpad boundary samples, producing the final 32-byte solution hash.

### Zero-Allocation Hot Path

`KovanicaHasher` allocates its 2 MiB scratchpad on the heap during initialization. Subsequent evaluations reuse the preallocated buffer, incurring **zero heap allocations** in the hashing loop.

---

## 3. Local Hashrate Benchmarking (`kovanica miner bench`)

Before deploying mining rigs or participating in testnet rounds, operators can evaluate local CPU throughput using the offline `kovanica miner bench` command.

The benchmark runs completely **offline and standalone** — no running node or RPC connection is needed.

### CLI Syntax & Flags

```bash
kovanica miner bench [OPTIONS]
```

Or using the alias:

```bash
kovanica mining bench [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--threads <N>` | `-t` | Logical CPU cores | Number of parallel worker threads (1 to 256). |
| `--duration <SECS>` | `-d` | `5` | Benchmark duration in seconds ($\ge 1$). |
| `--json` | | `false` | Output machine-readable JSON instead of tabular summary. |

### Human-Readable Output Example

Running a 5-second benchmark on 4 CPU threads:

```bash
kovanica miner bench --threads 4 --duration 5
```

Output:

```text
============================================================
            KovanicaHash PoW Benchmark Results             
============================================================
 Algorithm:          KovanicaHash (BLAKE3 + 2 MiB mix)
 Worker Threads:     4
 Target Duration:    5 s
 Elapsed Time:       5.002 s
 Total Hashes:       6280
 Total Hashrate:     1.26 kH/s (1255.50 H/s)
 Per-Thread Rate:    313.87 H/s
 Scratchpad Memory:  8 MiB (4 threads x 2 MiB)
============================================================
```

### JSON Output Example (`--json`)

When invoked with `--json`, the command emits structured JSON suitable for monitoring scripts and CI pipelines:

```bash
kovanica --json miner bench --threads 2 --duration 3
```

```json
{
  "algorithm": "KovanicaHash (BLAKE3 + 2 MiB mix)",
  "threads": 2,
  "target_duration_secs": 3,
  "elapsed_secs": 3.001,
  "total_hashes": 1892,
  "hashrate_hps": 630.46,
  "hashrate_formatted": "630.46 H/s",
  "per_thread_hps": 315.23,
  "scratchpad_memory_mib": 4,
  "thread_hashes": [
    946,
    946
  ]
}
```

### Hardware Hashrate Reference (KovanicaHash)

| CPU Tier | Physical / Logical Cores | Memory Config | Expected Total Hashrate |
|---|---|---|---|
| VPS Entry (2 vCPU) | 2 vCPU | DDR4 (shared host) | 400 – 650 H/s |
| VPS Mid (4 vCPU) | 4 vCPU | DDR4 (dedicated) | 1.0 – 1.4 kH/s |
| Modern Desktop (AMD Ryzen 7 7800X3D) | 8C / 16T | DDR5-6000 (96 MB L3 Cache) | 4.5 – 6.2 kH/s |
| Server Node (AMD EPYC 7763) | 32 Threads allocated | 8-Channel DDR4-3200 | 12.0 – 16.5 kH/s |

---

## 4. Solo Mining Operations

Solo miners connect directly to a Kovanica node, fetch work templates via JSON-RPC, compute PoW solutions locally, and submit solved blocks to the network.

### Step 1: Generate or Select Payout Address

Mining rewards (2.5 KVNC per block) are paid directly to your account address. Generate a fresh keypair using the CLI:

```bash
kovanica keygen --out ~/.kovanica/miner.json
```

Display your canonical PRO-21 address (`kvnc...dag`):

```bash
kovanica import --keystore ~/.kovanica/miner.json
```

Example payout address:
`kvncqqqsyqcyq5rqwzqfpg9scrgwpugpzysnzs23v9ccrydpk8qarc07228f2dag`

### Step 2: Launch Mining Worker

#### Mode A: Standalone CLI Solo Miner (`kovanica miner run`)

The standalone CLI solo miner connects to a local or remote node on kovanica-testnet (`v0.3.0-node`, network tag `2-24cdb5fadd98b207`). Bootstrap is `seed1.kovanica.online:8000` (DNS name only):

```bash
kovanica miner run \
  --address kvncqqqsyqcyq5rqwzqfpg9scrgwpugpzysnzs23v9ccrydpk8qarc07228f2dag \
  --threads 4 \
  --poll-interval-ms 1000 \
  --rpc-url http://127.0.0.1:8545
```

##### Command-Line Options

| Flag | Short | Default | Description |
|---|---|---|---|
| `--address <ADDR>` | `-a` | *Required* | Canonical PRO-21 miner payout address (`kvnc...dag`). |
| `--threads <N>` | `-t` | Logical CPU cores | Worker thread count (1–256). |
| `--poll-interval-ms <MS>` | | `1000` | RPC work polling interval in milliseconds. |
| `--rpc-url <URL>` | | `http://127.0.0.1:8545` | Kovanica node JSON-RPC endpoint. |
| `--stratum <HOST:PORT>` | | `None` | Connect to Stratum V1 mining pool instead of direct HTTP RPC. |
| `--worker <NAME>` | | `worker1` | Worker name for Stratum pool authentication. |
| `--password <PWD>` | | `x` | Worker password for Stratum pool authorization. |

Terminal Output:

```text
============================================================
   Kovanica Standalone Solo CPU Miner (kovanica-testnet)
============================================================
 Chain ID:        2
 RPC Endpoint:    http://127.0.0.1:8545
 Payout Address:  kvncqqqsyqcyq5rqwzqfpg9scrgwpugpzysnzs23v9ccrydpk8qarc07228f2dag
 Worker Threads:  4
 Poll Interval:   1000 ms
 PoW Hash:        KovanicaHash (BLAKE3 + 2 MiB mix)
 Press Ctrl-C to stop mining gracefully.
============================================================
[INFO] Fetched candidate work: round 42, target_bits 0x207fffff, parents: 1
[INFO] Solved block! round 42, nonce 192840, hash 00000abc...
[INFO] Block submitted successfully! Total accepted: 1
```

Press `Ctrl-C` at any time to shut down the worker gracefully; aggregate hash counts, runtime duration, and acceptance statistics will be summarized upon exit.

##### Production Solo Mining Runner Script (`scripts/mining/run-solo-miner.sh`)

For production Linux deployments, unattended VPS nodes, or systemd services, Kovanica provides an automated runner harness:

```bash
# Basic run with auto-detected CPU cores
./scripts/mining/run-solo-miner.sh --address kvncqqqsyqcyq5rqwzqfpg9scrgwpugpzysnzs23v9ccrydpk8qarc07228f2dag

# Customized production run
./scripts/mining/run-solo-miner.sh \
  --address kvncqqqsyqcyq5rqwzqfpg9scrgwpugpzysnzs23v9ccrydpk8qarc07228f2dag \
  --threads 8 \
  --rpc http://seed1.kovanica.online:8545 \
  --log-file /var/log/kovanica/miner.log
```

Key features of the production runner:
- **Topology Autodetection:** Automatically detects physical and logical CPU cores (`nproc`) if `--threads` is omitted.
- **Address Validation:** Pre-validates canonical PRO-21 address syntax (`kvnc...dag`, 64 characters) prior to spawning miner threads.
- **Release Compilation Check:** Auto-detects `target/release/kovanica` or falls back to `cargo build --release -p kovanica-cli` if binaries are missing.
- **Signal Forwarding:** Trap-handles `SIGINT` and `SIGTERM` to perform zero-loss graceful miner shutdown.

#### Mode B: In-Node Built-in Miner

Nodes can author blocks natively by enabling internal mining in their configuration file (`config.chain2.toml`):

```toml
[network]
chain_id = 2

[mining]
enabled = true
author = 0
threads = 4
payout_address = "kvncqqqsyqcyq5rqwzqfpg9scrgwpugpzysnzs23v9ccrydpk8qarc07228f2dag"
# Optional (for local/dev/private chains; leave false on public mainnet/testnet miners):
# on_demand = true
# on_demand_trailing_blocks = 0
```

Or passing flags to the node binary:

```bash
kovanica-node --config config.chain2.toml --mining.enabled --mining.threads 4
```

---

## 5. Stratum Mining & Pool Integration

Mining pools aggregate hashrate across multiple mining rigs and distribute block rewards proportionally among participants.

### Pool Topology

```
   +--------------------+     +--------------------+
   |   Miner Rig #1     |     |   Miner Rig #2     |
   | (Stratum Client)   |     | (Stratum Client)   |
   +---------+----------+     +---------+----------+
             | Stratum TCP              | Stratum TCP
             +------------+-------------+
                          |
                          v
             +--------------------------+
             |   Stratum Pool Bridge    |
             | (e.g. kovanica-stratum)  |
             +------------+-------------+
                          | JSON-RPC (HTTP)
                          | kovanica_getWork / kovanica_submitWork
                          v
             +--------------------------+
             |   Kovanica Node (RPC)    |
             |   (Chain ID 2 / Testnet) |
             +--------------------------+
```

### Stratum V1 Protocol Mapping

The Stratum pool bridge translates a Stratum V1-style TCP session into Kovanica JSON-RPC calls. The shipped `kovanica-stratum` gateway and the built-in `kovanica-cli` Stratum miner are designed to work together. A third-party miner must implement the `kovanicahash-v1` profile: it must be able to compute KovanicaHash over the canonical DAG-aware preimage and verify the capability advertised in the `mining.subscribe` response. Generic SHA-256 ASIC/GPU Stratum V1 firmware is not supported because Kovanica headers are not Bitcoin-style 80-byte headers with a coinbase/merkle tree.

#### 1. Connection & Subscription (`mining.subscribe`)

The miner connects via TCP and requests session parameters:

```json
{"id": 1, "method": "mining.subscribe", "params": ["kovanica-miner/0.4.0", "kovanicahash-v1"]}
```

Pool response:

```json
{
  "id": 1,
  "result": [
    [["mining.set_difficulty", "session_sub_id"], ["mining.notify", "notify_sub_id"]],
    "01a4b8",
    4,
    {"algorithm": "kovanicahash", "profile": "kovanicahash-v1", "version": 1}
  ],
  "error": null
}
```
- `01a4b8`: Allocated `extranonce1` (assigned uniquely per client connection).
- `4`: `extranonce2_size` in bytes.
- The fourth result element advertises the required algorithm and profile. A miner that cannot evaluate KovanicaHash must disconnect instead of sending unusable shares. Legacy gateways without this field are still accepted by the built-in client.

#### 2. Worker Authorization (`mining.authorize`)

```json
{"id": 2, "method": "mining.authorize", "params": ["kvnc...dag.worker1", "x"]}
```

Pool response:

```json
{"id": 2, "result": true, "error": null}
```

#### 3. Share Difficulty Adjustment (`mining.set_difficulty`)

The pool sends the configured static share difficulty. Variable difficulty based on measured worker hashrate is not implemented yet:

```json
{"id": null, "method": "mining.set_difficulty", "params": [0.05]}
```

#### 4. Job Notification (`mining.notify`)

When new DAG tips arrive at the node (detected via `kovanica_getWork`), the pool broadcasts a new job:

The gateway emits the standard 9-parameter `mining.notify` layout. For Kovanica jobs, `coinb1` carries the canonical header preimage prefix (everything except the trailing 8-byte nonce); `coinb2` and `merkle_branch` are empty.

```json
{
  "id": null,
  "method": "mining.notify",
  "params": [
    "job_42a",
    "0000000000000000000000000000000000000000000000000000000000000000",
    "...canonical preimage prefix hex...",
    "",
    [],
    "00001092",
    "207fffff",
    "673a5f20",
    true
  ]
}
```

- Parameter 1: `job_id`.
- Parameter 2: First DAG parent digest (or zero hash).
- Parameter 3: `coinb1` — Kovanica header preimage prefix, used by the built-in client.
- Parameter 4: `coinb2` — empty.
- Parameter 5: `merkle_branch` — empty.
- Parameter 6: `version` — DAG round (hex).
- Parameter 7: `nbits` — network compact difficulty bits.
- Parameter 8: `ntime` — template timestamp (hex).
- Parameter 9: `clean_jobs` — always `true` for a new template.

#### 5. Share Submission (`mining.submit`)

When a worker finds a nonce meeting the assigned pool share target, it submits:

```json
{
  "id": 4,
  "method": "mining.submit",
  "params": [
    "kvnc...dag.worker1",
    "job_42a",
    "00000001",
    "673a5f20",
    "0000000100000042"
  ]
}
```
- Param 1: worker username (`<payout address>[.worker]`).
- Param 2: `job_id`.
- Param 3: `extranonce2` (hex, 4 bytes).
- Param 4: `ntime` (hex); accepted but ignored by the gateway.
- Param 5: 64-bit nonce (hex) assembled as `(extranonce2 << 32) | nonce`. The gateway also accepts the legacy 4-parameter form without `ntime`.

### Share Validation & Network Block Submission

1. **Pool Share Verification:**
   The gateway reconstructs the header from its cached template, sets `nonce = (extranonce2 << 32) | nonce`, replaces the payout with the address from the authorized worker username, computes `KovanicaHash`, and checks it against the assigned share target. `extranonce1` is a session identifier only and is not part of the hashed preimage. Duplicate shares are tracked in memory per connection.
2. **Network Solution Check:**
   If the hash also satisfies the full network target, the gateway submits the solved header to the local node via `kovanica_submitWork`. Reward accounting and payout distribution (PPLNS/PPS) are not implemented; accepted-share/block counters are in-memory statistics only.

### Running the Stratum V1 Gateway (`kovanica-stratum`)

The `kovanica-stratum` binary serves as the Stratum V1 TCP gateway for the built-in Kovanica miner:

```bash
# Launch Stratum V1 gateway on port 3333 connecting to local node JSON-RPC
kovanica-stratum \
  --listen 0.0.0.0:3333 \
  --rpc-url http://127.0.0.1:8545 \
  --difficulty 1.0 \
  --chain-id 2
```

##### Command-Line Options:
| Flag | Default | Description |
|---|---|---|
| `--listen <IP:PORT>` | `0.0.0.0:3333` | Stratum TCP listener address. |
| `--rpc-url <URL>` | `http://127.0.0.1:8545` | Upstream Kovanica node JSON-RPC endpoint. |
| `--difficulty <DIFF>` | `1.0` | Initial share difficulty assigned to connected miners. |
| `--chain-id <ID>` | `2` | Target network chain ID (1 = Mainnet, 2 = Testnet). |
| `--payout-address <ADDR>` | Canonical burn address | Pool payout address (`kvnc...dag`) used when fetching work; must be 64-character canonical PRO-21. |
| `--poll-interval-ms <MS>` | `500` | Upstream work template polling interval in milliseconds. |

### Mining via CLI with Stratum Client

Miners can connect directly to any Kovanica Stratum V1 pool endpoint using the built-in Stratum client:

```bash
kovanica miner run \
  --stratum pool.kovanica.online:3333 \
  --address kvncqqqsyqcyq5rqwzqfpg9scrgwpugpzysnzs23v9ccrydpk8qarc07228f2dag \
  --worker rig01 \
  --password x \
  --threads 8
```

Terminal Output:

```text
============================================================
   Kovanica Stratum V1 CPU Miner (kovanica-testnet)
============================================================
 Stratum Endpoint: pool.kovanica.online:3333
 Worker Identity:  kvncqqq...dag.rig01
 Worker Threads:   8
 PoW Hash:         KovanicaHash (BLAKE3 + 2 MiB mix)
 Press Ctrl-C to stop mining gracefully.
============================================================
[INFO] Connected to Stratum pool at pool.kovanica.online:3333
[INFO] Subscribed: extranonce1=01a4b8, extranonce2_size=4
[INFO] Authorized worker kvncqqq...dag.rig01
[INFO] Set difficulty target: 0.05
[INFO] Received new job: job_42a (clean: true)
[INFO] Share accepted by pool! (Total accepted: 1, rejected: 0)
```

---

## 6. Mining JSON-RPC Interfaces

The Kovanica node exposes standard JSON-RPC 2.0 endpoints for mining operations:

### `kovanica_getWork`

Fetches candidate work templates for the next DAG block.

- **Parameters:** `[payout_address: string]` (required on hybrid chains) or `{"address": "string", "author": optional_u16}`
- **Response:**
  ```json
  {
    "chain_id": 2,
    "round": 42,
    "author": 0,
    "payout_address": "kvnc...",
    "parents": ["0x55aa..."],
    "parent_refs": [{"digest": "0x55aa...", "round": 41}],
    "merkle_root": "0x3344...",
    "timestamp": 1700000000000,
    "target_bits": "0x207fffff",
    "target": "0x000007ffff...",
    "preimage": "0x4b564e432d504f572d4844522d7631...",
    "transactions_count": 0,
    "header": { ... }
  }
  ```

### `kovanica_submitWork`

Submits a solved block header containing a valid nonce and PoW solution.

- **Parameters:** `[solved_header: object]`
- **Response:** `true` on successful acceptance and network broadcast; error object if rejected.

### `kovanica_getMiningInfo`

Returns network difficulty, current target bits, estimated network hashrate, and active tips.

- **Parameters:** `[]`
- **Response:**
  ```json
  {
    "chain_id": 5,
    "mining_enabled": true,
    "current_round": 42,
    "target_bits": "0x207fffff",
    "difficulty": 1.0,
    "network_hashrate_estimate_hps": 12500.0,
    "active_tips_count": 3
  }
  ```

### `kovanica_getDagTips`

Inspects the current frontier tips of the GHOSTDAG block DAG.

For detailed RPC schemas and error handling, refer to [RPC Documentation](RPC.md).

---

## 7. Hardware Guidelines & Troubleshooting

### Memory Sizing & VPS Resource Constraints

- **Scratchpad Footprint:** Exactly 2 MiB per worker thread context.
  - 1 thread = 2 MiB RAM.
  - 16 threads = 32 MiB RAM.
  - 64 threads = 128 MiB RAM.
- **Node Memory Footprint:** The Kovanica node and GHOSTDAG state comfortably fit within a **2–4 GB RAM** VPS footprint.
- **Thread Allocation Strategy:** Set worker threads to physical CPU cores or logical cores. Because KovanicaHash repeatedly accesses a 2 MiB memory buffer, CPUs with large L3 caches (e.g. AMD 3D V-Cache with 96 MB L3) can host dozens of threads simultaneously in fast cache without DDR bus saturation.

### Minimizing DAG Orphan & Stale Rates

With a 4-second block time:
1. **Network Latency:** Ensure your mining node maintains high-speed peer connections to testnet seed nodes (see [Testnet Seed Nodes](TESTNET-SEED-NODES.md)).
2. **Polling Frequency:** When running the standalone miner, configure `--poll-interval-ms` between `500` and `1000` ms to ensure work templates switch promptly when peers produce blocks.
3. **Time Synchronization:** Run `systemd-timesyncd` or `chrony` on your VPS to ensure timestamps remain within the allowable DAG tolerance.

### Common Error Codes & Diagnostics

| Symptom / Error | Root Cause | Remediation |
|---|---|---|
| `invalid payout address for kovanica-testnet` | Payout address is corrupted or invalid checksum. | Verify address was generated via `kovanica keygen` and starts with `kvnc` and ends with `dag`. |
| `stale block header / parent not in DAG` | Work template was solved after a new round committed. | Lower `--poll-interval-ms` (e.g. 500 ms) and optimize node P2P connectivity. |
| `pow hash does not meet target` | Nonce or preimage mismatch between miner and node. | Ensure miner uses `KovanicaHasher` and does not mutate preimage bytes other than the tail 8-byte nonce. |
| `requested thread count exceeds maximum` | `--threads` exceeds 256. | Reduce thread allocation below 256. |

---

## 8. Document References

- [Architecture & System Overview](ARCHITECTURE.md)
- [Hybrid PoW + GHOSTDAG Architectural Decision Record (ADR 0003)](adr/0003-hybrid-pow-ghostdag.md)
- [PoW Block Header & Mining Specification](dev/POW-HEADER-SPEC.md)
- [Hybrid Testnet M8 Milestone Spec](dev/HYBRID-TESTNET-M8.md)
- [JSON-RPC API Documentation](RPC.md)
- [Tokenomics Specification](TOKENOMICS.md)
- [Testnet Seed Nodes Runbook](TESTNET-SEED-NODES.md)
