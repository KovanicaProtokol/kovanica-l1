# Design: Stratum V1 miner compatibility

> **One-line summary:** Define what it would take to connect real mining hardware to Kovanica through Stratum V1, and why off-the-shelf SHA-256 ASIC/GPU firmware cannot mine Kovanica without a consensus/PoW change.

**Status:** Draft — design note, not a numbered RFC. Governance may assign an RFC number later.

**Consensus impact:** none — no change to block header, PoW verification, DAA, DAG rules, or validation. **Layer:** client-only / mining infrastructure; no ledger-state change.

**Baseline:** `origin/main` `30b6e6e5c60feb4d4f1dfb4a0cad02e3f0df26dd`.

## 1. Motivation

`kovanica-stratum` already exposes a Stratum V1-style TCP endpoint and the built-in `kovanica-cli` Stratum miner can connect to it. PR #321 aligns those two implementations on a single Kovanica job profile: the gateway sends the canonical header preimage prefix in `coinb1`, the miner hashes that same preimage, and both derive the 64-bit nonce identically from `extranonce2` and `nonce`.

The remaining question is whether *generic* Stratum V1 miners — commercial ASICs or GPU mining software — can join the pool. This document records the answer and the conditions under which broader compatibility is possible. It intentionally does **not** propose changing Kovanica's PoW.

## 2. Constraints from the protocol

Three properties of Kovanica make a direct wire-protocol shim insufficient:

1. **PoW algorithm.** Kovanica uses KovanicaHash, a custom 2 MiB memory-hard BLAKE3 construction (`docs/dev/POW-HEADER-SPEC.md` §4, `crates/kovanica-types/src/pow.rs`). Standard Stratum V1 ASIC/GPU firmware implements a fixed algorithm — typically SHA-256 — and has no way to evaluate KovanicaHash.
2. **Header shape.** A Kovanica PoW preimage is a variable-length, domain-separated byte string containing DAG parent references, round, author, timestamp, target bits, and a 64-bit nonce (`docs/dev/POW-HEADER-SPEC.md` §2). It is not a Bitcoin-style 80-byte header with a coinbase transaction and merkle root.
3. **Miner-built coinbase.** In Bitcoin Stratum V1, the miner builds a coinbase transaction from `coinb1 + extranonce1 + extranonce2 + coinb2` and computes a merkle root from `merkle_branch`. Kovanica has no such coinbase/merkle structure in the PoW preimage; the gateway reconstructs the full header from its own cached template and only uses the submitted nonce and authorized payout address.

A miner that cannot compute KovanicaHash cannot produce a hash that satisfies Kovanica's target. Accepting its SHA-256 "shares" would only create misleading telemetry; those shares could not be converted into valid Kovanica blocks.

This is consistent with the project's stated ASIC-resistance goal (`docs/AUDIT-SCOPE.md`) and with `docs/MINING.md`, which now explicitly limits the shipped gateway to the built-in Kovanica miner.

## 3. Non-goals

- No change to KovanicaHash, the block header format, compact target representation, or node validation.
- No claim that generic SHA-256 ASIC/GPU Stratum V1 firmware is supported.
- No pool accounting system (PPLNS/PPS), persistent share database, or payout executor.
- No TLS, rate limiting, per-connection resource caps, or variable difficulty in this design note. Those are separate hardening tasks.

## 4. Viable compatibility paths

### Path A — KovanicaHash-capable Stratum V1 profile (recommended)

Define a Kovanica-specific Stratum V1 profile for miner software that *can* evaluate KovanicaHash. This is the only path that preserves consensus and still allows remote hashrate.

**Required miner capability**

- Implement KovanicaHash exactly as specified in `docs/dev/POW-HEADER-SPEC.md` §4: BLAKE3 seed phase, 2 MiB XOF scratchpad, 1,024 mixing rounds, and keyed BLAKE3 finalize. Byte order and domain tags must match the Rust implementation.
- Reconstruct the canonical preimage from the job fields below, append/overwrite the 8-byte little-endian nonce slot, and hash the result.
- Interpret `nbits` using Kovanica compact-target conversion, not Bitcoin's double-SHA256 difficulty convention.
- Submit the resulting nonce using the Kovanica submit layout.

**Job layout (`mining.notify`)**

The existing 9-parameter wire layout is retained; only the interpretation is Kovanica-specific:

| Index | Field | Kovanica meaning |
|---:|---|---|
| 0 | `job_id` | Opaque job identifier from the gateway. |
| 1 | `prevhash` | First DAG parent digest (informational; the full parent list is in the cached template). |
| 2 | `coinb1` | Hex of the canonical header preimage prefix, excluding the trailing 8-byte nonce slot. |
| 3 | `coinb2` | Empty. |
| 4 | `merkle_branch` | Empty. |
| 5 | `version` | DAG round encoded as 8 hex digits. |
| 6 | `nbits` | Compact network target bits, 8 hex digits. |
| 7 | `ntime` | Template timestamp, 8 hex digits. |
| 8 | `clean_jobs` | `true` for a new template. |

**Submit layout (`mining.submit`)**

Use the five-parameter form: `[username, job_id, extranonce2, ntime, nonce]`. The gateway computes `full_nonce = (u32_from(extronce2) << 32) | u32_from(nonce)` and sets it in the cached header before hashing. `extranonce1` is a per-session identifier and is not part of the preimage. The payout address is taken from the authorized `username` (`<kvnc...dag>[.worker]`), not from the template fallback.

**Acceptance criteria for a future implementation PR**

- A KovanicaHash-capable miner simulator (not a generic SHA-256 miner) completes subscribe → authorize → notify → submit against `kovanica-stratum` and produces a header accepted by `kovanica_submitWork`.
- The test asserts the submitted header's nonce, payout, target bits, and PoW hash, and covers duplicate/short-job rejection.
- The subscribe handshake advertises and verifies the `kovanicahash-v1` profile; the built-in client rejects a mismatched profile.
- `docs/MINING.md` lists the required miner capability explicitly and does not describe generic ASIC/GPU support.

### Path B — Consensus/PoW change (not recommended)

Changing KovanicaHash to a Stratum-friendly algorithm (for example SHA-256 over an 80-byte header) would make generic miners work, but it is a consensus change: new header bytes, new validation, DAA retuning, migration/activation plan, and a break with the documented ASIC-resistance stance. It is out of scope for this design and would require an explicit governance decision and height-gated activation or re-genesis per `docs/AGENT-RULES.md` §4.

### Rejected — SHA-256 share-only proxy

A proxy could let generic miners submit SHA-256 shares while the operator separately mines real Kovanica blocks. This is rejected: the shares do not correspond to Kovanica PoW, cannot be converted into valid blocks, and would misrepresent pool hashrate and miner rewards.

## 5. Backwards compatibility

- The Stratum V1 *wire framing* (newline-delimited JSON-RPC, standard method names, 9-param `mining.notify`) is unchanged.
- The *meaning* of `coinb1`, `version`, `nbits`, and `ntime` is Kovanica-specific. A generic miner that assumes Bitcoin semantics will produce unusable hashes.
- The five-parameter `mining.submit` is used by the built-in client; the gateway also accepts the legacy four-parameter form without `ntime`.
- Existing built-in-miner deployments from PR #321 remain compatible; Path A adds capabilities rather than replacing the profile.

## 6. Open questions

1. Is there an existing GPU/FPGA miner that can implement KovanicaHash with its 2 MiB scratchpad, or must one be written?
2. **Resolved:** the gateway advertises `{"algorithm":"kovanicahash","profile":"kovanicahash-v1","version":1}` as the fourth `mining.subscribe` result element, and the built-in client sends/verifies the `kovanicahash-v1` token. Miners that cannot evaluate KovanicaHash must reject a mismatched profile; legacy responses without the field remain accepted for backwards compatibility.
3. How should variable difficulty be assigned once real hashrate is measurable? Current behavior accepts miner-suggested difficulty, which is not safe for a public pool.
4. Where should share accounting and payout state live if the pool becomes multi-user? Current counters are in-memory statistics only.
5. Should `kovanica_getWork` gain a Stratum-specific template mode, or is the current full-header template sufficient for Path A?

## 7. Related documents

- [`docs/MINING.md`](../MINING.md) — operator-facing mining and Stratum instructions.
- [`docs/dev/POW-HEADER-SPEC.md`](../dev/POW-HEADER-SPEC.md) — canonical header preimage and KovanicaHash specification.
- [`docs/AUDIT-SCOPE.md`](../AUDIT-SCOPE.md) — KovanicaHash ASIC-resistance rationale.
- [`docs/AGENT-RULES.md`](../AGENT-RULES.md) — frozen values and consensus-change discipline.
