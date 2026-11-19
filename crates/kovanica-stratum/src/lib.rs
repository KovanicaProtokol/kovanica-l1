//! Production-grade Stratum V1 mining pool gateway for Kovanica (`kovanica-stratum`).
//!
//! Bridges downstream Stratum V1 TCP mining rigs (e.g. ASIC/GPU/CPU miners) to an upstream
//! Kovanica full node JSON-RPC endpoint (`kovanica_getWork` / `kovanica_submitWork`), per
//! `docs/MINING.md` Section 5.

/// PoW algorithm required by the Kovanica Stratum V1 job profile.
pub const KOVANICA_STRATUM_ALGORITHM: &str = "kovanicahash";

/// Capability token advertised during `mining.subscribe` for the Kovanica job profile.
///
/// A miner must be able to evaluate KovanicaHash and reconstruct the canonical DAG-aware
/// preimage before it can produce shares that the gateway can convert into valid blocks.
pub const KOVANICA_STRATUM_CAPABILITY: &str = "kovanicahash-v1";

pub mod config;
pub mod difficulty;
pub mod gateway;
pub mod job;
pub mod protocol;
pub mod rpc;
pub mod session;

pub use config::{StratumArgs, StratumConfig};
pub use difficulty::{
    check_network_solution, check_share_hash_meets_difficulty, check_share_hash_meets_target,
    target_from_difficulty, DIFFICULTY_1_TARGET_BITS,
};
pub use gateway::StratumGateway;
pub use job::{Job, JobManager};
pub use protocol::{StratumNotification, StratumRequest, StratumResponse};
pub use rpc::{NodeRpcClient, WorkTemplate};
pub use session::{parse_and_validate_username, AuthorizedWorker, SessionRegistry, WorkerSession};
