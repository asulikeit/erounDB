//! 노드 간 (TCP 9787) Raft RPC 메시지.

use serde::{Deserialize, Serialize};

use jamsil_common::{DbError, NodeId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireEntry {
    pub index: u64,
    pub term: u64,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerRequest {
    AppendEntries {
        term: u64,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<WireEntry>,
        leader_commit: u64,
        round: u64,
    },
    RequestVote {
        term: u64,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
        pre_vote: bool,
    },
    InstallSnapshot {
        term: u64,
        leader_id: NodeId,
        last_included_index: u64,
        last_included_term: u64,
        offset: u64,
        total_size: u64,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
        done: bool,
    },
    /// Follower → Leader: Linearizable Read Index.
    ReadIndex,
    /// Follower → Leader: 클라이언트 요청 전달 (응용 계층 bincode).
    Forward(#[serde(with = "serde_bytes")] Vec<u8>),
    /// Leader → Follower 생존 확인. Raft 코어를 거치지 않고 전송 계층에서 즉시 응답한다
    /// (Follower가 대형 Entry fsync 중이어도 Check-quorum이 오판하지 않도록). 복제·확정에는 쓰지 않는다.
    /// `core_stall_ms`: Leader Raft 코어가 마지막으로 진행한 뒤 지난 시간. 이 값이 한도 안이면
    /// Follower는 코어가 느린(대형 Entry 처리 중) Leader를 살아 있다고 보고 선거를 미룬다.
    Ping { term: u64, core_stall_ms: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PeerResponse {
    Append {
        term: u64,
        success: bool,
        match_index: u64,
        conflict_index: u64,
        round: u64,
    },
    Vote {
        term: u64,
        granted: bool,
    },
    Snapshot {
        term: u64,
        next_offset: u64,
        installed: bool,
    },
    ReadIndex(Result<u64, DbError>),
    Forward(#[serde(with = "serde_bytes")] Vec<u8>),
    Pong { term: u64 },
    Error(DbError),
}

/// 연결 핸드셰이크 (평문 TCP 위). cluster_secret이 설정되면 HMAC-SHA256으로 상호 인증.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub cluster_id: String,
    pub from: NodeId,
    pub to: NodeId,
    pub protocol_version: u32,
    pub nonce: u64,
    pub ts_ms: u64,
    pub mac: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HelloReply {
    Ok { node_id: NodeId, mac: String },
    Rejected(String),
}
