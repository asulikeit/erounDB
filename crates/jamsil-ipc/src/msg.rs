//! 프로세스 간 메시지 계약 (설계서 11.2의 제안 인터페이스를 bincode로 구현).
//! 프로토콜 버전이 바뀌면 `IPC_PROTOCOL_VERSION`을 올리고 Hello에서 거부한다.

use serde::{Deserialize, Serialize};

use jamsil_common::metrics::MetricSample;
use jamsil_common::{
    ColumnDesc, DbError, Id128, NodeId, QueryResult, ReplicatedCommand, Value,
};

pub const IPC_PROTOCOL_VERSION: u32 = 2;

// ---------------------------------------------------------------------------
// jamsil-store
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SearchConsistency {
    Eventual,
    Committed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Statement {
    pub sql: String,
    pub params: Vec<Value>,
    /// 클라이언트가 지정한 파라미터 타입 OID (0 = 미지정).
    pub param_types: Vec<u32>,
    /// 실행 사용자 (권한 검사·current_user). 빈 문자열 = 내부 호출.
    pub user: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StoreRequest {
    Hello {
        protocol_version: u32,
    },
    Status,
    /// 로컬 Reader Pool 조회. `min_applied`까지 적용될 때까지 대기한다.
    Query {
        session: Id128,
        stmt: Statement,
        min_applied: u64,
        wait_timeout_ms: u64,
        search_consistency: SearchConsistency,
        /// jamsil_catalog.* 등 요청 시점에 채우는 가상 테이블.
        virtual_tables: Vec<VirtualTable>,
        /// REPEATABLE READ/SERIALIZABLE: 트랜잭션에 고정된 Snapshot에서 실행 (RQ-06).
        snapshot: Option<Id128>,
    },
    /// 트랜잭션 Snapshot 고정. 응답은 `Applied { index }` (Snapshot의 applied_index).
    SnapshotOpen {
        snapshot: Id128,
        min_applied: u64,
        wait_timeout_ms: u64,
    },
    SnapshotClose {
        snapshot: Id128,
    },
    /// 문장을 실행하지 않고 파라미터/결과 컬럼을 기술한다.
    /// `min_applied`: 확정 스키마를 보기 위해 이 index까지 적용될 때까지 대기 (RQ-02).
    Describe {
        stmt: Statement,
        min_applied: u64,
        wait_timeout_ms: u64,
    },
    /// Leader 전용: 쓰기 트랜잭션 안에서 문장 실행 (미COMMIT).
    /// `request_id`: autocommit 멱등 키 — Writer 직렬화 경계 안에서 기존 결과를 확인한다 (RQ-01).
    /// `expect_applied`: REPEATABLE READ/SERIALIZABLE 트랜잭션의 Snapshot index — 첫 쓰기 시점까지
    ///  다른 COMMIT이 있었으면 40001 (RQ-06).
    TxExec {
        txn: Id128,
        stmt: Statement,
        lock_timeout_ms: u64,
        request_id: Option<String>,
        expect_applied: Option<u64>,
    },
    /// Leader 전용: 미확정 트랜잭션의 Changeset/DDL을 추출. 변경이 없으면 None.
    TxPrepare {
        txn: Id128,
        request_id: Option<String>,
    },
    /// 미확정 트랜잭션 Rollback.
    TxAbort {
        txn: Id128,
    },
    /// 모든 미확정 트랜잭션 Rollback (Leader 교체 시).
    AbortAll,
    /// Raft 확정 로그 적용. 같은 tx_id의 Prepared 트랜잭션이 있으면 그것을 COMMIT한다.
    Apply {
        index: u64,
        term: u64,
        cmd: ReplicatedCommand,
    },
    /// 일관된 SQLite Snapshot 파일 생성.
    CreateSnapshot {
        path: String,
    },
    /// Snapshot 파일로 RDB 교체.
    InstallSnapshot {
        path: String,
        index: u64,
        term: u64,
    },
    /// 검색 Change Feed.
    OutboxRead {
        after_seq: u64,
        max_events: usize,
        max_bytes: usize,
    },
    /// 검색 프로세스가 영속 Checkpoint한 seq까지 Outbox 정리.
    OutboxAck {
        upto_seq: u64,
    },
    /// 전체 재색인용 Row Scan.
    ScanRows {
        index_name: String,
        after_pk: Option<Vec<Value>>,
        limit: usize,
        max_bytes: usize,
    },
    SearchIndexes,
    /// 멱등 키 조회.
    RequestStatus {
        request_id: String,
    },
    /// 데이터 Checksum (복제본 일치 검증용, T-01).
    Checksum,
    /// 세션의 진행 중 읽기 질의 중단 (CancelRequest / statement_timeout).
    CancelQuery { session: Id128 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualTable {
    pub name: String,
    /// (컬럼 이름, PG type OID)
    pub columns: Vec<(String, u32)>,
    pub rows: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StoreResponse {
    Hello {
        protocol_version: u32,
        node_ready: bool,
    },
    Status(StoreStatus),
    Result(QueryResult),
    Describe(DescribeResult),
    /// 복제할 명령 (변경이 없거나 이미 처리된 멱등 요청이면 None)
    Prepared(Option<ReplicatedCommand>),
    Ok,
    Applied {
        index: u64,
    },
    Snapshot(SnapshotInfo),
    Outbox(OutboxBatch),
    Rows(ScanBatch),
    SearchIndexes(Vec<SearchIndexDef>),
    RequestStatus(Option<RequestOutcome>),
    Checksum {
        applied_index: u64,
        checksum: String,
    },
}

pub type StoreResult = Result<StoreResponse, DbError>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StoreStatus {
    pub applied_index: u64,
    pub applied_term: u64,
    pub schema_version: u64,
    pub ready: bool,
    pub quarantined: Option<String>,
    pub db_bytes: u64,
    pub wal_bytes: u64,
    pub text_bytes_estimated: u64,
    pub outbox_seq: u64,
    pub search_rebuild_epoch: u64,
    pub writer_owner: Option<String>,
    pub open_transactions: usize,
    pub text_rejected_total: u64,
    pub queries_total: u64,
    pub writes_total: u64,
    /// 이 프로세스의 지표 샘플 (RQ-12)
    pub metrics: Vec<MetricSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeResult {
    pub param_types: Vec<u32>,
    pub columns: Option<Vec<ColumnDesc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotInfo {
    pub path: String,
    pub index: u64,
    pub term: u64,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestOutcome {
    pub raft_index: u64,
    pub tag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SearchIndexDef {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub pk_columns: Vec<String>,
    pub analyzer: String,
    pub scoring: String,
    /// 인덱스 정의가 생성된 Raft index (재색인 세대 판정용).
    pub created_index: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChangeOp {
    Upsert,
    Delete,
    /// 인덱스 정의 생성 → 해당 인덱스 전체 재색인 필요.
    IndexCreated,
    /// 인덱스 정의 삭제.
    IndexDropped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub seq: u64,
    pub raft_index: u64,
    pub index_name: String,
    pub op: ChangeOp,
    pub pk: Vec<Value>,
    /// Upsert일 때 색인 대상 컬럼 값 (Row를 그 시점에 읽은 결과; 이미 삭제됐으면 None).
    pub texts: Option<Vec<Option<String>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxBatch {
    pub events: Vec<ChangeEvent>,
    /// 현재 Store applied index (이 배치를 모두 처리하면 visible로 간주 가능한 index).
    pub applied_index: u64,
    /// 배치 이후 남은 이벤트가 없으면 true.
    pub drained: bool,
    /// Snapshot 설치 등으로 Outbox가 초기화된 세대. 바뀌면 전체 재색인.
    pub rebuild_epoch: u64,
    pub indexes: Vec<SearchIndexDef>,
    /// 같은 Snapshot에서 본 Outbox 최대 seq (전체 재색인 시작점).
    pub max_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanBatch {
    pub rows: Vec<(Vec<Value>, Vec<Option<String>>)>,
    pub applied_index: u64,
    pub done: bool,
}

// ---------------------------------------------------------------------------
// jamsil-sync (SQL 프로세스 → 로컬 Sync, Sync → Leader Sync 전달)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncRequest {
    Status,
    /// Linearizable Read를 위한 Read Index.
    ReadIndex,
    /// 명시적 트랜잭션 안의 문장 (Leader Writer에서 실행).
    TxExec {
        txn: Id128,
        stmt: Statement,
        expect_applied: Option<u64>,
    },
    /// 트랜잭션 확정: Prepare → Raft Quorum → Apply.
    TxCommit {
        txn: Id128,
        request_id: Option<String>,
    },
    TxRollback {
        txn: Id128,
    },
    /// Autocommit 단일 문장: 실행 + 확정을 한 번에.
    Exec {
        txn: Id128,
        stmt: Statement,
        request_id: Option<String>,
    },
    /// SQL 프로세스 세션 종료 통지.
    SessionClosed {
        txns: Vec<Id128>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncResponse {
    Status(ClusterStatus),
    ReadIndex { index: u64 },
    Result(QueryResult),
    Committed { index: u64 },
    Ok,
}

pub type SyncResult = Result<SyncResponse, DbError>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub node_id: NodeId,
    pub cluster_id: String,
    pub role: String,
    pub term: u64,
    pub leader_id: Option<NodeId>,
    pub commit_index: u64,
    pub last_log_index: u64,
    pub snapshot_index: u64,
    pub store_applied_index: u64,
    pub quorum_available: bool,
    pub leader_ready: bool,
    pub fsync_errors: u64,
    pub peers: Vec<PeerStatus>,
    pub metrics: Vec<MetricSample>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerStatus {
    pub node_id: NodeId,
    pub address: String,
    pub connected: bool,
    pub match_index: u64,
    pub next_index: u64,
    pub last_contact_ms: Option<u64>,
}

// ---------------------------------------------------------------------------
// jamsil-search
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SearchRequest {
    Status,
    /// `index_name`은 table + column으로 찾은 검색 인덱스 이름.
    /// 결과는 일치하는 **모든** 문서다. `limit`(후보 상한)을 넘으면 JDB-4104로 실패한다.
    Query {
        index_name: String,
        /// 검색 대상 필드 (TEXT_MATCH 첫 인자 컬럼). None이면 인덱스의 모든 필드.
        field: Option<String>,
        query: String,
        limit: usize,
        /// committed 모드에서 요구되는 최소 visible index (eventual이면 0).
        min_visible_index: u64,
        timeout_ms: u64,
    },
    /// 인덱스를 신규 세대로 전체 재구축.
    Rebuild,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SearchResponse {
    Status(SearchStatus),
    Hits(Vec<SearchHit>),
    Ok,
}

pub type SearchResult = Result<SearchResponse, DbError>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchStatus {
    pub state: String,
    pub visible_index: u64,
    pub store_applied_index: u64,
    pub outbox_seq: u64,
    pub indexes: Vec<SearchIndexStatus>,
    pub metrics: Vec<MetricSample>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SearchIndexStatus {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub analyzer: String,
    pub docs: u64,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    pub pk: Vec<Value>,
    pub score: f32,
}

// ---------------------------------------------------------------------------
// jamsil-log
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum LogCategory {
    System,
    Cluster,
    Query,
    Audit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecord {
    pub ts_unix_ms: u64,
    pub process: String,
    pub pid: u32,
    pub level: String,
    pub category: LogCategory,
    pub target: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogRequest {
    Records(Vec<LogRecord>),
}

// ---------------------------------------------------------------------------
// jamsild (Supervisor)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SupervisorRequest {
    Status,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessStatus {
    pub name: String,
    pub pid: Option<u32>,
    pub running: bool,
    pub restarts: u32,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SupervisorResponse {
    Status {
        node_id: NodeId,
        processes: Vec<ProcessStatus>,
    },
}
