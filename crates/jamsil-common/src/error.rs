use serde::{Deserialize, Serialize};
use std::fmt;

pub type DbResult<T> = Result<T, DbError>;

/// jamsilDB 오류. `code`는 JDB-xxxx 벤더 코드(없으면 빈 문자열), `sqlstate`는 PG SQLSTATE.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DbError {
    pub code: String,
    pub sqlstate: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    /// 서버가 FATAL로 연결을 끊어야 하는 오류인지.
    pub fatal: bool,
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.code.is_empty() {
            write!(f, "[{}] {}", self.sqlstate, self.message)
        } else {
            write!(f, "{} [{}] {}", self.code, self.sqlstate, self.message)
        }
    }
}

impl std::error::Error for DbError {}

impl DbError {
    pub fn new(code: &str, sqlstate: &str, message: impl Into<String>) -> Self {
        DbError {
            code: code.to_string(),
            sqlstate: sqlstate.to_string(),
            message: message.into(),
            detail: None,
            hint: None,
            fatal: false,
        }
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn fatal(mut self) -> Self {
        self.fatal = true;
        self
    }

    /// 사용자에게 보이는 메시지 (벤더 코드 접두).
    pub fn display_message(&self) -> String {
        if self.code.is_empty() {
            self.message.clone()
        } else {
            format!("{}: {}", self.code, self.message)
        }
    }

    // ---- 설계서 11.3 에러 코드 ----
    pub fn auth_failed(user: &str) -> Self {
        Self::new(
            "JDB-1001",
            "28P01",
            format!("password authentication failed for user \"{user}\""),
        )
        .fatal()
    }
    pub fn no_quorum() -> Self {
        Self::new(
            "JDB-2001",
            "57P03",
            "no quorum: cluster cannot commit or serve linearizable reads",
        )
        .with_hint("retry the whole transaction later")
    }
    pub fn leader_changed(detail: impl Into<String>) -> Self {
        Self::new(
            "JDB-2002",
            "40001",
            format!("leader changed: {}", detail.into()),
        )
        .with_hint("reconnect and restart the transaction")
    }
    pub fn commit_unknown(detail: impl Into<String>) -> Self {
        Self::new(
            "JDB-2003",
            "08007",
            format!("commit outcome unknown: {}", detail.into()),
        )
        .with_hint("query the result with the idempotency key (jamsil.request_id)")
    }
    pub fn store_recovery_required(detail: impl Into<String>) -> Self {
        Self::new(
            "JDB-3001",
            "57P03",
            format!("store recovery required: {}", detail.into()),
        )
        .with_hint("use another node")
    }
    pub fn text_too_large(table: &str, column: &str, bytes: usize, max: usize) -> Self {
        Self::new(
            "JDB-3101",
            "54000",
            format!("TEXT exceeds 32MiB ({max} bytes): {table}.{column} is {bytes} bytes"),
        )
    }
    pub fn mutation_too_large(bytes: usize, max: usize) -> Self {
        Self::new(
            "JDB-3102",
            "54000",
            format!("transaction mutation payload exceeds 64MiB: {bytes} > {max} bytes"),
        )
        .with_hint("split the transaction")
    }
    pub fn row_too_large(detail: impl Into<String>) -> Self {
        Self::new(
            "JDB-3103",
            "54000",
            format!(
                "SQLite row size exceeds configured limit: {}",
                detail.into()
            ),
        )
    }
    pub fn search_index_missing(detail: impl Into<String>) -> Self {
        Self::new(
            "JDB-4101",
            "42704",
            format!("search index missing: {}", detail.into()),
        )
        .with_hint("CREATE SEARCH INDEX ... ON table(column)")
    }
    pub fn like_on_text() -> Self {
        Self::new(
            "JDB-4102",
            "0A000",
            "LIKE is not supported for TEXT; use TEXT_MATCH().",
        )
    }
    pub fn search_lag_timeout(want: u64, have: u64) -> Self {
        Self::new(
            "JDB-4103",
            "57014",
            format!("search index lag timeout: required index {want}, visible {have}"),
        )
        .with_hint("use eventual consistency or retry")
    }
    /// 검색 후보가 상한을 넘어 완전한 결과를 만들 수 없음 (불완전 결과를 성공으로 돌려주지 않는다).
    pub fn search_too_many_candidates(total: usize, limit: usize) -> Self {
        Self::new(
            "JDB-4104",
            "54000",
            format!("search matched {total} documents, more than the candidate limit {limit}"),
        )
        .with_hint("narrow the TEXT_MATCH query or raise [search] max_candidates")
    }
    /// 고정 Snapshot(REPEATABLE READ/SERIALIZABLE) 트랜잭션의 검색 거부.
    /// 검색 인덱스는 현재 상태만 있어 Snapshot 시점의 문서 버전과 맞출 수 없다 (틀린 결과 대신 오류).
    pub fn search_in_snapshot_txn(isolation: &str) -> Self {
        Self::new(
            "JDB-4105",
            "0A000",
            format!(
                "TEXT_MATCH/TEXT_SCORE is not supported in {} transactions: the search index cannot be read as of the transaction snapshot",
                isolation.to_ascii_uppercase()
            ),
        )
        .with_hint("run the search outside the transaction block or in a READ COMMITTED transaction")
    }
    pub fn unsupported(feature: impl Into<String>) -> Self {
        Self::new(
            "JDB-5101",
            "0A000",
            format!("unsupported SQL feature: {}", feature.into()),
        )
    }

    // ---- 일반 오류 ----
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new("", "XX000", msg)
    }
    pub fn io(msg: impl Into<String>) -> Self {
        Self::new("", "58030", msg)
    }
    pub fn syntax(msg: impl Into<String>) -> Self {
        Self::new("", "42601", msg)
    }
    pub fn protocol(msg: impl Into<String>) -> Self {
        Self::new("", "08P01", msg)
    }
    pub fn unavailable(msg: impl Into<String>) -> Self {
        Self::new("", "57P03", msg)
    }
    pub fn timeout(msg: impl Into<String>) -> Self {
        Self::new("", "57014", msg)
    }
    pub fn invalid_param(msg: impl Into<String>) -> Self {
        Self::new("", "22023", msg)
    }
    pub fn in_failed_tx() -> Self {
        Self::new(
            "",
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        )
    }
    pub fn invalid_utf8() -> Self {
        Self::new("", "22021", "invalid byte sequence for encoding \"UTF8\"")
    }
    pub fn read_only(msg: impl Into<String>) -> Self {
        Self::new("", "25006", msg)
    }
}

impl From<std::io::Error> for DbError {
    fn from(e: std::io::Error) -> Self {
        DbError::io(e.to_string())
    }
}
