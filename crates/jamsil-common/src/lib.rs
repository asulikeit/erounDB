//! jamsilDB 공통 타입: 오류 코드, SQL 값, 설정, 크기 상한, 복제 명령.

pub mod acl;
pub mod budget;
pub mod cli;
pub mod command;
pub mod config;
pub mod error;
pub mod fault;
pub mod limits;
pub mod metrics;
pub mod value;

pub use command::{ReplicatedCommand, RequestRecord, TxnPayload};
pub use config::Config;
pub use error::{DbError, DbResult};
pub use value::{ColumnDesc, QueryResult, Value};

/// 클러스터 내 노드 식별자.
pub type NodeId = u64;

/// 128-bit 트랜잭션/세션/요청 식별자.
pub type Id128 = u128;

pub fn new_id() -> Id128 {
    uuid::Uuid::new_v4().as_u128()
}

pub fn id_to_string(id: Id128) -> String {
    uuid::Uuid::from_u128(id).to_string()
}

/// 빌드 식별자 (scripts/build-rpm.sh가 JAMSIL_BUILD_ID/JAMSIL_RELEASE로 주입, 개발 빌드는 "dev").
pub const BUILD_ID: &str = match option_env!("JAMSIL_BUILD_ID") {
    Some(s) => s,
    None => "dev",
};
pub const RELEASE: &str = match option_env!("JAMSIL_RELEASE") {
    Some(s) => s,
    None => "dev",
};

/// 예: `0.1.0-3 (build 5b8173f-1a2b3c4d)`
pub fn build_info() -> String {
    format!("{}-{} (build {})", env!("CARGO_PKG_VERSION"), RELEASE, BUILD_ID)
}

/// 데이터 포맷 버전. 디스크 포맷이 바뀌면 증가시키고 업그레이드 경로를 제공해야 한다.
pub const FORMAT_VERSION: u32 = 1;
