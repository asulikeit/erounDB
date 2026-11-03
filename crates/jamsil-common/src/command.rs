//! Raft 로그에 기록되는 복제 명령 (설계서 9.3 ReplicatedCommand).

use serde::{Deserialize, Serialize};

/// 하나의 Raft Entry 페이로드. 항상 하나의 완전한 논리 명령이다.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ReplicatedCommand {
    /// Leader 선출 직후 현재 임기 확정을 위한 빈 명령.
    Noop,
    /// 하나의 SQL 트랜잭션 결과 (DDL + Row Changeset).
    Txn(TxnPayload),
    /// 멱등 키(jamsil.request_id)가 있는 트랜잭션: 같은 원자 Apply에서 요청 이력과 결과를 기록한다.
    /// (기존 Raft 로그 호환을 위해 enum 끝에 추가)
    TxnWithRequest { payload: TxnPayload, request: RequestRecord },
}

impl ReplicatedCommand {
    pub fn payload(&self) -> Option<&TxnPayload> {
        match self {
            ReplicatedCommand::Noop => None,
            ReplicatedCommand::Txn(p) => Some(p),
            ReplicatedCommand::TxnWithRequest { payload, .. } => Some(payload),
        }
    }
}

/// 멱등 요청 이력 (설계서 6.4: 같은 Request ID 재요청 시 저장된 결과 조회).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RequestRecord {
    pub request_id: String,
    /// 문장+파라미터 지문 (같은 키를 다른 요청에 재사용하면 거부). 명시적 트랜잭션은 "txn".
    pub fingerprint: String,
    /// bincode(QueryResult): 재시도 시 그대로 돌려줄 결과 (RETURNING 행 포함)
    #[serde(with = "serde_bytes")]
    pub result: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TxnPayload {
    pub tx_id: u128,
    /// 클라이언트 멱등 키 (SET jamsil.request_id).
    pub request_id: Option<String>,
    /// 적용 전 기대 스키마 버전.
    pub schema_version: u64,
    /// 결정적으로 재실행 가능한 DDL/시스템 문장 (순서대로 먼저 실행).
    pub ddl: Vec<String>,
    /// SQLite Session Extension changeset.
    #[serde(with = "serde_bytes")]
    pub changeset: Vec<u8>,
    /// 결과 태그 요약 (멱등 조회용).
    pub tag: String,
    /// 전체 페이로드 체크섬 (ddl + changeset), crc32.
    pub checksum: u32,
}

impl TxnPayload {
    pub fn compute_checksum(ddl: &[String], changeset: &[u8]) -> u32 {
        let mut h = crc32fast::Hasher::new();
        for d in ddl {
            h.update(&(d.len() as u64).to_le_bytes());
            h.update(d.as_bytes());
        }
        h.update(changeset);
        h.finalize()
    }

    pub fn verify(&self) -> bool {
        Self::compute_checksum(&self.ddl, &self.changeset) == self.checksum
    }

    pub fn payload_bytes(&self) -> usize {
        self.ddl.iter().map(|d| d.len()).sum::<usize>() + self.changeset.len()
    }

    pub fn has_ddl(&self) -> bool {
        !self.ddl.is_empty()
    }
}
