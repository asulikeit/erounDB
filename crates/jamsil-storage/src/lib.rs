//! SQLite 기반 로컬 Storage Engine (설계서 5장, 7장, 8장).
//!
//! * 단일 Writer 스레드가 쓰기 Connection을 소유한다. 쓰기 트랜잭션은 SQLite에서 미확정 상태로
//!   유지되며 Session Extension Patchset을 추출해 Raft로 제안한다. Raft 확정 후
//!   원래 트랜잭션 + `applied_index`를 같은 SQLite COMMIT으로 묶는다.
//! * Follower는 확정 로그의 DDL과 Patchset을 같은 원자 트랜잭션으로 적용한다.
//! * Reader Pool은 WAL Snapshot 읽기를 병렬 처리한다.

pub mod catalog;
pub mod convert;
pub mod engine;
pub mod exec;
pub mod functions;
pub mod pgcatalog;
pub mod pinned;
pub mod reader;
pub mod typeval;
pub mod writer;

pub use engine::{Engine, EngineOptions, SearchProvider};
