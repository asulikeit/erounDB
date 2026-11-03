//! 독립 전문 검색엔진 (설계서 10장): Tantivy 역색인 + Lindera 한국어 형태소 분석.
//!
//! * 검색 인덱스는 RDB의 파생 데이터다. 노드마다 로컬 디렉터리에 저장하고 Raft로 복제하지 않는다.
//! * 한 RDB Row = 한 Tantivy 문서. 문서 키는 PK(JSON)이며 갱신은 delete+add.
//! * Checkpoint(처리한 Outbox seq, visible index, 재색인 세대)는 Tantivy commit 이후 원자적으로 기록한다.

pub mod analyzer;
pub mod index;

pub use index::{Checkpoint, Indexer, IndexerConfig, QueryHandle, SearchShared};
