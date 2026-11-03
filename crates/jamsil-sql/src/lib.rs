//! SQL 호환 계층: PostgreSQL 문법 분류·검증·SQLite 변환, PG 타입 매핑, 검색 DDL.

pub mod classify;
pub mod compat;
pub mod scope;
pub mod search_ddl;
pub mod translate;
pub mod types;

pub use classify::{classify, Classified, StmtKind};
pub use translate::{
    Catalog, ColumnInfo, SearchIndexRef, TextMatch, TextMatchQuery, Translated, Translator,
};
