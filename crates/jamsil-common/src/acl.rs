//! 사용자 역할과 테이블 권한 (설계서 4.4 / RQ-13).
//!
//! | 역할 | 읽기 | DML | DDL·검색 인덱스 | `jamsil_catalog.*` |
//! |---|---|---|---|---|
//! | superuser | 전체 | 전체 | 허용 | 허용 |
//! | readwrite | `tables` 범위 | `tables` 범위 | 거부 | 거부 |
//! | readonly | `tables` 범위 | 거부 | 거부 | 거부 |
//!
//! `tables`를 지정하지 않으면 모든 사용자 테이블이 범위다. `pg_catalog`/`information_schema`
//! 메타데이터 읽기는 드라이버 호환을 위해 모든 역할에 허용한다. 내부 테이블(`_jamsil*`, `sqlite_*`)은
//! superuser만 읽을 수 있다.

use std::collections::HashMap;

use crate::{Config, DbError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Superuser,
    ReadWrite,
    ReadOnly,
}

impl Role {
    pub fn parse(s: &str) -> Option<Role> {
        match s.trim().to_ascii_lowercase().as_str() {
            "superuser" => Some(Role::Superuser),
            "readwrite" | "read_write" => Some(Role::ReadWrite),
            "readonly" | "read_only" => Some(Role::ReadOnly),
            _ => None,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Role::Superuser => "superuser",
            Role::ReadWrite => "readwrite",
            Role::ReadOnly => "readonly",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Principal {
    pub name: String,
    pub role: Role,
    /// 허용 테이블 (소문자). None = 모든 사용자 테이블.
    pub tables: Option<Vec<String>>,
}

/// 관계 이름 분류 (Translator가 스키마를 정규화한 이름 기준).
#[derive(Debug, PartialEq, Eq)]
pub enum RelClass {
    /// pg_catalog / information_schema 메타데이터
    Metadata,
    /// jamsil_catalog.* 운영 뷰 (다른 세션의 질의·상태 포함)
    Operations,
    /// 내부 테이블
    Internal,
    User,
}

pub fn classify_relation(name: &str) -> RelClass {
    let n = name.to_ascii_lowercase();
    if n.starts_with("jamsil_catalog") {
        RelClass::Operations
    } else if n.starts_with("_jamsil") || n.starts_with("sqlite_") || n.starts_with("_jm_") {
        RelClass::Internal
    } else if n.starts_with("pg_") || n.starts_with("information_schema") {
        RelClass::Metadata
    } else {
        RelClass::User
    }
}

fn denied(what: String) -> DbError {
    DbError::new("", "42501", what)
}

impl Principal {
    pub fn superuser(name: &str) -> Principal {
        Principal {
            name: name.into(),
            role: Role::Superuser,
            tables: None,
        }
    }

    pub fn is_superuser(&self) -> bool {
        self.role == Role::Superuser
    }

    fn in_scope(&self, table: &str) -> bool {
        match &self.tables {
            None => true,
            Some(v) => v.iter().any(|t| t.eq_ignore_ascii_case(table)),
        }
    }

    pub fn check_read(&self, rel: &str) -> Result<(), DbError> {
        if self.is_superuser() {
            return Ok(());
        }
        match classify_relation(rel) {
            RelClass::Metadata => Ok(()),
            RelClass::User if self.in_scope(rel) => Ok(()),
            _ => Err(denied(format!("permission denied for table {rel} (user {}, role {})", self.name, self.role.name()))),
        }
    }

    pub fn check_write(&self, rel: &str) -> Result<(), DbError> {
        if self.is_superuser() {
            return Ok(());
        }
        if self.role == Role::ReadOnly {
            return Err(denied(format!(
                "permission denied: user {} has a read-only role",
                self.name
            )));
        }
        match classify_relation(rel) {
            RelClass::User if self.in_scope(rel) => Ok(()),
            _ => Err(denied(format!("permission denied for table {rel} (user {}, role {})", self.name, self.role.name()))),
        }
    }

    pub fn check_ddl(&self) -> Result<(), DbError> {
        if self.is_superuser() {
            return Ok(());
        }
        Err(denied(format!(
            "permission denied: user {} (role {}) cannot change the schema",
            self.name,
            self.role.name()
        )))
    }

    /// `has_table_privilege` 의미: privs는 쉼표로 구분한 SELECT/INSERT/UPDATE/DELETE/TRUNCATE/REFERENCES/TRIGGER.
    pub fn has_table_privilege(&self, rel: &str, privs: &str) -> bool {
        privs.split(',').any(|p| {
            let p = p.trim().to_ascii_uppercase();
            let p = p.trim_end_matches(" WITH GRANT OPTION").trim();
            match p {
                "SELECT" => self.check_read(rel).is_ok(),
                "INSERT" | "UPDATE" | "DELETE" => self.check_write(rel).is_ok(),
                // TRUNCATE·REFERENCES·TRIGGER는 스키마 수준 권한으로 본다.
                _ => self.is_superuser(),
            }
        })
    }
}

/// 설정의 사용자 목록 → 이름별 권한.
pub fn principals(cfg: &Config) -> HashMap<String, Principal> {
    cfg.security
        .users
        .iter()
        .map(|u| {
            (
                u.name.clone(),
                Principal {
                    name: u.name.clone(),
                    role: u.effective_role(),
                    tables: u
                        .tables
                        .as_ref()
                        .map(|v| v.iter().map(|t| t.to_ascii_lowercase()).collect()),
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles() {
        let rw = Principal {
            name: "app".into(),
            role: Role::ReadWrite,
            tables: Some(vec!["orders".into()]),
        };
        assert!(rw.check_read("orders").is_ok());
        assert!(rw.check_read("pg_class").is_ok());
        assert_eq!(rw.check_read("salaries").unwrap_err().sqlstate, "42501");
        assert!(rw.check_read("jamsil_catalog_active_sessions").is_err());
        assert!(rw.check_read("_jamsil_state").is_err());
        assert!(rw.check_write("orders").is_ok());
        assert!(rw.check_write("pg_class").is_err());
        assert!(rw.check_ddl().is_err());
        assert!(rw.has_table_privilege("orders", "SELECT,INSERT"));
        assert!(!rw.has_table_privilege("salaries", "SELECT"));
        let ro = Principal {
            name: "rpt".into(),
            role: Role::ReadOnly,
            tables: None,
        };
        assert!(ro.check_read("anything").is_ok());
        assert!(ro.check_write("anything").is_err());
        assert!(!ro.has_table_privilege("anything", "UPDATE"));
        assert!(Principal::superuser("x").check_ddl().is_ok());
    }
}
