//! 사용자 인증 정보 (설정 파일 기반).

use std::collections::HashMap;

use jamsil_common::{Config, DbError};
use jamsil_protocol::scram::ScramSecret;

pub struct User {
    pub name: String,
    pub secret: ScramSecret,
    pub password: Option<String>,
    pub role: jamsil_common::acl::Role,
}

pub struct UserDb {
    pub users: HashMap<String, User>,
    pub method: String,
}

impl UserDb {
    pub fn from_config(cfg: &Config) -> Result<UserDb, DbError> {
        let mut users = HashMap::new();
        for u in &cfg.security.users {
            let secret = match (&u.scram_secret, &u.password) {
                (Some(s), _) => ScramSecret::parse(s).map_err(|e| {
                    DbError::invalid_param(format!("user {}: invalid scram_secret: {e}", u.name))
                })?,
                (None, Some(p)) => ScramSecret::generate(p),
                (None, None) => {
                    return Err(DbError::invalid_param(format!(
                        "user {} needs password or scram_secret",
                        u.name
                    )))
                }
            };
            users.insert(
                u.name.clone(),
                User {
                    name: u.name.clone(),
                    secret,
                    password: u.password.clone(),
                    role: u.effective_role(),
                },
            );
        }
        if users.is_empty() && cfg.security.authentication != "trust" {
            tracing::warn!("no users configured: all logins will fail (security.users is empty)");
        }
        Ok(UserDb {
            users,
            method: cfg.security.authentication.clone(),
        })
    }

    /// cleartext 비밀번호 검증 (password 방식).
    pub fn check_password(&self, user: &str, password: &str) -> bool {
        let Some(u) = self.users.get(user) else {
            return false;
        };
        if let Some(p) = &u.password {
            return constant_eq(p.as_bytes(), password.as_bytes());
        }
        let probe =
            ScramSecret::from_password(password, u.secret.salt.clone(), u.secret.iterations);
        constant_eq(&probe.stored_key, &u.secret.stored_key)
    }
}

fn constant_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut d = 0u8;
    for i in 0..a.len() {
        d |= a[i] ^ b[i];
    }
    d == 0
}
