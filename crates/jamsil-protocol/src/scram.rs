//! SCRAM-SHA-256 서버 (RFC 5802/7677, channel binding 미사용).

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::{Digest, Sha256};

use jamsil_common::DbError;

pub const DEFAULT_ITERATIONS: u32 = 4096;

#[derive(Debug, Clone)]
pub struct ScramSecret {
    pub iterations: u32,
    pub salt: Vec<u8>,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
}

fn hmac(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut m = Hmac::<Sha256>::new_from_slice(key).expect("hmac");
    m.update(msg);
    m.finalize().into_bytes().into()
}

impl ScramSecret {
    pub fn from_password(password: &str, salt: Vec<u8>, iterations: u32) -> ScramSecret {
        let mut salted = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, iterations, &mut salted);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key: [u8; 32] = Sha256::digest(client_key).into();
        let server_key = hmac(&salted, b"Server Key");
        ScramSecret {
            iterations,
            salt,
            stored_key,
            server_key,
        }
    }

    pub fn generate(password: &str) -> ScramSecret {
        let mut salt = vec![0u8; 16];
        rand::thread_rng().fill_bytes(&mut salt);
        Self::from_password(password, salt, DEFAULT_ITERATIONS)
    }

    /// PostgreSQL 형식: SCRAM-SHA-256$<iter>:<salt>$<StoredKey>:<ServerKey>
    pub fn to_pg_string(&self) -> String {
        format!(
            "SCRAM-SHA-256${}:{}${}:{}",
            self.iterations,
            B64.encode(&self.salt),
            B64.encode(self.stored_key),
            B64.encode(self.server_key)
        )
    }

    pub fn parse(s: &str) -> Result<ScramSecret, String> {
        let rest = s
            .strip_prefix("SCRAM-SHA-256$")
            .ok_or("missing SCRAM-SHA-256$ prefix")?;
        let (a, b) = rest.split_once('$').ok_or("bad format")?;
        let (iter, salt) = a.split_once(':').ok_or("bad format")?;
        let (sk, svk) = b.split_once(':').ok_or("bad format")?;
        let dec32 = |x: &str| -> Result<[u8; 32], String> {
            let v = B64.decode(x).map_err(|e| e.to_string())?;
            v.try_into().map_err(|_| "key length".to_string())
        };
        Ok(ScramSecret {
            iterations: iter.parse().map_err(|_| "bad iterations")?,
            salt: B64.decode(salt).map_err(|e| e.to_string())?,
            stored_key: dec32(sk)?,
            server_key: dec32(svk)?,
        })
    }
}

/// 서버 측 교환 상태.
pub struct ScramServer {
    secret: ScramSecret,
    /// 사용자 미존재 시에도 같은 절차를 밟고 마지막에 실패시킨다 (사용자 열거 방지).
    valid_user: bool,
    client_first_bare: String,
    server_first: String,
    nonce: String,
}

impl ScramServer {
    pub fn new(secret: Option<ScramSecret>) -> ScramServer {
        let valid_user = secret.is_some();
        let secret = secret
            .unwrap_or_else(|| ScramSecret::generate(&format!("{:x}", rand::random::<u128>())));
        ScramServer {
            secret,
            valid_user,
            client_first_bare: String::new(),
            server_first: String::new(),
            nonce: String::new(),
        }
    }

    /// client-first-message → server-first-message
    pub fn first(&mut self, client_first: &[u8]) -> Result<Vec<u8>, DbError> {
        let s = std::str::from_utf8(client_first)
            .map_err(|_| DbError::protocol("invalid SCRAM message"))?;
        // gs2 header: "n,," 또는 "y,," (channel binding 미사용)
        let bare = if let Some(r) = s.strip_prefix("n,,") {
            r
        } else if let Some(r) = s.strip_prefix("y,,") {
            r
        } else if s.starts_with("p=") {
            return Err(DbError::new(
                "",
                "28000",
                "SCRAM channel binding is not supported",
            ));
        } else {
            return Err(DbError::protocol("invalid SCRAM gs2 header"));
        };
        let cnonce = bare
            .split(',')
            .find_map(|kv| kv.strip_prefix("r="))
            .ok_or_else(|| DbError::protocol("SCRAM nonce missing"))?;
        let mut sn = [0u8; 18];
        rand::thread_rng().fill_bytes(&mut sn);
        self.nonce = format!("{cnonce}{}", B64.encode(sn));
        self.client_first_bare = bare.to_string();
        self.server_first = format!(
            "r={},s={},i={}",
            self.nonce,
            B64.encode(&self.secret.salt),
            self.secret.iterations
        );
        Ok(self.server_first.as_bytes().to_vec())
    }

    /// client-final-message → server-final-message (검증 실패 시 Err)
    pub fn finish(&mut self, client_final: &[u8], user: &str) -> Result<Vec<u8>, DbError> {
        let s = std::str::from_utf8(client_final)
            .map_err(|_| DbError::protocol("invalid SCRAM message"))?;
        let (without_proof, proof) = s
            .rsplit_once(",p=")
            .ok_or_else(|| DbError::protocol("SCRAM proof missing"))?;
        let nonce = without_proof
            .split(',')
            .find_map(|kv| kv.strip_prefix("r="))
            .ok_or_else(|| DbError::protocol("SCRAM nonce missing"))?;
        if nonce != self.nonce {
            return Err(DbError::auth_failed(user));
        }
        let proof = B64.decode(proof).map_err(|_| DbError::auth_failed(user))?;
        if proof.len() != 32 {
            return Err(DbError::auth_failed(user));
        }
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof
        );
        let client_sig = hmac(&self.secret.stored_key, auth_message.as_bytes());
        let mut client_key = [0u8; 32];
        for (k, (p, c)) in client_key
            .iter_mut()
            .zip(proof.iter().zip(client_sig.iter()))
        {
            *k = p ^ c;
        }
        let computed: [u8; 32] = Sha256::digest(client_key).into();
        // 상수 시간 비교
        let diff = computed
            .iter()
            .zip(self.secret.stored_key.iter())
            .fold(0u8, |d, (a, b)| d | (a ^ b));
        if diff != 0 || !self.valid_user {
            return Err(DbError::auth_failed(user));
        }
        let server_sig = hmac(&self.secret.server_key, auth_message.as_bytes());
        Ok(format!("v={}", B64.encode(server_sig)).into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7677 테스트 벡터 (user, pencil)
    #[test]
    fn rfc7677_vector() {
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let secret = ScramSecret::from_password("pencil", salt, 4096);
        let mut s = ScramServer::new(Some(secret.clone()));
        s.first(b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO").unwrap();
        // 서버 nonce를 테스트 벡터로 고정
        s.nonce = "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0".into();
        s.server_first = format!("r={},s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096", s.nonce);
        let fin = s
            .finish(
                b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
                "user",
            )
            .unwrap();
        assert_eq!(
            String::from_utf8(fin).unwrap(),
            "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
        );
        let round = ScramSecret::parse(&secret.to_pg_string()).unwrap();
        assert_eq!(round.stored_key, secret.stored_key);
    }
}
