//! TLS 서버 설정. 인증서가 없으면 데이터 디렉터리에 자체 서명 인증서를 생성한다.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

use jamsil_common::DbError;

pub fn ensure_self_signed(
    dir: &Path,
    hostnames: Vec<String>,
) -> Result<(PathBuf, PathBuf), DbError> {
    std::fs::create_dir_all(dir)?;
    let cert = dir.join("server.crt");
    let key = dir.join("server.key");
    if cert.exists() && key.exists() {
        return Ok((cert, key));
    }
    let ck = rcgen::generate_simple_self_signed(hostnames)
        .map_err(|e| DbError::internal(format!("certificate generation: {e}")))?;
    std::fs::write(&cert, ck.cert.pem())?;
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&key)?;
        f.write_all(ck.key_pair.serialize_pem().as_bytes())?;
    }
    tracing::warn!(
        "generated self-signed TLS certificate at {}",
        cert.display()
    );
    Ok((cert, key))
}

pub fn acceptor(cert: &Path, key: &Path) -> Result<TlsAcceptor, DbError> {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut std::io::BufReader::new(
        std::fs::File::open(cert).map_err(|e| DbError::io(format!("{}: {e}", cert.display())))?,
    ))
    .collect::<Result<_, _>>()
    .map_err(|e| DbError::io(format!("bad certificate: {e}")))?;
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut std::io::BufReader::new(
        std::fs::File::open(key).map_err(|e| DbError::io(format!("{}: {e}", key.display())))?,
    ))
    .map_err(|e| DbError::io(format!("bad key: {e}")))?
    .ok_or_else(|| DbError::io("no private key found"))?;
    let cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| DbError::io(format!("tls config: {e}")))?;
    Ok(TlsAcceptor::from(Arc::new(cfg)))
}
