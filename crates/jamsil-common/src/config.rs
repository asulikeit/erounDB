//! TOML 운영 설정 (설계서 12.1).

use serde::Deserialize;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

use crate::error::DbError;
use crate::limits::MIB;
use crate::NodeId;

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[derive(Default)]
pub struct Config {
    pub server: ServerConfig,
    pub network: NetworkConfig,
    pub security: SecurityConfig,
    pub database: DatabaseConfig,
    pub storage: StorageConfig,
    pub text: TextConfig,
    pub cluster: ClusterConfig,
    pub search: SearchConfig,
    pub logging: LoggingConfig,
    /// 설정 파일 경로 (로드 후 채움).
    #[serde(skip)]
    pub source_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Standalone,
    Cluster,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub name: String,
    pub node_id: NodeId,
    pub mode: Mode,
    /// UDS 소켓 디렉터리 (기본 /run/jamsildb, 개발 시 data_dir/run).
    pub run_dir: Option<PathBuf>,
    /// 자식 실행파일 위치. 없으면 jamsild와 같은 디렉터리.
    pub bin_dir: Option<PathBuf>,
    /// 정상 종료 대기 시간.
    pub shutdown_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NetworkConfig {
    pub listen_ipv4: Option<String>,
    pub listen_ipv6: Option<String>,
    pub port: u16,
    pub cluster_bind: String,
    pub access: AccessConfig,
    pub max_connections: usize,
    /// IP당 초당 신규 연결 허용 수 (0이면 무제한).
    pub connect_rate_per_ip: u32,
    pub idle_session_timeout_ms: u64,
    pub statement_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AccessConfig {
    pub default: String,
    pub allow_cidrs: Vec<String>,
    pub deny_cidrs: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SecurityConfig {
    /// scram-sha-256 | password | trust
    pub authentication: String,
    pub tls_enabled: bool,
    pub tls_required: bool,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub cluster_mtls: bool,
    /// 노드 간 9787 핸드셰이크 공유 비밀 (HMAC). 클러스터 모드에서 권장.
    pub cluster_secret: Option<String>,
    pub users: Vec<UserConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserConfig {
    pub name: String,
    /// 평문 비밀번호 (개발용). 운영에서는 scram_secret 사용 권장.
    pub password: Option<String>,
    /// SCRAM-SHA-256$<iter>:<salt>$<StoredKey>:<ServerKey> 형식.
    pub scram_secret: Option<String>,
    /// `role`을 지정하지 않으면 superuser=true → superuser, false → readwrite. `role`이 있으면 role 우선.
    #[serde(default = "default_true")]
    pub superuser: bool,
    /// superuser | readwrite | readonly (RQ-13)
    #[serde(default)]
    pub role: Option<String>,
    /// 비관리자 역할이 접근할 수 있는 테이블 목록 (생략 시 모든 사용자 테이블)
    #[serde(default)]
    pub tables: Option<Vec<String>>,
}

impl UserConfig {
    pub fn effective_role(&self) -> crate::acl::Role {
        match self.role.as_deref().and_then(crate::acl::Role::parse) {
            Some(r) => r,
            None if self.superuser => crate::acl::Role::Superuser,
            None => crate::acl::Role::ReadWrite,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DatabaseConfig {
    pub name: String,
    pub encoding: String,
    /// linearizable | local
    pub read_consistency: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub engine: String,
    pub data_dir: PathBuf,
    pub synchronous: String,
    pub journal_mode: String,
    pub encryption: String,
    pub compression: String,
    pub checksum: bool,
    pub reader_pool_size: usize,
    /// 다른 세션이 쓰기 잠금을 가진 경우 대기 한도.
    pub write_lock_timeout_ms: u64,
    /// 쓰기 트랜잭션이 유휴 상태로 잠금을 쥘 수 있는 최대 시간.
    pub idle_tx_timeout_ms: u64,
    pub limits: StorageLimits,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageLimits {
    pub sqlite_max_length_mib: usize,
    pub max_mutation_payload_mib: usize,
    pub raft_transport_frame_mib: usize,
    /// 동시에 처리 중인 대형 값(>1MiB) 총 바이트 예산.
    pub large_value_budget_mib: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TextConfig {
    pub max_size_mib: usize,
    pub storage_mode: String,
    pub like_enabled: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterConfig {
    pub cluster_id: String,
    pub mode: String,
    pub initial_voters: Vec<NodeId>,
    pub peers: Vec<PeerConfig>,
    pub heartbeat_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    /// 이 개수만큼 적용된 로그가 쌓이면 Snapshot 생성 후 로그 압축.
    pub snapshot_threshold_entries: u64,
    /// Snapshot 이후에도 Follower Catch-up용으로 남겨둘 로그 개수.
    pub snapshot_keep_entries: u64,
    /// AppendEntries 하나에 담을 최대 바이트.
    pub max_append_bytes_mib: usize,
    /// 클라이언트 요청 처리(쓰기/ReadIndex) 대기 한도.
    pub request_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    pub node_id: NodeId,
    pub address: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SearchConfig {
    pub enabled: bool,
    pub engine: String,
    pub index_dir: Option<PathBuf>,
    /// eventual | committed
    pub default_consistency: String,
    pub committed_timeout_ms: u64,
    /// TEXT_MATCH 한 번에 가져올 최대 후보 수.
    pub max_candidates: usize,
    pub indexing: SearchIndexingConfig,
    pub analyzer: SearchAnalyzerConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SearchIndexingConfig {
    pub workers: usize,
    pub max_memory_mib: usize,
    pub commit_interval_ms: u64,
    /// 한 배치에서 읽을 Outbox 이벤트 수.
    pub batch_events: usize,
    /// 한 배치에서 읽을 TEXT 총 바이트.
    pub batch_bytes_mib: usize,
    /// 문서 하나당 최대 토큰 수 (초과분은 색인하지 않음).
    pub max_tokens_per_doc: usize,
    /// 토큰 하나의 최대 바이트 길이.
    pub max_token_bytes: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SearchAnalyzerConfig {
    pub english: String,
    pub korean: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    pub directory: PathBuf,
    pub level: String,
    /// json | text
    pub format: String,
    /// 쿼리 로그 기록 여부.
    pub query_log: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            name: "jamsilDB".into(),
            node_id: 1,
            mode: Mode::Standalone,
            run_dir: None,
            bin_dir: None,
            shutdown_timeout_ms: 15_000,
        }
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        NetworkConfig {
            listen_ipv4: Some("0.0.0.0".into()),
            listen_ipv6: None,
            port: 9786,
            cluster_bind: "0.0.0.0:9787".into(),
            access: AccessConfig::default(),
            max_connections: 1000,
            connect_rate_per_ip: 0,
            idle_session_timeout_ms: 0,
            statement_timeout_ms: 0,
        }
    }
}

impl Default for AccessConfig {
    fn default() -> Self {
        AccessConfig {
            default: "allow".into(),
            allow_cidrs: vec!["0.0.0.0/0".into(), "::/0".into()],
            deny_cidrs: vec![],
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        SecurityConfig {
            authentication: "scram-sha-256".into(),
            tls_enabled: true,
            tls_required: false,
            cert_file: None,
            key_file: None,
            cluster_mtls: false,
            cluster_secret: None,
            users: vec![],
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        DatabaseConfig {
            name: "jamsil".into(),
            encoding: "UTF8".into(),
            read_consistency: "linearizable".into(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            engine: "sqlite".into(),
            data_dir: PathBuf::from("/var/lib/jamsildb"),
            synchronous: "FULL".into(),
            journal_mode: "WAL".into(),
            encryption: "none".into(),
            compression: "none".into(),
            checksum: true,
            reader_pool_size: 8,
            write_lock_timeout_ms: 30_000,
            idle_tx_timeout_ms: 300_000,
            limits: StorageLimits::default(),
        }
    }
}

impl Default for StorageLimits {
    fn default() -> Self {
        StorageLimits {
            sqlite_max_length_mib: 64,
            max_mutation_payload_mib: 64,
            raft_transport_frame_mib: 1,
            large_value_budget_mib: 512,
        }
    }
}

impl Default for TextConfig {
    fn default() -> Self {
        TextConfig {
            max_size_mib: 32,
            storage_mode: "inline".into(),
            like_enabled: false,
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig {
            cluster_id: "jamsildb-dev".into(),
            mode: "raft".into(),
            initial_voters: vec![],
            peers: vec![],
            heartbeat_ms: 150,
            election_timeout_min_ms: 1000,
            election_timeout_max_ms: 2000,
            snapshot_threshold_entries: 20_000,
            snapshot_keep_entries: 5_000,
            max_append_bytes_mib: 8,
            request_timeout_ms: 30_000,
        }
    }
}

impl Default for SearchConfig {
    fn default() -> Self {
        SearchConfig {
            enabled: true,
            engine: "tantivy".into(),
            index_dir: None,
            default_consistency: "eventual".into(),
            committed_timeout_ms: 10_000,
            max_candidates: 100_000,
            indexing: SearchIndexingConfig::default(),
            analyzer: SearchAnalyzerConfig::default(),
        }
    }
}

impl Default for SearchIndexingConfig {
    fn default() -> Self {
        SearchIndexingConfig {
            workers: 4,
            max_memory_mib: 512,
            commit_interval_ms: 1000,
            batch_events: 512,
            batch_bytes_mib: 64,
            max_tokens_per_doc: 4_000_000,
            max_token_bytes: 256,
        }
    }
}

impl Default for SearchAnalyzerConfig {
    fn default() -> Self {
        SearchAnalyzerConfig {
            english: "default".into(),
            korean: "lindera-ko-dic".into(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        LoggingConfig {
            directory: PathBuf::from("/var/log/jamsildb"),
            level: "info".into(),
            format: "json".into(),
            query_log: false,
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Config, DbError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| DbError::io(format!("cannot read config {}: {e}", path.display())))?;
        let mut cfg: Config = toml::from_str(&text).map_err(|e| {
            DbError::invalid_param(format!("invalid config {}: {e}", path.display()))
        })?;
        cfg.source_path = Some(path.canonicalize().unwrap_or_else(|_| path.to_path_buf()));
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), DbError> {
        let bad = |m: String| Err(DbError::invalid_param(m));
        if self.text.max_size_mib == 0 || self.text.max_size_mib > 32 {
            return bad("text.max_size_mib must be between 1 and 32".into());
        }
        if self.text.storage_mode != "inline" {
            return bad("text.storage_mode must be \"inline\"".into());
        }
        if self.storage.limits.sqlite_max_length_mib < self.text.max_size_mib * 2 {
            return bad(
                "storage.limits.sqlite_max_length_mib must be >= 2 * text.max_size_mib".into(),
            );
        }
        if self.storage.limits.sqlite_max_length_mib > 900 {
            return bad("storage.limits.sqlite_max_length_mib exceeds SQLite build limit".into());
        }
        if self.storage.engine != "sqlite" {
            return bad(format!(
                "unsupported storage.engine {}",
                self.storage.engine
            ));
        }
        if self.storage.encryption != "none" {
            return bad("storage.encryption other than \"none\" is not supported in 1.0".into());
        }
        match self.database.read_consistency.as_str() {
            "linearizable" | "local" => {}
            o => return bad(format!("invalid database.read_consistency {o}")),
        }
        match self.search.default_consistency.as_str() {
            "eventual" | "committed" => {}
            o => return bad(format!("invalid search.default_consistency {o}")),
        }
        match self.security.authentication.as_str() {
            "scram-sha-256" | "password" | "trust" => {}
            o => return bad(format!("invalid security.authentication {o}")),
        }
        for u in &self.security.users {
            if let Some(r) = &u.role {
                // role이 있으면 superuser 플래그보다 우선한다.
                if crate::acl::Role::parse(r).is_none() {
                    return bad(format!(
                        "user {}: role must be superuser, readwrite or readonly",
                        u.name
                    ));
                }
            }
            if u.tables.is_some() && u.effective_role() == crate::acl::Role::Superuser {
                return bad(format!(
                    "user {}: tables applies only to readwrite/readonly roles",
                    u.name
                ));
            }
        }
        // 설정해도 효과가 없는 옵션은 조용히 무시하지 않고 거부한다 (RQ-15).
        if self.security.cluster_mtls {
            return bad("security.cluster_mtls=true is not supported in this release: node-to-node traffic (9787) uses HMAC-authenticated plaintext (no encryption). Set cluster_mtls=false and isolate the cluster network".into());
        }
        if self.text.like_enabled {
            return bad("text.like_enabled=true is not supported: LIKE on TEXT is always rejected (JDB-4102); use TEXT_MATCH()".into());
        }
        if self.storage.compression != "none" {
            return bad("storage.compression other than \"none\" is not supported".into());
        }
        if !self.storage.journal_mode.eq_ignore_ascii_case("wal") {
            return bad("storage.journal_mode must be \"WAL\"".into());
        }
        if !self.storage.checksum {
            return bad("storage.checksum=false is not supported (frame/log checksums are always on)".into());
        }
        if self.storage.limits.raft_transport_frame_mib != 1 {
            return bad("storage.limits.raft_transport_frame_mib is fixed at 1 in this release".into());
        }
        if self.storage.limits.max_mutation_payload_mib < self.text.max_size_mib
            || self.storage.limits.max_mutation_payload_mib > 64
        {
            return bad("storage.limits.max_mutation_payload_mib must be between text.max_size_mib and 64".into());
        }
        if self.search.indexing.workers == 0 || self.search.indexing.workers > 16 {
            return bad("search.indexing.workers must be between 1 and 16".into());
        }
        let min_budget = self.storage.limits.max_mutation_payload_mib + 4;
        if self.storage.limits.large_value_budget_mib < min_budget
            || self.storage.limits.large_value_budget_mib > 65_536
        {
            return bad(format!(
                "storage.limits.large_value_budget_mib must be between {min_budget} (one maximum-size message) and 65536"
            ));
        }
        if self.search.max_candidates == 0 || self.search.max_candidates > 10_000_000 {
            return bad("search.max_candidates must be between 1 and 10000000".into());
        }
        if self.search.indexing.max_memory_mib < 16 {
            return bad("search.indexing.max_memory_mib must be >= 16".into());
        }
        if self.security.tls_required && !self.security.tls_enabled {
            return bad("security.tls_required needs tls_enabled".into());
        }
        if self.server.mode == Mode::Cluster {
            if self.cluster.peers.len() != 3 {
                return bad("cluster mode requires exactly 3 peers in 1.0".into());
            }
            if !self
                .cluster
                .peers
                .iter()
                .any(|p| p.node_id == self.server.node_id)
            {
                return bad("server.node_id must be listed in cluster.peers".into());
            }
            let voters: Vec<NodeId> = self.voters();
            if voters.len() != 3 {
                return bad("cluster.initial_voters must contain 3 node ids".into());
            }
            if self.cluster.cluster_id.is_empty() {
                return bad("cluster.cluster_id must not be empty".into());
            }
        }
        if self.cluster.election_timeout_min_ms <= self.cluster.heartbeat_ms * 2
            || self.cluster.election_timeout_max_ms <= self.cluster.election_timeout_min_ms
        {
            return bad("cluster election timeouts must be > 2*heartbeat and max > min".into());
        }
        for c in self
            .network
            .access
            .allow_cidrs
            .iter()
            .chain(self.network.access.deny_cidrs.iter())
        {
            Cidr::parse(c).map_err(DbError::invalid_param)?;
        }
        Ok(())
    }

    pub fn voters(&self) -> Vec<NodeId> {
        match self.server.mode {
            Mode::Standalone => vec![self.server.node_id],
            Mode::Cluster => {
                if self.cluster.initial_voters.is_empty() {
                    self.cluster.peers.iter().map(|p| p.node_id).collect()
                } else {
                    self.cluster.initial_voters.clone()
                }
            }
        }
    }

    pub fn data_dir(&self) -> &Path {
        &self.storage.data_dir
    }
    pub fn run_dir(&self) -> PathBuf {
        self.server
            .run_dir
            .clone()
            .unwrap_or_else(|| self.storage.data_dir.join("run"))
    }
    pub fn store_socket(&self) -> PathBuf {
        self.run_dir().join("store.sock")
    }
    pub fn sync_socket(&self) -> PathBuf {
        self.run_dir().join("sync.sock")
    }
    pub fn search_socket(&self) -> PathBuf {
        self.run_dir().join("search.sock")
    }
    pub fn log_socket(&self) -> PathBuf {
        self.run_dir().join("log.sock")
    }
    pub fn supervisor_socket(&self) -> PathBuf {
        self.run_dir().join("jamsild.sock")
    }
    pub fn sqlite_path(&self) -> PathBuf {
        self.storage.data_dir.join("rdb").join("main.sqlite")
    }
    pub fn raft_dir(&self) -> PathBuf {
        self.storage.data_dir.join("raft")
    }
    pub fn search_dir(&self) -> PathBuf {
        self.search
            .index_dir
            .clone()
            .unwrap_or_else(|| self.storage.data_dir.join("search"))
    }
    pub fn metadata_dir(&self) -> PathBuf {
        self.storage.data_dir.join("metadata")
    }
    pub fn tmp_dir(&self) -> PathBuf {
        self.storage.data_dir.join("tmp")
    }
    pub fn text_max_bytes(&self) -> usize {
        self.text.max_size_mib * MIB
    }
    pub fn sqlite_max_length(&self) -> usize {
        self.storage.limits.sqlite_max_length_mib * MIB
    }
    pub fn max_mutation_bytes(&self) -> usize {
        self.storage.limits.max_mutation_payload_mib * MIB
    }
    pub fn frame_bytes(&self) -> usize {
        self.storage.limits.raft_transport_frame_mib.max(1) * MIB
    }
    pub fn peer_address(&self, id: NodeId) -> Option<&str> {
        self.cluster
            .peers
            .iter()
            .find(|p| p.node_id == id)
            .map(|p| p.address.as_str())
    }
}

/// IPv4/IPv6 CIDR.
#[derive(Debug, Clone, Copy)]
pub struct Cidr {
    pub addr: IpAddr,
    pub prefix: u8,
}

impl Cidr {
    pub fn parse(s: &str) -> Result<Cidr, String> {
        let (a, p) = match s.split_once('/') {
            Some((a, p)) => (a, Some(p)),
            None => (s, None),
        };
        let addr: IpAddr = a
            .trim()
            .parse()
            .map_err(|_| format!("invalid CIDR address: {s}"))?;
        let max = if addr.is_ipv4() { 32 } else { 128 };
        let prefix = match p {
            Some(p) => p
                .trim()
                .parse::<u8>()
                .map_err(|_| format!("invalid CIDR prefix: {s}"))?,
            None => max,
        };
        if prefix > max {
            return Err(format!("invalid CIDR prefix: {s}"));
        }
        Ok(Cidr { addr, prefix })
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        let ip = match (self.addr, ip) {
            (IpAddr::V6(_), IpAddr::V4(v4)) => IpAddr::V6(v4.to_ipv6_mapped()),
            (IpAddr::V4(_), IpAddr::V6(v6)) => match v6.to_ipv4_mapped() {
                Some(v4) => IpAddr::V4(v4),
                None => return false,
            },
            _ => ip,
        };
        match (self.addr, ip) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                if self.prefix == 0 {
                    return true;
                }
                let mask = u32::MAX << (32 - self.prefix as u32);
                (u32::from(net) & mask) == (u32::from(ip) & mask)
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                if self.prefix == 0 {
                    return true;
                }
                let mask = u128::MAX << (128 - self.prefix as u32);
                (u128::from(net) & mask) == (u128::from(ip) & mask)
            }
            _ => false,
        }
    }
}

/// 접속 IP 정책: Deny 우선, 이후 Allow, 둘 다 해당 없으면 default.
#[derive(Debug, Clone)]
pub struct AccessPolicy {
    allow: Vec<Cidr>,
    deny: Vec<Cidr>,
    default_allow: bool,
}

impl AccessPolicy {
    pub fn from_config(c: &AccessConfig) -> AccessPolicy {
        AccessPolicy {
            allow: c
                .allow_cidrs
                .iter()
                .filter_map(|s| Cidr::parse(s).ok())
                .collect(),
            deny: c
                .deny_cidrs
                .iter()
                .filter_map(|s| Cidr::parse(s).ok())
                .collect(),
            default_allow: c.default != "deny",
        }
    }

    pub fn is_allowed(&self, ip: IpAddr) -> bool {
        if self.deny.iter().any(|c| c.contains(ip)) {
            return false;
        }
        if self.allow.iter().any(|c| c.contains(ip)) {
            return true;
        }
        self.default_allow
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cidr_matching() {
        let c = Cidr::parse("192.168.10.0/24").unwrap();
        assert!(c.contains("192.168.10.11".parse().unwrap()));
        assert!(!c.contains("192.168.11.1".parse().unwrap()));
        assert!(Cidr::parse("0.0.0.0/0")
            .unwrap()
            .contains("8.8.8.8".parse().unwrap()));
        assert!(Cidr::parse("::/0")
            .unwrap()
            .contains("::1".parse().unwrap()));
        let p = AccessPolicy::from_config(&AccessConfig {
            default: "allow".into(),
            allow_cidrs: vec!["0.0.0.0/0".into()],
            deny_cidrs: vec!["10.0.0.0/8".into()],
        });
        assert!(!p.is_allowed("10.1.2.3".parse().unwrap()));
        assert!(p.is_allowed("11.1.2.3".parse().unwrap()));
    }

    #[test]
    fn default_config_is_valid() {
        Config::default().validate().unwrap();
    }
}

#[cfg(test)]
mod example_tests {
    use super::*;

    #[test]
    fn shipped_examples_are_valid() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");
        for f in ["standalone.toml", "cluster.toml"] {
            let text = std::fs::read_to_string(root.join(f)).unwrap();
            let cfg: Config = toml::from_str(&text).unwrap_or_else(|e| panic!("{f}: {e}"));
            cfg.validate().unwrap_or_else(|e| panic!("{f}: {e}"));
        }
    }

    #[test]
    fn no_effect_options_rejected() {
        let mut cfg: Config = toml::from_str("").unwrap();
        cfg.security.cluster_mtls = true;
        assert!(cfg.validate().unwrap_err().message.contains("cluster_mtls"));
        let mut cfg: Config = toml::from_str("").unwrap();
        cfg.storage.limits.large_value_budget_mib = 10;
        assert!(cfg.validate().is_err());
    }
}
