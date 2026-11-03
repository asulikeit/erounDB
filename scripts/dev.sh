#!/usr/bin/env bash
# 개발/시험용 로컬 실행 스크립트.
#   scripts/dev.sh standalone   - 1노드 (포트 9786)
#   scripts/dev.sh cluster      - 3노드 127.0.0.1 (SQL 19786/29786/39786, Raft 19787/29787/39787)
#   scripts/dev.sh stop         - 모두 종료
#   scripts/dev.sh clean        - 종료 후 데이터 삭제
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEV="${JAMSIL_DEV_DIR:-$ROOT/dev-data}"
BIN="${JAMSIL_BIN_DIR:-$ROOT/target/release}"
SEARCH="${JAMSIL_SEARCH:-true}"
mkdir -p "$DEV"

write_config() { # name node_id mode sql_port raft_port
  local name=$1 id=$2 mode=$3 port=$4 rport=$5 dir="$DEV/$1"
  mkdir -p "$dir"
  cat > "$dir/node.toml" <<CFG
[server]
name = "erounDB-dev"
node_id = $id
mode = "$mode"
run_dir = "$dir/run"
bin_dir = "$BIN"

[network]
listen_ipv4 = "127.0.0.1"
port = $port
cluster_bind = "127.0.0.1:$rport"

[security]
authentication = "scram-sha-256"
tls_enabled = true
tls_required = false
cluster_secret = "dev-cluster-secret"

[[security.users]]
name = "jamsil"
password = "jamsil"

# 권한 시험용 (개발 전용 비밀번호)
[[security.users]]
name = "app_rw"
password = "app_rw"
role = "readwrite"
tables = ["qa_acl"]

[[security.users]]
name = "app_ro"
password = "app_ro"
role = "readonly"

[database]
name = "jamsil"

[storage]
data_dir = "$dir/data"
idle_tx_timeout_ms = 60000

[cluster]
cluster_id = "jamsildb-dev"
heartbeat_ms = 100
election_timeout_min_ms = 600
election_timeout_max_ms = 1200
snapshot_threshold_entries = ${JAMSIL_SNAPSHOT_THRESHOLD:-20000}
snapshot_keep_entries = ${JAMSIL_SNAPSHOT_KEEP:-1000}
[[cluster.peers]]
node_id = 1
address = "127.0.0.1:19787"
[[cluster.peers]]
node_id = 2
address = "127.0.0.1:29787"
[[cluster.peers]]
node_id = 3
address = "127.0.0.1:39787"

[search]
enabled = $SEARCH
committed_timeout_ms = 10000

[search.indexing]
commit_interval_ms = 300

[logging]
directory = "$dir/log"
level = "info"
format = "text"
query_log = true
CFG
}

start() { # name
  local dir="$DEV/$1"
  JAMSIL_FAULT_INJECTION="${JAMSIL_FAULT_INJECTION:-1}" nohup "$BIN/jamsild" --config "$dir/node.toml" > "$dir/jamsild.out" 2>&1 &
  echo $! > "$dir/jamsild.pid"
  echo "started $1 (pid $!) -> $dir"
}

stop_all() {
  for p in "$DEV"/*/jamsild.pid; do
    [ -f "$p" ] || continue
    local pid; pid=$(cat "$p")
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    rm -f "$p"
  done
}

case "${1:-}" in
  standalone)
    write_config standalone 1 standalone 9786 9787
    start standalone ;;
  cluster)
    write_config node1 1 cluster 19786 19787
    write_config node2 2 cluster 29786 29787
    write_config node3 3 cluster 39786 39787
    start node1; start node2; start node3 ;;
  start)  start "$2" ;;
  stop)   stop_all ;;
  clean)  stop_all; rm -rf "$DEV" ;;
  *) echo "usage: $0 standalone|cluster|start <node>|stop|clean"; exit 2 ;;
esac
