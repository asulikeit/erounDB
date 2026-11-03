#!/usr/bin/env bash
# VM 3대 시험 준비/원복 + 권한 시험 계정.
#   scripts/vm-acceptance.sh setup     # 설정 백업(SHA-256 기록) → 시험용 설정 추가 → 롤링 재시작
#   scripts/vm-acceptance.sh restore   # 백업 복원 → SHA-256 동일 확인 → 롤링 재시작
# 시험용 설정: [cluster] snapshot_threshold_entries=200 / snapshot_keep_entries=20, JAMSIL_FAULT_INJECTION=1 drop-in,
#             권한 시험 계정 qa_rw(readwrite, tables=QA_RW_TABLES 기본 qa_acl) / qa_ro(readonly, tables=QA_RO_TABLES 기본 제한 없음).
# 시험 계정 비밀번호는 실행할 때마다 무작위로 만들어 $QA_CRED(기본 ~/.jamsil_qa_roles, 0600)에만 저장한다.
set -euo pipefail
HOSTS="${JAMSIL_HOSTS:-192.168.56.101,192.168.56.102,192.168.56.100}"
SSH_USER="${JAMSIL_SSH_USER:-daniel}"
QA_CRED="${QA_CRED:-$HOME/.jamsil_qa_roles}"
TAG=acceptance-$(date +%Y%m%d)
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAMSILD="${JAMSILD:-$ROOT/target/release/jamsild}"
SSH() { ssh -o BatchMode=yes -o ConnectTimeout=5 "$SSH_USER@$1" "${@:2}"; }

case "${1:-}" in
  setup)
    umask 077
    RW=$(head -c 24 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 24)
    RO=$(head -c 24 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 24)
    printf 'JAMSIL_RW_USER=qa_rw\nJAMSIL_RW_PASSWORD=%s\nJAMSIL_RO_USER=qa_ro\nJAMSIL_RO_PASSWORD=%s\n' "$RW" "$RO" > "$QA_CRED"
    # 허용 테이블 (쉼표 구분). 기본: qa_rw=qa_acl, qa_ro=제한 없음. 예) QA_RW_TABLES=t1 QA_RO_TABLES=t1
    q() { local o="" t; for t in ${1//,/ }; do o="${o:+$o, }\\\"$t\\\""; done; printf '%s' "$o"; }
    RW_TABLES=$(q "${QA_RW_TABLES:-qa_acl}")
    RO_TABLES_LINE=""; [ -n "${QA_RO_TABLES:-}" ] && RO_TABLES_LINE="tables = [$(q "$QA_RO_TABLES")]\\n"
    RWS=$("$JAMSILD" hash-password "$RW")
    ROS=$("$JAMSILD" hash-password "$RO")
    for h in ${HOSTS//,/ }; do
      SSH "$h" "set -eu
        cfg=/etc/jamsildb/jamsildb.toml; b=/etc/jamsildb/jamsildb.toml.$TAG
        sudo test ! -e \$b
        sudo install -m 0600 \$cfg \$b
        echo \"$h original \$(sudo sha256sum \$cfg | cut -d' ' -f1)\"
        sudo sed -i '/^\[cluster\]\$/a snapshot_threshold_entries = 200\nsnapshot_keep_entries = 20' \$cfg
        printf '\n[[security.users]]\nname = \"qa_rw\"\nscram_secret = \"%s\"\nrole = \"readwrite\"\ntables = [$RW_TABLES]\n\n[[security.users]]\nname = \"qa_ro\"\nscram_secret = \"%s\"\nrole = \"readonly\"\n$RO_TABLES_LINE' '$RWS' '$ROS' | sudo tee -a \$cfg >/dev/null
        sudo mkdir -p /etc/systemd/system/jamsildb.service.d
        printf '[Service]\nEnvironment=JAMSIL_FAULT_INJECTION=1\n' | sudo tee /etc/systemd/system/jamsildb.service.d/90-$TAG.conf >/dev/null
        sudo systemctl daemon-reload; sudo systemctl restart jamsildb; sudo systemctl is-active jamsildb"
      sleep 8
    done
    echo "QA role credentials: $QA_CRED" ;;
  restore)
    for h in ${HOSTS//,/ }; do
      SSH "$h" "set -eu
        cfg=/etc/jamsildb/jamsildb.toml
        sudo nft delete table inet jtest 2>/dev/null || true
        sudo sh -c 'rm -f /etc/systemd/system/jamsildb.service.d/90-acceptance-*.conf /var/lib/jamsildb/rdb/crash_before_apply_commit \
          /var/lib/jamsildb/rdb/crash_after_apply_commit /var/lib/jamsildb/rdb/sqlite_full /var/lib/jamsildb/raft/crash_* /var/lib/jamsildb/raft/fail_raft_fsync'
        # /etc/jamsildb 는 root 전용이라 glob도 root 셸에서 펼친다 (사용자 셸에서는 펼쳐지지 않아 복원이 건너뛰어진다).
        n=\$(sudo sh -c 'ls -1 /etc/jamsildb/jamsildb.toml.acceptance-* 2>/dev/null | wc -l')
        [ \"\$n\" = 1 ] || { echo \"$h: expected 1 backup, found \$n\" >&2; exit 1; }
        sudo sh -c 'b=\$(ls -1 /etc/jamsildb/jamsildb.toml.acceptance-*); cp \"\$b\" '\$cfg' && cmp \"\$b\" '\$cfg' && rm \"\$b\"'
        echo \"$h restored \$(sudo sha256sum \$cfg | cut -d' ' -f1)\"
        sudo systemctl daemon-reload; sudo systemctl restart jamsildb; sudo systemctl is-active jamsildb"
      sleep 8
    done
    rm -f "$QA_CRED" ;;
  *) echo "usage: $0 setup|restore"; exit 2 ;;
esac
