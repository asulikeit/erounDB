#!/usr/bin/env bash
# 시험용 장애 주입 스위치 (3대 VM). 시험 후 반드시 disable.
#   scripts/vm-fault-injection.sh enable|disable
# JAMSIL_FAULT_INJECTION=1 은 Store를 Raft 확정 후 COMMIT 직전에 죽일 수 있게 하는 시험 전용 훅을 켠다.
set -euo pipefail
HOSTS="${JAMSIL_HOSTS:-192.168.56.100,192.168.56.101,192.168.56.102}"
SSH_USER="${JAMSIL_SSH_USER:-daniel}"
D=/etc/systemd/system/jamsildb.service.d
for h in ${HOSTS//,/ }; do
  case "${1:-}" in
    enable)
      ssh -o BatchMode=yes "$SSH_USER@$h" "sudo mkdir -p $D && printf '[Service]\nEnvironment=JAMSIL_FAULT_INJECTION=1\n' | sudo tee $D/fault.conf >/dev/null && sudo systemctl daemon-reload && sudo systemctl restart jamsildb" ;;
    disable)
      ssh -o BatchMode=yes "$SSH_USER@$h" "sudo rm -f $D/fault.conf /var/lib/jamsildb/rdb/crash_before_apply_commit; sudo nft delete table inet jtest 2>/dev/null; sudo systemctl daemon-reload && sudo systemctl restart jamsildb" ;;
    *) echo "usage: $0 enable|disable"; exit 2 ;;
  esac
  echo "$1 done on $h"
  sleep 8   # 한 대씩 롤링 재시작 (과반수 유지)
done
