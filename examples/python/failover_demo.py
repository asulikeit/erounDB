"""장애 전환 데모: 쓰기를 계속하다 접속 중인 노드를 systemctl 로 중지/기동해도 쓰기가 한 건도 유실·중복되지 않는지 본다.
시험 VM 전용 (ssh 계정에 비밀번호 없는 sudo 필요).  JAMSIL_HOSTS, JAMSIL_USER, JAMSIL_PASSWORD, JAMSIL_SSH_USER(기본 daniel)
    python failover_demo.py
"""
import os
import subprocess
import threading
import time

from jamsil_client import JamsilClient, new_request_key

HOSTS = os.environ["JAMSIL_HOSTS"].split(",")
SSH_USER = os.environ.get("JAMSIL_SSH_USER", "daniel")
T = "sample_failover"


def ssh(host: str, cmd: str) -> None:
    subprocess.run(["ssh", "-o", "BatchMode=yes", f"{SSH_USER}@{host}", cmd], check=True, capture_output=True, timeout=90)


db = JamsilClient(HOSTS, os.environ["JAMSIL_USER"], os.environ["JAMSIL_PASSWORD"], application_name="failover-demo-py", max_attempts=30)
db.write(new_request_key("ddl"), lambda c: (c.execute(f"DROP TABLE IF EXISTS {T}"), c.execute(f"CREATE TABLE {T} (id BIGINT PRIMARY KEY)")))
acked, stop, errors, retries = [], threading.Event(), [], [0]


def writer() -> None:
    i = 0
    try:
        while not stop.is_set():
            i += 1
            db.execute(new_request_key(f"fo-{i}"), f"INSERT INTO {T} VALUES (%s)", (i,))   # ON CONFLICT 없음: 중복 반영이면 23505
            retries[0] += db.retries
            acked.append(i)
            time.sleep(0.04)
    except Exception as e:  # noqa: BLE001
        errors.append(e)


th = threading.Thread(target=writer)
th.start()
time.sleep(2)
victim = db.host.rsplit(":", 1)[0]
print("client connected to", db.host, "-> stopping that node")
ssh(victim, "sudo systemctl stop jamsildb")
n0 = len(acked)
time.sleep(5)
print("writes acknowledged while the node is down:", len(acked) - n0, "| now connected to", db.host)
ssh(victim, "sudo systemctl start jamsildb --no-block")
time.sleep(15)
stop.set()
th.join()
rows = {r[0] for r in db.read(lambda c: c.execute(f"SELECT id FROM {T}").fetchall())}
print(f"acknowledged={len(acked)} rows={len(rows)} retried={retries[0]} errors={errors}")
assert not errors and rows == set(acked), "lost or duplicated writes"
db.write(new_request_key("drop"), lambda c: c.execute(f"DROP TABLE IF EXISTS {T}"))
print("OK: no lost or duplicated writes")
