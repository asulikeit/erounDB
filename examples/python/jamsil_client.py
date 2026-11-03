"""erounDB Python 연계 샘플 (psycopg 3). 운영 코드에 복사해 쓸 수 있도록 이 파일 하나로 완결된다.

규칙 (examples/jdbc/JamsilClient.java 와 동일)
 1) 모든 노드를 나열한다 — 한 노드만 쓰면 그 노드 장애가 곧 서비스 장애.
 2) 연결 끊김(08xxx)·일시 오류(57P03, 40001, 40P01)는 새 연결로 처음부터 다시 실행한다.
 3) 쓰기는 논리 작업마다 멱등 키(jamsil.request_id)를 한 번 만들고 재시도에 같은 키를 쓴다
    → 응답이 유실된(08007 / 연결 끊김) 쓰기도 정확히 한 번만 반영된다.
 4) 비밀번호는 코드에 쓰지 않는다 (환경변수/비밀 저장소).
"""
from __future__ import annotations

import random
import re
import time
import uuid
from typing import Callable, Optional, Sequence, TypeVar

import psycopg

T = TypeVar("T")
_SAFE_KEY = re.compile(r"[A-Za-z0-9._:-]{1,128}")
_DUP_TABLE = re.compile(r"UNIQUE constraint failed: ([A-Za-z_][A-Za-z0-9_]*)\.")
_RETRY_STATES = {"57P01", "57P03", "40001", "40P01"}


def is_retryable(e: BaseException) -> bool:
    """새 연결/재시도로 해결될 수 있는 오류인가."""
    if isinstance(e, (psycopg.OperationalError, OSError)) and not isinstance(e, psycopg.errors.InsufficientPrivilege):
        state = getattr(e, "sqlstate", None)
        if state is None or state.startswith("08") or state in _RETRY_STATES:
            return True  # sqlstate 없는 OperationalError = 접속 실패/연결 끊김
    state = getattr(e, "sqlstate", None)
    return bool(state) and (state.startswith("08") or state in _RETRY_STATES)


def new_request_key(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


class JamsilClient:
    def __init__(self, hosts: Sequence[str], user: str, password: str, dbname: str = "jamsil",
                 application_name: str = "jamsil-py", connect_timeout: int = 3,
                 max_attempts: int = 8, base_backoff: float = 0.2, max_backoff: float = 3.0):
        if not hosts:
            raise ValueError("list ALL cluster nodes")
        self.hosts = [h if ":" in h else f"{h}:9786" for h in hosts]
        self._kw = dict(user=user, password=password, dbname=dbname, sslmode="require",
                        connect_timeout=connect_timeout, application_name=application_name,
                        keepalives=1, keepalives_idle=15, keepalives_interval=5, keepalives_count=3)
        self.max_attempts, self.base_backoff, self.max_backoff = max_attempts, base_backoff, max_backoff
        self._conn: Optional[psycopg.Connection] = None
        self.host: Optional[str] = None   # 현재 연결된 노드(관측용)
        self.retries = 0                  # 마지막 작업에서 재시도한 횟수

    # --- 연결 -------------------------------------------------------------
    def _connect(self) -> psycopg.Connection:
        if self._conn is not None and not self._conn.closed:
            return self._conn
        order = self.hosts[:]
        random.shuffle(order)             # 부하 분산 + 죽은 노드는 다음 후보로
        last: Optional[Exception] = None
        for hp in order:
            host, port = hp.rsplit(":", 1)
            try:
                self._conn = psycopg.connect(host=host, port=int(port), autocommit=True, **self._kw)
                self.host = hp
                return self._conn
            except psycopg.OperationalError as e:
                last = e
        raise last  # type: ignore[misc]

    def _discard(self) -> None:
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        self._conn = None

    def close(self) -> None:
        self._discard()

    def __enter__(self) -> "JamsilClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # --- 실행 -------------------------------------------------------------
    def read(self, work: Callable[[psycopg.Connection], T]) -> T:
        """읽기 작업. 끊기면 다른 노드로 재연결해 다시 실행한다 (기본 일관성: linearizable)."""
        return self._retrying(None, False, work)

    def write(self, request_key: str, work: Callable[[psycopg.Connection], T], transaction: bool = False) -> T:
        """쓰기 1건. transaction=False: autocommit 단일 문장(RETURNING 결과까지 재생).
        transaction=True: BEGIN…COMMIT, 키는 COMMIT 효과에 적용(재실행 중 문장 결과는 버려진다).
        이미 확정된 트랜잭션을 재시도하다 PK 충돌(23505)이 나면 확정된 것으로 보고 None 을 돌려준다."""
        return self._retrying(request_key, transaction, work)

    def execute(self, request_key: str, sql: str, params: Sequence = ()) -> int:
        """단일 문장 INSERT/UPDATE/DELETE. 영향 행 수를 돌려준다 (재시도 시에도 처음 값)."""
        def work(c: psycopg.Connection) -> int:
            return c.execute(sql, params).rowcount
        return self.write(request_key, work)

    def _set_request_id(self, c: psycopg.Connection, key: str) -> None:
        if not _SAFE_KEY.fullmatch(key):
            raise ValueError("unsafe request key")
        c.execute(f"SET jamsil.request_id = '{key}'")

    def _already_committed(self, key: str, dup_message: str) -> bool:
        """이 키로 트랜잭션이 이미 확정됐는지 확인한다. 서버는 COMMIT 의 키를 "문장 전체 지문"과 대조하므로, 다른 내용의
        트랜잭션을 같은 키로 COMMIT 해 보면 확정된 키는 22023 으로 거절된다. 키는 쓰기 문장이 Writer 를 잡을 때 적용되므로
        확인용 문장도 쓰기여야 한다 → 방금 충돌한 테이블(오류 메시지에서 얻는다. 이 사용자가 쓸 수 있는 테이블)에 행을
        바꾸지 않는 DELETE ... WHERE 1=0 을 쓴다. 확정되지 않은 키였다면 이 확인이 키를 소모하지만, 호출자는 원래 오류
        (진짜 중복)를 그대로 받고 작업을 포기하므로 문제없다. 테이블을 알 수 없으면 확인하지 않는다."""
        m = _DUP_TABLE.search(dup_message)
        if not m:
            return False
        try:
            c = self._connect()
            try:
                with c.transaction():
                    c.execute(f"DELETE FROM {m.group(1)} WHERE 1 = 0")
                    self._set_request_id(c, key)
                return False
            except psycopg.Error as e:
                return getattr(e, "sqlstate", None) == "22023"
        except Exception:                               # noqa: BLE001 - 확인 불가면 원래 오류를 그대로 낸다
            return False

    def _retrying(self, key: Optional[str], tx: bool, work: Callable[[psycopg.Connection], T]) -> T:
        last: Optional[BaseException] = None
        self.retries = 0
        for attempt in range(1, self.max_attempts + 1):
            try:
                c = self._connect()
                if key is not None and tx:
                    with c.transaction():              # 예외면 ROLLBACK, 정상이면 COMMIT
                        out = work(c)
                        self._set_request_id(c, key)   # COMMIT 에 적용되는 멱등 키
                    return out
                if key is not None:
                    self._set_request_id(c, key)       # 바로 다음 쓰기 문장 1건에 적용
                return work(c)
            except Exception as e:                      # noqa: BLE001 - 분류 후 재던진다
                last = e
                # 이전 시도의 COMMIT 은 성공했는데 응답만 유실된 경우: 재실행한 INSERT 가 COMMIT 시점의 멱등 확인보다
                # 먼저 PK 충돌(23505)로 실패한다. 같은 키가 이미 확정됐는지 확인해 그렇다면 성공으로 처리한다.
                if (key is not None and tx and getattr(e, "sqlstate", None) == "23505"
                        and self._already_committed(key, str(e))):
                    return None  # type: ignore[return-value]
                if not is_retryable(e):
                    raise
                if self._conn is not None and (self._conn.closed or isinstance(e, psycopg.OperationalError)):
                    self._discard()                     # 죽은 연결은 버리고 다른 노드로
                self.retries += 1
                if attempt == self.max_attempts:
                    break
                d = min(self.max_backoff, self.base_backoff * (2 ** (attempt - 1)))
                time.sleep(d / 2 + random.random() * d / 2)   # 지터
        assert last is not None
        raise last
