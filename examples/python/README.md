# erounDB Python 연계 샘플 (psycopg 3)

| 파일 | 설명 |
|---|---|
| `jamsil_client.py` | 운영 코드로 복사해 쓸 수 있는 얇은 클라이언트 (파일 하나): 모든 노드에 접속 시도, 재연결·재시도, 멱등 키 |
| `quickstart.py` | 사용 예 (단일 문장 쓰기 / RETURNING / 다중 문장 트랜잭션 / 읽기) |
| `failover_demo.py` | 쓰기 중 접속 노드를 `systemctl stop`/`start` 해도 유실·중복이 없음을 보이는 데모 (**시험 VM 전용**) |

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install "psycopg[binary]"                      # psycopg 3.x (Python 3.9+)
export JAMSIL_USER=... JAMSIL_PASSWORD=...         # 코드·저장소에 비밀번호를 쓰지 않는다
export JAMSIL_HOSTS=10.0.0.1,10.0.0.2,10.0.0.3     # 클러스터의 모든 노드
python quickstart.py
```

## 규칙 (Java 샘플과 같다)

1. **모든 노드를 나열한다.** 어느 노드에 붙어도 읽기·쓰기가 된다. `JamsilClient`는 노드 순서를 섞어 차례로 시도하므로 죽은 노드는 건너뛴다.
2. **재시도 대상:** `08xxx`(연결 끊김, `08007`=커밋 결과 불명), 접속 실패, `57P03`(과반수 없음·선출 중), `40001`, `40P01`. 그 밖의 오류(`23505`, `42xxx`, `22023` …)는 그대로 던진다.
3. **쓰기는 논리 작업마다 멱등 키를 한 번 만들어 재시도에 같은 키를 쓴다** (`new_request_key`). 응답을 못 받은 쓰기도 정확히 한 번만 반영된다.
   - `execute()`/`write(key, fn)`: autocommit 단일 문장 — `RETURNING` 결과·영향 행 수까지 재생.
   - `write(key, fn, transaction=True)`: 여러 문장, 키는 COMMIT에 적용. 재실행 중의 문장 결과는 버려진다.
     COMMIT 응답이 유실된 뒤 재실행하면 `INSERT`가 서버의 COMMIT 시점 멱등 확인보다 먼저 PK 충돌(`23505`)로 실패한다. `write(…, transaction=True)`는 이때 같은 키가 이미 확정됐는지 확인해 그렇다면 `None`을 돌려주고(성공), 확정된 적 없는 진짜 중복이면 `UniqueViolation`을 그대로 던진다. 이 확인은 오류 메시지에서 충돌한 테이블 이름을 얻을 수 있을 때만 동작하므로, 본문은 가능하면 `ON CONFLICT DO NOTHING`처럼 다시 실행해도 실패하지 않게 쓴다.
4. 재시도되는 본문(`fn`)에서 DB 밖의 부수 효과(메일, 메시지 발행)를 만들지 않는다.
5. 연결은 `autocommit=True`로 열고(문장 단위 쓰기), 트랜잭션은 `conn.transaction()` 블록으로 묶는다.
6. `REPEATABLE READ`/`SERIALIZABLE` 트랜잭션에서는 `TEXT_MATCH`/`TEXT_SCORE`가 `JDB-4105`(`0A000`)로 거부된다.

## psycopg2 / SQLAlchemy 를 쓴다면

같은 규칙을 적용한다. libpq 계열이므로 접속 문자열에 `sslmode=require`가 필요하고(`tls_required`), 다중 호스트는 `host=ip1,ip2,ip3 port=9786,9786,9786`처럼 쓸 수 있다
(`target_session_attrs`는 기본값 `any` — 어느 노드든 쓰기 가능; 이 방식은 erounDB에서 직접 시험하지 않았다). 단, libpq는 끊긴 연결의 재시도·멱등 키를 대신해 주지 않는다.

## 검증

실제 3노드 클러스터에서 `quickstart.py`와 `failover_demo.py`를 실행했다. 데모: 접속 중인 노드를 중지한 동안 쓰기 69건이 다른 노드로 계속 확정되었고,
확정 333건이 테이블에 정확히 333행(유실·중복 없음)으로 남았다. (psycopg 3.2.3, Python 3.9)
