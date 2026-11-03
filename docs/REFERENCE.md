# erounDB 상세 문서

> 구현 사양·정합성 계약·SQL 범위·시험 방법/결과·알려진 제한을 담은 참조 문서다. 소개, 빌드·배포, 클라이언트 연계는 [README.md](../README.md), 설계서는 [DESIGN.md](DESIGN.md).
> 이름에 관해: 프로젝트(배포) 이름은 erounDB이고, 실행 파일·크레이트·설정 경로·서비스·기본 DB/계정·SQL 확장(`jamsil_catalog`, `jamsil.request_id`)은 코드명 `jamsil`을 그대로 쓴다.

Rust 기반 분산 관계형 데이터베이스 — [설계서](DESIGN.md) v1.3 구현.

- PostgreSQL v3 Wire Protocol (TCP **9786**) → DBeaver / 표준 PostgreSQL JDBC 접속
- SQLite 로컬 저장 엔진 + **Raft 3노드 동기 복제** (TCP **9787**)
- TEXT **최대 32MiB (33,554,432 bytes)** SQLite Inline 저장, `LIKE` 금지, `TEXT_MATCH` 전문 검색
- Tantivy + Lindera(ko-dic) 한국어 형태소 분석 검색 프로세스
- 요청·저장·복제·검색·로그를 6개 OS 프로세스로 분리

## 1. 빌드

```bash
# Rocky Linux 9: gcc 필요 (SQLite bundled 빌드). protoc 불필요.
cargo build --release                 # target/release/{jamsild,jamsil-sql,jamsil-store,jamsil-sync,jamsil-search,jamsil-log}
cargo build --profile dist            # 배포용 (thin LTO)
cargo test --workspace                # 단위/통합 시험
```

첫 빌드는 Lindera ko-dic 사전을 내려받아 포함하므로 시간이 걸린다.

## 2. 실행

### 개발용 (로컬)

```bash
scripts/dev.sh standalone     # 1노드: 127.0.0.1:9786
scripts/dev.sh cluster        # 3노드: 127.0.0.1:19786 / 29786 / 39786 (Raft 19787 / 29787 / 39787)
scripts/dev.sh stop
scripts/dev.sh clean          # 종료 + dev-data 삭제
```

접속 (계정 `jamsil` / `jamsil`):

```
jdbc:postgresql://127.0.0.1:9786/jamsil?sslmode=require
jdbc:postgresql://127.0.0.1:19786,127.0.0.1:29786,127.0.0.1:39786/jamsil?sslmode=require&loadBalanceHosts=true
```

DBeaver: PostgreSQL 드라이버로 새 연결 → Host/Port/Database `jamsil` → SSL `require`.

### 클라이언트 접속과 장애 전환

모든 노드가 같은 포트(9786)에서 읽기·쓰기를 받는다(쓰기는 Follower가 Leader로 전달). 따라서 **클라이언트는 모든 노드를 URL에 나열**해야 한다.
노드 하나만 적으면 그 노드가 죽을 때 새 연결이 불가능하고 기존 연결도 끊긴다(서버가 살아 있어도 해당 클라이언트는 장애).

```
jdbc:postgresql://10.0.0.1:9786,10.0.0.2:9786,10.0.0.3:9786/jamsil?sslmode=require&loadBalanceHosts=true&hostRecheckSeconds=2&connectTimeout=3&socketTimeout=30
```

- pgjdbc는 연결할 때 목록에서 살아 있는 노드를 찾는다. **이미 끊긴 연결과 진행 중이던 트랜잭션은 복구되지 않으므로** 애플리케이션이 재연결·재시도해야 한다.
- 재시도 대상: SQLSTATE `08xxx`(연결 끊김, `08007`=커밋 결과 불명 `JDB-2003`), `57P03`(`JDB-2001` 과반수 없음·Leader 선출 중), `40001`(`JDB-2002` 포함), `40P01`.
- 쓰기는 논리 작업마다 멱등 키(`SET jamsil.request_id`)를 한 번 만들어 재시도에 같은 키를 쓴다 — 응답이 유실된 쓰기도 정확히 한 번만 반영된다(§4).
- `socketTimeout`을 반드시 설정한다(Leader 교체·과반수 상실 중 무기한 대기 방지). 풀(HikariCP 등)을 쓰면 `maxLifetime`·`keepaliveTime`을 짧게 두고 연결 검증을 켠다.
- DBeaver: 연결 설정에서 **URL 모드**(Connect by: URL)를 선택해 위 URL을 직접 입력한다 (Host 칸에 쉼표 목록을 쓰면 DBeaver가 포트를 덧붙여 URL이 깨질 수 있다). 자세한 절차는 [README.md](../README.md).
- **샘플 코드:** `examples/jdbc/` — `JamsilClient`(재시도·재연결·멱등 키 포함, 외부 의존성 없음)와 `Quickstart`. 실행: `examples/jdbc/run.sh ip1,ip2,ip3` (자세한 설명은 [examples/jdbc/README.md](../examples/jdbc/README.md)).
- **장애 전환 시험:** `tests/run-multihost-failover.sh ip1,ip2,ip3` — 실제 VM 3대를 `systemctl stop/start`로 차례로 중지하며 재연결·무손실·무중복을 검증한다(§8.4).

### 운영

```bash
jamsild hash-password '<password>'          # security.users.scram_secret 값 생성
jamsild --config /etc/jamsildb/jamsildb.toml
jamsild status --config /etc/jamsildb/jamsildb.toml
```

설정 예시: [config/standalone.toml](../config/standalone.toml), [config/cluster.toml](../config/cluster.toml).
systemd: [packaging/systemd/jamsildb.service](../packaging/systemd/jamsildb.service) (`Type=notify`, 비root `jamsildb` 계정, 종료 코드 78은 재시작하지 않음).
RPM: `scripts/build-rpm.sh` (`rpm-build` 필요) → `~/rpmbuild/RPMS/x86_64/jamsildb-*.rpm`.

종료 코드: `78` 영구 실패(설정·Cluster ID·포맷 불일치 — 재시작 금지), `75` 내구성 보장 불가(fsync 실패 — 재기동 후 복구).

빌드 식별: `jamsild --version` → `0.1.0-6 (build <git>+src.<소스 해시>)`. 같은 값이 각 프로세스 기동 로그, `SELECT version()`,
`jamsil_catalog.metrics`의 `jamsil_build_info` 레이블에 나온다. RPM 빌드 시 `dist/jamsildb-<ver>-<rel>.manifest`에 빌드 ID·소스 해시·바이너리/RPM SHA-256 대응표를 남긴다.

**업그레이드 (0.1.0-2 → 0.1.0-3 이상)**: 프로세스 간/노드 간 프레임 형식과 복제 명령이 바뀌어 롤링 업그레이드를 지원하지 않는다.
세 노드를 모두 중지 → RPM 업그레이드 → 세 노드 기동 순서로 진행한다(데이터 디렉터리는 그대로 사용, `_jamsil_requests`는 기동 시 자동 확장).

설정 검증: 효과가 없는 옵션은 조용히 무시하지 않고 기동을 거부한다 — `security.cluster_mtls = true`(미구현), `text.like_enabled = true`,
`storage.compression` ≠ `none`, `storage.journal_mode` ≠ `WAL`, `storage.checksum = false`, `storage.limits.raft_transport_frame_mib` ≠ 1.

## 3. 프로세스 구조 (설계서 3.2)

| 실행파일 | 역할 | IPC |
|---|---|---|
| `jamsild` | Supervisor: 디렉터리 Lock, `cluster.meta` 검증, 순서 기동/재시작/역순 종료, systemd notify | `jamsild.sock` |
| `jamsil-sql` | PG Wire, SCRAM-SHA-256, TLS, 세션/트랜잭션 상태, 읽기/쓰기 라우팅, `jamsil_catalog` 뷰 | TCP 9786 |
| `jamsil-store` | SQLite 단일 Writer + Reader Pool, Session Patchset, `applied_index` 원자 COMMIT, Outbox | `store.sock` |
| `jamsil-sync` | Raft(PreVote·ReadIndex·Snapshot), 쓰기 조정, Follower→Leader 전달 | `sync.sock`, TCP 9787 |
| `jamsil-search` | Tantivy 색인/검색, Outbox 소비, Checkpoint, 재색인 | `search.sock` |
| `jamsil-log` | system/cluster/query/audit 로그 집계 (WAL 아님, 손실 허용) | `log.sock` |

Supervisor가 죽으면 자식은 `PR_SET_PDEATHSIG`로 함께 종료된다.

## 4. 쓰기 경로와 정합성 (설계서 6장)

```
Client → jamsil-sql ─UDS→ jamsil-sync ─(Follower면 TCP 9787로 Leader에 전달)→ Leader jamsil-sync
  → Store.TxExec   : BEGIN IMMEDIATE + Session attach, 문장 실행 (미COMMIT)
  → Store.TxPrepare: Patchset + DDL 추출, TEXT 32MiB / Mutation 64MiB 검사
  → Raft propose   : Leader fsync + Follower Durable ACK(fsync 후) 과반수 → commit_index
  → Store.Apply(N) : 원래 트랜잭션 + applied_index=N + 멱등 키 + 검색 Outbox 를 같은 SQLite COMMIT
  → 성공 응답
Follower: 확정 로그의 DDL + Patchset 을 같은 원자 트랜잭션으로 적용
```

- SQLite COMMIT은 Apply 경로에서만 일어난다 → "SQLite 선COMMIT 후 Raft 제안"은 구조적으로 불가능.
- Leader 자신은 **fsync가 끝난 로그 index까지만** 과반수 계산에 포함한다.
- Leader 교체 시 미확정 Prepared 트랜잭션은 모두 Rollback되고, 같은 Entry가 나중에 확정되면 Patchset으로 적용된다.
- 응답 유실/타임아웃/취소 중 쓰기는 `JDB-2003 commit outcome unknown`(SQLSTATE 08007)으로 보고한다.
- 멱등 키 `SET jamsil.request_id = '...'` (다음 쓰기 문장 또는 COMMIT 1회에 적용):
  - 확인·기록이 모두 **Leader Store Writer 안에서** 직렬화된다. 같은 키의 동시 요청 중 하나만 실행되고, 나머지는 Writer를 얻은 뒤 기록을 보고 재생한다.
  - 요청 이력(키, 문장+파라미터 지문, 결과)은 Row 변경과 **같은 Raft Entry·같은 SQLite COMMIT**으로 모든 복제본에 기록된다 → Leader 교체·재기동 후에도 같은 판단.
  - 재시도는 처음 결과(`RETURNING` 행, 영향 행 수)를 그대로 돌려준다. 같은 키를 다른 문장/파라미터에 쓰면 `22023`.
  - 기록 가능한 결과는 1MiB까지(초과 시 `54000`). 키는 명시적으로 삭제되지 않는다(`_jamsil_requests`).
  - 명시적 트랜잭션(`BEGIN ... SET jamsil.request_id ... COMMIT`)은 COMMIT 효과에 대해 멱등이다: 실행한 문장·파라미터 순서 전체가 지문이며,
    같은 트랜잭션을 다시 실행해 COMMIT하면 이번 실행분은 버리고 성공을 돌려준다. 다른 내용이면 `22023`.
    재실행 중의 문장 결과는 재실행 결과이므로(버려짐), 결과 재생이 필요하면 autocommit 문장에 키를 쓴다.
- 읽기 기본값 `linearizable`: ReadIndex(현재 임기 commit + 과반수 Heartbeat 확인) 후 로컬 `applied_index` 대기. `SET jamsil.read_consistency = 'local'` 은 stale 경고와 함께 즉시 읽기.
- Check-quorum: 과반수와 통신이 끊긴 Leader는 스스로 강등 → 1:2 분리 시 소수 측 쓰기/기본 읽기 거부.
- Leader의 로그 fsync는 별도 스레드에서 수행하고(그동안 복제·Heartbeat 계속), Leader 자신의 match는 fsync 완료 후에만 반영된다.
  Follower는 fsync 후에만 ACK한다. 노드 간 `Ping`은 Raft 코어를 거치지 않고 전송 계층에서 응답하며 Check-quorum과
  Follower 선거 지연에만 쓰인다(Leader 코어가 `election_timeout_max × 4` 이상 멈추면 무시 → 멈춘 Leader는 교체).
  32MiB Entry 처리 중 불필요한 선거가 일어나지 않게 하기 위함이다.
- Raft 로그 레코드는 CRC32를 포함하고, 마지막 세그먼트의 찢어진 꼬리는 기동 시 잘라낸다. fsync 실패 시 프로세스를 종료(75)하고 재기동 복구에 맡긴다.
- 노드 간 메시지는 1MiB 프레임으로 나뉘어 전송되며 모든 프레임 수신 + CRC 검증 후에만 처리된다(부분 Entry는 ACK되지 않음).

### 4.1 트랜잭션 격리 수준

| 수준 | 동작 |
|---|---|
| `READ COMMITTED` (기본) / `READ UNCOMMITTED` | 문장마다 최신 확정 Snapshot (PostgreSQL과 같이 READ UNCOMMITTED는 READ COMMITTED) |
| `REPEATABLE READ` / `SERIALIZABLE` | 첫 문장에서 접속 노드의 Snapshot을 트랜잭션 끝까지 고정(non-repeatable read·phantom 없음). 첫 쓰기 시점에 Snapshot 이후 다른 COMMIT이 있었으면 `40001`로 거부(재시도 대상). 첫 쓰기 이후에는 클러스터 Writer를 독점하므로 트랜잭션 전체가 직렬 실행과 같다 |

- `BEGIN ISOLATION LEVEL ...`, `START TRANSACTION ... READ ONLY|READ WRITE`, `SET TRANSACTION ...`, `SET SESSION CHARACTERISTICS AS TRANSACTION ...`,
  `SET default_transaction_isolation / transaction_isolation / default_transaction_read_only`를 지원하고 `SHOW`가 실제 값을 돌려준다(JDBC `setTransactionIsolation`/`setReadOnly`).
- 알 수 없는 수준은 `22023`, 첫 문장 이후 수준 변경은 `25001`, READ ONLY 트랜잭션의 쓰기는 `25006`.
- 판정 기준이 "Snapshot 이후 아무 COMMIT"이므로 PostgreSQL보다 보수적이다(다른 행을 바꾼 COMMIT도 `40001`). 고정 Snapshot은 노드당 256개, 유휴 `storage.idle_tx_timeout_ms` 후 해제.
- **고정 Snapshot 트랜잭션의 검색 거부 (0.1.0-6)**: `REPEATABLE READ`/`SERIALIZABLE` 트랜잭션 블록 안에서 `TEXT_MATCH`/`TEXT_SCORE`를 쓰는 문장은
  `ERROR JDB-4105`(SQLSTATE `0A000`)로 거부한다. 검색 인덱스는 현재 문서 버전만 가지므로 고정 Snapshot 행과 결합하면 Snapshot에서 조건이 거짓인 행을 돌려줄 수 있기 때문이다.
  - 적용: SELECT·서브쿼리·CTE·DML 조건 어디든, 쓰기 전후 모두, Simple/Extended(PreparedStatement) 프로토콜 모두, `WITH SEARCH CONSISTENCY COMMITTED`/`EVENTUAL` 모두.
  - 오류 후 트랜잭션은 실패 상태다(다음 문장 `25P02`). `ROLLBACK` 하거나, 오류 전에 만든 Savepoint로 `ROLLBACK TO SAVEPOINT` 하면 같은 Snapshot으로 계속한다.
  - 검색이 필요하면 트랜잭션 블록 밖의 단일 문장(autocommit — 세션 기본 격리 수준이 REPEATABLE READ여도 허용) 또는 `READ COMMITTED` 트랜잭션에서 실행한다.
    이 경우 문장마다 최신 확정 상태를 본다. 한 번에 보낸 여러 문장(Simple Query의 암묵적 트랜잭션)은 세션 기본 격리 수준을 따르므로, 기본값이 REPEATABLE READ/SERIALIZABLE이면 거부된다.

## 5. SQL 지원 범위 (1.0)

- `SELECT/INSERT/UPDATE/DELETE/RETURNING/ON CONFLICT`, JOIN, GROUP BY/HAVING, ORDER BY, LIMIT/OFFSET, 서브쿼리/CTE, 뷰
- `CREATE/DROP TABLE`, `CREATE [UNIQUE] INDEX`, 제한된 `ALTER TABLE` (ADD/DROP/RENAME COLUMN, RENAME TO), `TRUNCATE`
- `BEGIN/COMMIT/ROLLBACK/SAVEPOINT`, Simple + Extended Query, Prepared Statement, 바이너리 포맷, CancelRequest, `statement_timeout`
- 타입: SMALLINT/INTEGER/BIGINT/SERIAL/BIGSERIAL, REAL/DOUBLE PRECISION, BOOLEAN, CHAR(n)/VARCHAR(n) (길이 검사), TEXT(32MiB), BYTEA, DATE/TIME/TIMESTAMP/TIMESTAMPTZ(UTC), UUID, JSON/JSONB(유효성 검사)
  - 정수: 범위·정수성 검사(`22003`), 실수 값을 정수 컬럼에 넣으면 거부. `INTEGER PRIMARY KEY`는 int4 OID·32비트 범위·NULL 거부.
    `SERIAL/BIGSERIAL`은 SQLite rowid 자동 증가를 쓰며 범위 검사는 각 타입 기준(OID는 int8로 보고).
  - UUID/DATE/TIME/TIMESTAMP/TIMESTAMPTZ: 쓰기 시점에 검증(`22P02`/`22007`/`22008`)하고 정규형으로 저장(UUID 소문자, TIMESTAMPTZ는 UTC `+00`).
    비교·IN·BETWEEN 상수도 같은 정규형으로 바꿔 비교한다(`ts = '2026-10-27 09:00:00+09:00'`가 같은 순간과 일치).
- `SAVEPOINT / ROLLBACK TO / RELEASE`: 오류로 실패한 트랜잭션에서도 `ROLLBACK TO`로 복구한다. Savepoint 이후의 DDL도 복제 대상에서 제거된다.
- PostgreSQL 캐스트(`::`), `ILIKE`, `~` 정규식, `ANY(ARRAY[...])`, `IS DISTINCT FROM`, `EXTRACT`, `now()`, `gen_random_uuid()` 등
- 메타데이터: `pg_catalog`(pg_class/pg_attribute/pg_index/pg_constraint/pg_type/...)와 `information_schema` 가상 뷰
- 시스템 뷰: `jamsil_catalog.cluster_nodes / replication_status / storage_status / search_indexes / active_sessions / metrics / replica_checksum`

복제 계약상의 제약 (명시적 오류 `JDB-5101`):
- 모든 테이블에 PRIMARY KEY 필요 (PK 컬럼은 NOT NULL 강제)
- `DECIMAL/NUMERIC` 미지원 (정확한 고정소수점 미구현, REAL 대체 금지 — 설계서 9.2)
- 같은 트랜잭션에서 DML 이후 DDL 불가, TEMP 테이블·시퀀스 함수·트리거·프로시저 미지원
- 쓰기는 클러스터 전체에서 단일 Writer로 직렬화된다 (SQLite 단일 Writer, 설계서 5.1)

## 6. TEXT와 전문 검색

```sql
CREATE TABLE documents (id BIGINT PRIMARY KEY, title VARCHAR(200), content TEXT);
CREATE SEARCH INDEX idx_documents_content ON documents(content) WITH (analyzer = 'korean', scoring = 'bm25');

SELECT id, title, TEXT_SCORE(content) AS score
FROM documents
WHERE TEXT_MATCH(content, '금융 AND (주식 OR 채권)')
ORDER BY TEXT_SCORE(content) DESC LIMIT 20;

SELECT id FROM documents WHERE TEXT_MATCH(content, '"금융 투자"') WITH SEARCH CONSISTENCY COMMITTED;
SELECT id FROM documents WHERE content LIKE '%금융%';   -- ERROR JDB-4102
```

- 분석기: `korean`(Lindera ko-dic), `english`(stemming), `standard`. 색인 시 NFKC 정규화(원문은 무손실 저장).
- 검색 인덱스는 RDB의 파생 데이터: Store가 Apply와 같은 SQLite 트랜잭션에 Outbox 이벤트를 기록하고, `jamsil-search`가 순서대로 소비 → Tantivy Commit → Checkpoint 기록 → Outbox Ack.
- `eventual`(기본) / `committed` (현재 applied index까지 반영될 때까지 대기, 시간 초과 시 `JDB-4103`).
- `REPEATABLE READ`/`SERIALIZABLE` 트랜잭션 블록 안의 검색은 `JDB-4105`(0A000)로 거부한다(§4.1).
- `TEXT_MATCH(col, q)`는 다중 필드 인덱스에서도 **col 필드만** 검색하고 `TEXT_SCORE(col)`도 그 필드의 BM25다. 질의 안에서 다른 필드를 지정(`title:x`)하면 `42601`.
  여러 필드는 `TEXT_MATCH(title,'a') AND TEXT_MATCH(body,'b')`처럼 조합한다.
- 결과는 **완전하거나 실패**: 일치 문서 전체를 후보로 가져와 SQL 필터·JOIN·COUNT·ORDER BY·LIMIT을 정확히 적용한다.
  일치 문서가 `search.max_candidates`(기본 100,000)를 넘으면 불완전한 결과 대신 `JDB-4104`(SQLSTATE 54000)로 실패한다.
- TEXT `LIKE/ILIKE/SIMILAR TO` 금지는 TEXT 컬럼에서 파생된 모든 표현식에 적용된다: 함수(`lower(body)`), 연결(`a || body`), CAST(`body::varchar` 포함),
  파생 테이블·CTE 별칭, 뷰 컬럼. 패턴 검색이 필요한 값은 `VARCHAR(n)` 컬럼으로 정의한다(명시적 VARCHAR 변환은 예외로 두지 않는다).
- Snapshot 설치·인덱스 손상·세대 변경 시 신규 디렉터리로 전체 재색인.

### 6.1 메모리 예산 (설계서 8.3 / 10.6 / 14.3)

- `storage.limits.large_value_budget_mib`(기본 512, 최소 68): **프로세스마다** 1MiB를 넘는 메시지가 동시에 차지할 수 있는 총 바이트.
  - PG 수신(jamsil-sql): 메시지 본문을 읽기 **전에** 길이만큼 예약. 부족하면 `cluster.request_timeout_ms`까지 대기 후 `53200`(연결 유지).
  - 프로세스 간·노드 간 프레임 재조립(모든 프로세스): 첫 프레임 헤더의 전체 길이로 예약, 부족하면 대기하지 않고 해당 요청만 `53200`으로 거부(같은 연결의 다른 요청은 계속).
  - 예약은 요청 처리(Writer 대기 포함)가 끝날 때 반납된다.
- Raft Apply 대기열: 적용 중인 배치(최대 64MiB, Entry 하나는 그보다 클 수 있음)가 끝나야 다음 배치를 읽는다.
- 검색 색인 Writer: `search.indexing.max_memory_mib`를 인덱스 수로 나눠 배분하고, 인덱스 추가·삭제 후 Commit 시 다시 나눈다(합계 ≤ 설정값).
  인덱스당 15MB 미만이 되는 인덱스 추가는 `53200`으로 거부. `search.indexing.workers`는 인덱스당 색인 스레드 상한.

### 6.2 사용자 권한 (설계서 4.4)

```toml
[[security.users]]
name = "app"
scram_secret = "SCRAM-SHA-256$..."
role = "readwrite"          # superuser | readwrite | readonly (생략 시 superuser 플래그로 결정)
tables = ["orders", "items"] # 생략 시 모든 사용자 테이블
```

| 역할 | 읽기 | DML | DDL·검색 인덱스 | `jamsil_catalog.*`, 내부 테이블 |
|---|---|---|---|---|
| superuser | 전체 | 전체 | 허용 | 허용 |
| readwrite | `tables` | `tables` | `42501` | `42501` |
| readonly | `tables` | `42501` | `42501` | `42501` |

- `pg_catalog`/`information_schema`는 드라이버 호환을 위해 모든 역할이 읽을 수 있다. 검사는 Store의 SQL 변환 단계(읽기·쓰기 모두)에서 수행한다.
- `is_superuser`, `current_user`/`session_user`, `has_table_privilege`, `has_column_privilege`가 실제 접속 사용자 권한을 반영한다.
- 설정의 사용자 목록이 비어 있으면(trust 개발 모드) 모두 superuser, 목록에 없는 사용자는 메타데이터만 읽을 수 있다. 세 노드의 사용자 설정은 같아야 한다.

### 6.3 지표 (`jamsil_catalog.metrics`: node_id, process, name, labels, value float8, unit)

| 이름 | 프로세스 | 의미 |
|---|---|---|
| `jamsil_sql_duration_seconds{kind=read/write/ddl/other,quantile}` (+`_count`,`_sum`) | sql | 문장 처리 시간 P50/P95/P99 (최근 1~2분, 관측이 없으면 NULL), 누적 횟수·합계 |
| `jamsil_raft_fsync_duration_seconds` | sync | Raft 로그 fdatasync 시간 |
| `jamsil_storage_fsync_duration_seconds` | store | SQLite COMMIT 시간 (synchronous=FULL: WAL 기록+fsync) |
| `jamsil_mutation_payload_bytes` | store | 트랜잭션 Patchset+DDL 크기 분포 |
| `jamsil_search_query_duration_seconds` | search | 검색 질의 시간 |
| `jamsil_storage_disk_free_bytes`, `jamsil_raft_disk_free_bytes` | store, sync | 데이터/로그 파일시스템 여유 바이트 |
| `jamsil_large_text_inflight_bytes`, `_peak_bytes`, `jamsil_large_value_budget_bytes` | 전부 | 대형 메시지 예약량/최대/예산 |
| `jamsil_ipc_queue_depth{side=server/client}`, `jamsil_storage_writer_queue_depth` | 전부, store | IPC 처리 중·대기 요청 수, Writer 대기 수 |
| `jamsil_ipc_backpressure_events_total{reason=budget_wait/budget_reject/ipc_inflight_limit}` | 전부 | 예산 대기·거부, 연결당 동시 처리 한도 도달 |
| `jamsil_build_info{version,release,build}` | 전부 | 실행 중인 빌드 |

기존 카운터/게이지(raft term/commit/applied, storage db/wal bytes, search lag 등)도 같은 표에 있다.

## 7. 설계서 대비 구현 판단 (Deviation)

| 항목 | 설계서 | 구현 | 사유 |
|---|---|---|---|
| Raft | `openraft` 후보 | 자체 구현 (`jamsil-consensus`) | openraft 최신은 0.10 alpha. Leader의 미확정 SQLite 트랜잭션과 Raft 확정·Apply를 프로세스 경계 너머로 정밀 제어하고, 1MiB 프레임/Durable ACK 의미를 직접 보장하기 위함 |
| 로컬 IPC 직렬화 | Protobuf | serde + bincode, 길이 제한·CRC 프레임 | 빌드 환경에 `protoc` 없음. 메시지 계약은 `jamsil-ipc::msg`에 버전과 함께 정의 |
| PG Wire | `pgwire` crate 후보 | 직접 구현 (`jamsil-protocol`) | SCRAM/TLS/바이너리 포맷/Describe 의미를 세밀하게 제어 |
| 복제 단위 | Changeset | Session **Patchset** | 32MiB TEXT UPDATE 시 변경 전 값이 포함되지 않아 64MiB Mutation 상한 안에 들어온다 |
| 노드 간 보안 | mTLS 권장 | Cluster ID + HMAC-SHA256 상호 인증 핸드셰이크 (**암호화 없음**). `cluster_mtls=true`는 기동 거부 | mTLS는 후속 작업. 클러스터 망 분리 필요 |
| 사용자 계정 | - | 설정 파일 기반 (SCRAM secret + role/tables) | `CREATE USER`/`GRANT` 등 계정 DDL은 1.0 범위 밖 |

## 8. 시험

```bash
cargo test --workspace                     # 단위·통합 (Raft 3노드 in-process, Storage G-02, PG 코덱, SCRAM, 검색)

# JDBC 적합성 (pgjdbc 42.7.4 고정)
scripts/dev.sh standalone
J="java -cp tests/jdbc/postgresql-42.7.4.jar"
$J tests/jdbc/JdbcConformance.java "jdbc:postgresql://127.0.0.1:9786/jamsil?sslmode=require" big
$J tests/jdbc/DbeaverCompat.java   "jdbc:postgresql://127.0.0.1:9786/jamsil?sslmode=require"
$J tests/jdbc/SearchRecovery.java  "$PWD"
$J tests/jdbc/Concurrency.java     9786 16 30

# 3노드 장애 주입
scripts/dev.sh clean && scripts/dev.sh cluster
$J tests/jdbc/ClusterChaos.java "$PWD"
$J tests/jdbc/JdbcConformance.java "jdbc:postgresql://127.0.0.1:29786/jamsil?sslmode=require" big   # Follower 경유
$J tests/jdbc/Concurrency.java 19786,29786,39786 12 20
scripts/dev.sh clean && JAMSIL_SNAPSHOT_THRESHOLD=200 JAMSIL_SNAPSHOT_KEEP=20 scripts/dev.sh cluster
$J tests/jdbc/SnapshotCatchup.java "$PWD"

# 회귀 시험(FeedbackRegression), 3개 엔드포인트 (dev.sh가 app_rw/app_ro 시험 계정을 만든다)
JAMSIL_RW_USER=app_rw JAMSIL_RW_PASSWORD=app_rw JAMSIL_RO_USER=app_ro JAMSIL_RO_PASSWORD=app_ro \
  $J tests/jdbc/FeedbackRegression.java 127.0.0.1:19786,127.0.0.1:29786,127.0.0.1:39786
```

`JAMSIL_FAULT_INJECTION=1` 로 기동한 노드는 (시험 전용) `run_dir/partition` 파일에 적힌 노드 ID와의 통신을 차단하고,
데이터 디렉터리의 표식 파일로 정확한 지점에서 장애를 일으킨다 (`crates/jamsil-common/src/fault.rs`):

| 표식 | 지점 | 시험 |
|---|---|---|
| `raft/crash_before_raft_fsync` | Leader 로그 append 후 fsync 전 abort | T-06 |
| `rdb/crash_before_apply_commit` | Raft 확정 후 SQLite COMMIT 직전 abort | T-07 |
| `rdb/crash_after_apply_commit` | SQLite COMMIT 직후 abort (+ 시험이 WAL 꼬리 절단) | T-08 |
| `raft/crash_after_commit_ack` | 성공 응답 직후 Leader jamsil-sync abort | T-09 |
| `raft/crash_mid_large_append` | 8MiB 이상 AppendEntries 프레임 4개 전송 후 abort | T-11 |
| `raft/fail_raft_fsync`, `rdb/sqlite_full` | 표식이 있는 동안 fsync EIO / SQLite 파일 증가 불가 | T-15 |

```bash
# VM 3대 정확한 장애 지점 시험 (scripts/vm-fault-injection.sh enable 후)
$J tests/vm/FaultPointsVm.java      # T-06, T-08, T-09, T-10, T-11, T-12, T-15, T-16
```

로그 파일 기록 확인: `jamsil-log`가 `system.log`/`cluster.log`/`query.log`/`audit.log`에 쓰는지는 `cargo test -p jamsil-observability`(전달 프레임을 IPC 리더로 해석, 실제 Unix 소켓 서버까지 전달)와 `tests/jdbc/SnapshotCatchup.java`(로그 파일에서 snapshot 설치 기록 확인)로 시험한다.

### 8.1 검증 결과 (Rocky Linux 9.8 VirtualBox VM, 12 vCPU)

| 시험 | 결과 |
|---|---|
| JDBC 적합성 19항목 (Standalone / 3노드 Follower 경유) | 전부 통과 — DDL/DML/트랜잭션/25P02/배치/SQLSTATE/메타데이터/한글·이모지/멱등 키/Cancel·Timeout/TEXT_MATCH/32MiB |
| 32MiB TEXT (JDBC Bind) | 33,554,432 bytes 저장·조회·복제 일치, +1 byte `JDB-3101`, 32MiB UPDATE 성공, 2×32MiB 한 행 `JDB-3103`, 두 행 합계 `JDB-3102` |
| DBeaver PostgreSQL 플러그인 탐색 질의 36개 | 전부 통과 (DB/스키마/타입/역할/테이블/컬럼/제약/인덱스/트리거/규칙/함수) |
| T-01 3개 엔드포인트 쓰기 → 복제본 Checksum 동일 | 통과 |
| T-02 Follower SIGKILL / T-03 Leader SIGKILL (연속 쓰기 중) | 통과 — 성공 응답 쓰기 유실 0, 재기동 후 Catch-up·수렴 |
| T-04 1:2 Partition | 통과 — 소수 측 쓰기·Linearizable 읽기 거부(`JDB-2001`), 소수 측 쓰기 유출 없음 |
| T-05 2노드 중지 | 통과 — 쓰기 성공 응답 없음 |
| T-07 Raft 확정 후 SQLite COMMIT 전 Store Crash | 통과 — 재기동 후 재적용, 전 노드 수렴 |
| T-09 전체 SIGKILL 재기동 | 통과 |
| Snapshot Catch-up (압축된 로그) + 검색 재색인 | 통과 |
| T-13 검색 프로세스 SIGKILL / T-14 인덱스 삭제 | 통과 — Outbox 재처리 / RDB로부터 재생성 |
| T-16 Cluster ID 불일치 | 통과 — jamsild(cluster.meta)와 Store(_jamsil_state) 두 단계에서 거부, 종료 코드 78 |
| 동시성 (16 세션 Standalone, 12 세션 3노드) | 통과 — 행 수·카운터 정확, 교착 없음 |
| TLS 필수 / IP Deny / 인증 실패 / 감사 로그 | 통과 |

### 8.2 성능 측정 (참고값)

이 VM에서 `fdatasync` 1회 ≈ 4.4ms, 스레드 간 TCP 왕복 ≈ 0.45ms(일반 서버의 10~20배)로 측정됐다.
3노드 시험은 세 노드가 **하나의 가상 디스크**를 공유하므로 fsync 경합(50~200ms)이 꼬리 지연을 지배한다. 실제 성능은 노드별 디스크를 가진 환경에서 설계서 13.2 매트릭스로 측정해야 한다.

| 항목 (Standalone, JDBC, TLS) | p50 |
|---|---|
| PK SELECT (`local`) | ~2.2 ms |
| PK SELECT (`linearizable`) | ~2.7 ms |
| Autocommit INSERT (Raft fsync + SQLite WAL fsync) | ~13 ms |
| 명시적 트랜잭션 내 INSERT | ~2.6 ms |

쓰기는 SQLite 단일 Writer + "미확정 트랜잭션을 Raft 확정까지 유지"하는 설계상 클러스터 전체에서 직렬화된다 (설계서 5.1, G-07 비교 대상).
`storage.synchronous = "NORMAL"` 은 Raft 로그 재적용으로 복구되므로 데이터 유실은 없지만, 설계서 6.2의 "로컬 데이터 WAL 영속 후 응답" 조건을 완화하므로 기본값은 `FULL` 이다.

### 8.3 실제 VM 3대 검증 (Rocky 9.8, RPM 설치, 노드별 디스크)

```bash
set -a; . ~/.jamsil_test; set +a            # JAMSIL_USER / JAMSIL_PASSWORD
scripts/vm-fault-injection.sh enable        # 시험 전용 훅 켜기 (롤링 재시작) — 시험 후 반드시 disable
J="java -cp tests/jdbc/postgresql-42.7.4.jar"
$J tests/vm/ClusterChaosVm.java             # SSH + systemctl + nft 로 장애 주입 (JAMSIL_ONLY=T-04 처럼 일부만 실행 가능)
$J tests/vm/SnapshotCatchupVm.java          # 설정 [cluster] snapshot_threshold_entries=200, snapshot_keep_entries=20 필요
scripts/vm-fault-injection.sh disable
```

| 시험 | 결과 |
|---|---|
| JDBC 적합성 19항목 (Follower 경유, 32MiB 포함), DBeaver 질의 36개 | 통과 |
| 동시성 (12세션, 3개 엔드포인트) | 반복 시 오류 0, 행 수·카운터 정확, 약 31 commits/s |
| T-01 ~ T-09 (Follower/Leader SIGKILL, 실제 네트워크 분리(nft), 2노드 중지, Store 강제 종료, 전체 재기동) | 전부 통과 — 성공 응답 쓰기 유실 0, Leader 종료 후 약 2.4초 내 쓰기 재개 |
| Snapshot 전송 (34MB, 실제 네트워크) + 검색 재색인 | 통과 (약 4.3초) |

성능 (JDBC/TLS, p50): autocommit INSERT ≈ 26ms (p99 ≈ 40~50ms), 트랜잭션 내 INSERT ≈ 3ms, PK SELECT linearizable ≈ 5~6ms / local ≈ 2ms.
공유 디스크 환경(8.2)과 달리 꼬리 지연이 안정적이다.

시험 중 발견·수정한 문제:
- 대형 Entry(32MiB) 처리로 Raft 코어가 멈춘 직후 큐에 쌓인 Tick이 Heartbeat보다 먼저 처리되어 Follower가 불필요한 PreVote에 들어가고 `JDB-2001`을 반환 → Tick을 배치 끝에서 처리, 코어 정체 시 선거 타이머 연장, PreVote 중에도 Leader 정보 유지.
- 잘못된 `cluster_bind`로 `jamsil-sync`가 반복 재시작하며 `systemctl start`가 15분 대기 → 주소 오류는 영구 실패(78)로 즉시 종료, 준비 지연 시 시작 실패 보고.

### 8.4 다중 호스트 장애 전환 시험 (0.1.0-6)

`tests/run-multihost-failover.sh` (샘플 `JamsilClient` 사용, 시험 후 3노드 원복·시험 테이블 삭제). VM 3대 결과 5/5 통과.

| 시험 | 내용 | 결과 |
|---|---|---|
| M01 | 다중 호스트 URL로 새 연결 30개가 3노드에 분산 | 9/8/13 |
| M02 | 연결된 노드를 중지 → 같은 클라이언트가 다른 노드로 재연결해 같은 키로 재시도 | 통과 (재시도 1회) |
| M03 | 중지된 노드를 가리키는 단일 호스트 URL은 `08001` 실패, 다중 호스트 URL은 20/20 성공 | 통과 |
| M04 | 쓰기를 계속하며 Leader→Follower→Follower 순으로 한 대씩 중지·재기동 | 중지 후 첫 확정 쓰기까지 0.7~2.1초, 쓰기 중단 최대 2.6초, 확정 472건 3노드 모두 존재·중복 없음 |
| M05 | 2노드 중지: 접속은 되고 쓰기는 `57P03`(`JDB-2001`)로 거부 → 노드 복귀 후 대기 중이던 쓰기가 정확히 한 번 반영 | 통과 |

시험의 INSERT는 `ON CONFLICT`를 쓰지 않아, 재시도가 중복 반영되면 PK 충돌로 시험이 실패한다.

## 9. 알려진 제한 (1.0 PoC)

- 노드 간 9787은 HMAC 상호 인증만 하고 암호화(mTLS)는 하지 않는다 (`cluster_mtls=true`는 기동 거부).
- 쓰기 처리량은 단일 Writer·트랜잭션 유지 구간에 묶인다 (위 8.2).
- `DECIMAL/NUMERIC`, 시퀀스, 트리거, 프로시저, TEMP 테이블, `COPY`, `LISTEN/NOTIFY` 미지원.
- 시간대 변환 없음 (UTC 저장/표시).
- `pg_catalog` 는 JDBC/DBeaver가 쓰는 범위만 흉내 낸다. 주석(`COMMENT ON`)은 저장하지 않는다.
- 결과 집합은 서버 메모리에 모아 전송한다 (세션당 512MiB 상한).
- 사용자 계정·역할은 설정 파일로 관리한다 (`CREATE USER`/`GRANT` 미지원).
- 대형 메시지 예산(`large_value_budget_mib`)은 들어오는 메시지만 제한한다. Leader `jamsil-sync`의 Raft 로그 Entry·Follower별 전송 사본은 예산 밖이라 동시 32MiB 6건에서 Peak RSS 약 762MiB였다.
- 데이터를 삭제한 노드는 재기동 시 과반수에서 재구성된다. 단, 같은 임기의 투표 기록도 지워지므로 두 노드를 동시에 지우면 안 된다.
- 디스크 Full / fsync 실패 시 Raft 프로세스는 종료(75) 후 재기동 복구, Store는 격리(recovery required) 후 재기동 복구. 자동 시험은 주입 지점으로 수행한다(실제 디스크를 채우지 않음).

## 10. 디렉터리

```
crates/jamsil-common         오류 코드(JDB-xxxx), 값, 설정, 크기 상한, 복제 명령
crates/jamsil-ipc            프레임/RPC, 프로세스 간 메시지 계약
crates/jamsil-sql            PG→SQLite 변환, TEXT 규칙, TEXT_MATCH 재작성, 검색 DDL
crates/jamsil-storage        SQLite 엔진 (Writer/Reader/Patchset/Apply/Snapshot/Outbox/pg_catalog)
crates/jamsil-protocol       PG Wire 코덱, SCRAM, TLS, 값 인코딩
crates/jamsil-consensus      Raft 코어, 로그 세그먼트, 전송
crates/jamsil-search-engine  Tantivy/Lindera 인덱스
crates/jamsil-observability  tracing + jamsil-log 전달
bin/*                        6개 실행파일
config/                      standalone.toml, cluster.toml 설정 예시
packaging/, scripts/         RPM·systemd, 로컬 실행(dev.sh)·RPM 빌드·VM 시험 스크립트
examples/jdbc, examples/python  클라이언트 연계 샘플
tests/jdbc, tests/vm         JDBC 적합성·장애 주입·회귀·장애 전환 시험
docs/                        이 문서(REFERENCE.md), 설계서(DESIGN.md)
```
