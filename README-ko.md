# erounDB

> **erounDB** — a distributed relational database written in Rust: PostgreSQL wire protocol, SQLite storage, 3-node Raft synchronous replication, Korean full-text search.

Rust로 만든 분산 관계형 데이터베이스. **PostgreSQL 와이어 프로토콜**을 말하므로 DBeaver, 표준 PostgreSQL JDBC, psycopg 같은 기존 클라이언트로 바로 접속하고,
데이터는 **SQLite**에 저장하며 **Raft 3노드 동기 복제**로 노드 한 대가 죽어도 쓰기를 잃지 않는다. 한국어 형태소 분석 **전문 검색**(Tantivy + Lindera)과 최대 **32MiB TEXT**를 지원한다.

> **상태: 0.1.0 (PoC)** — 3노드 VM 장애 시험(Leader/Follower 강제 종료, 네트워크 분리, 2노드 중지, 전체 재기동)을 통과했지만 운영 투입 전에는 [알려진 제한](#알려진-제한)을 확인한다.
> 구현 사양·시험 방법/결과는 [docs/REFERENCE.md](docs/REFERENCE.md), 설계서는 [docs/DESIGN.md](docs/DESIGN.md).
>
> **이름에 관해:** 프로젝트(배포) 이름은 erounDB이고, 실행 파일(`jamsild` …)·크레이트·설정 경로(`/etc/jamsildb`)·서비스(`jamsildb.service`)·기본 DB/계정(`jamsil`)·SQL 확장(`jamsil_catalog`, `jamsil.request_id`)은 초기 코드명 `jamsil`을 그대로 쓴다. 문서와 예제의 `jamsil*`은 오타가 아니다.

## 목차

1. [특징](#특징)
2. [아키텍처](#아키텍처)
3. [빠른 시작 (로컬)](#빠른-시작-로컬)
4. [빌드와 배포 (Rocky Linux 9, 3노드)](#빌드와-배포-rocky-linux-9-3노드)
5. [클라이언트 연계](#클라이언트-연계) — [접속 정보](#접속-정보) · [Java](#java-jdbc) · [Python](#python-psycopg-3) · [DBeaver](#dbeaver)
6. [SQL 사용 시 알아둘 점](#sql-사용-시-알아둘-점)
7. [오류 코드와 재시도 가이드](#오류-코드와-재시도-가이드)
8. [시험](#시험)
9. [알려진 제한](#알려진-제한)
10. [저장소 구성](#저장소-구성)
11. [라이선스](#라이선스)

## 특징

| | |
|---|---|
| 접속 | PostgreSQL v3 프로토콜 (TCP **9786**), SCRAM-SHA-256 인증, TLS 필수(기본) |
| 복제 | 자체 구현 Raft, 3노드 동기 복제 (TCP **9787**). 과반수 fsync 후에만 성공 응답 → 응답받은 쓰기는 유실되지 않음 |
| 가용성 | Leader 종료 후 약 2.4초 안에 쓰기 재개 (3노드 시험). 어느 노드에 접속해도 읽기·쓰기 가능 (쓰기는 Leader로 자동 전달) |
| 정합성 | 읽기 기본값 linearizable, READ COMMITTED / REPEATABLE READ / SERIALIZABLE |
| 멱등 재시도 | `SET jamsil.request_id` — 응답이 유실된 쓰기를 다시 보내도 정확히 한 번만 반영 |
| TEXT | 최대 32MiB, `LIKE` 금지(`JDB-4102`), `TEXT_MATCH`/`TEXT_SCORE` 전문 검색(한국어·영어 분석기, BM25) |
| 프로세스 | 요청·저장·복제·검색·로그를 6개 OS 프로세스로 분리 (`jamsild`가 감독) |

## 아키텍처

```
Client ──PG v3(9786, TLS)──▶ jamsil-sql ──UDS──▶ jamsil-sync ──Raft(9787)──▶ 다른 노드
                                │                    │ (Follower면 Leader로 전달)
                                └──UDS──▶ jamsil-store (SQLite Writer/Reader, Apply)
                                              └─Outbox─▶ jamsil-search (Tantivy)
jamsild(감독) · jamsil-log(로그 수집)
```

모든 쓰기는 클러스터 전체에서 단일 Writer로 직렬화된다(SQLite 단일 Writer). 읽기는 접속한 노드의 복제본에서 처리한다.

## 빠른 시작 (로컬)

필요한 것: Linux(Rocky Linux 9에서 개발·시험), Rust stable (`rust-toolchain.toml`), `gcc`(SQLite bundled 빌드), JDK 17+(Java 샘플). `protoc`는 필요 없다.

```bash
git clone https://github.com/<owner>/erounDB.git && cd erounDB
cargo build --release          # 첫 빌드는 Lindera ko-dic 사전을 내려받으므로 인터넷이 필요하고 시간이 걸린다
scripts/dev.sh cluster         # 3노드: SQL 127.0.0.1:19786 / 29786 / 39786 (Raft 19787 / 29787 / 39787)
# 1노드만: scripts/dev.sh standalone   (127.0.0.1:9786)
scripts/dev.sh stop            # 종료
scripts/dev.sh clean           # 종료 + dev-data 삭제
```

개발 클러스터의 계정은 `jamsil` / `jamsil` 이다(**개발 전용**). 접속 URL:

```
jdbc:postgresql://127.0.0.1:19786,127.0.0.1:29786,127.0.0.1:39786/jamsil?sslmode=require&loadBalanceHosts=true
```

## 빌드와 배포 (Rocky Linux 9, 3노드)

### 1. 빌드

```bash
sudo dnf install -y gcc rpm-build git
# Rust: https://rustup.rs  (rust-toolchain.toml 이 stable 을 지정)
cargo test --workspace                 # 단위/통합 시험
cargo build --release                  # target/release/{jamsild,jamsil-sql,jamsil-store,jamsil-sync,jamsil-search,jamsil-log}
RELEASE=8 scripts/build-rpm.sh         # 배포용(thin LTO) RPM + 매니페스트
```

결과: `~/rpmbuild/RPMS/x86_64/eroundb-0.1.0-8.el9.x86_64.rpm`, `dist/eroundb-0.1.0-8.manifest`(빌드 ID·소스 해시·바이너리/RPM SHA-256 대응표).
`jamsild --version`이 `<버전> (build <git>+src.<소스 해시>)`를 출력하며 같은 값이 `SELECT version()`에도 나온다. 배포 후 매니페스트와 `sha256sum /usr/bin/jamsil*`을 대조하면 설치된 바이너리가 이 소스에서 나온 것임을 확인할 수 있다.

### 2. 설치 (노드마다)

```bash
sudo rpm -Uvh eroundb-0.1.0-8.el9.x86_64.rpm       # 바이너리, systemd 유닛, jamsildb 계정, /etc/jamsildb 설치
ls /etc/jamsildb/                                    # jamsildb.toml(1노드 기본), cluster.toml.example
```

### 3. 3노드 클러스터 설정

세 노드에서 `/etc/jamsildb/cluster.toml.example`을 `/etc/jamsildb/jamsildb.toml`로 복사해 고친다. 노드마다 다른 값은 `node_id`와 `cluster_bind` 뿐이다.

```bash
sudo cp /etc/jamsildb/cluster.toml.example /etc/jamsildb/jamsildb.toml
jamsild hash-password '<서비스 계정 비밀번호>'      # → SCRAM-SHA-256$4096:... (scram_secret 에 붙여넣기)
openssl rand -base64 48                              # → cluster_secret (세 노드 동일)
sudo vi /etc/jamsildb/jamsildb.toml
```

| 설정 | 노드 1 | 노드 2 | 노드 3 |
|---|---|---|---|
| `[server] node_id` | 1 | 2 | 3 |
| `[network] cluster_bind` | `10.0.0.1:9787` | `10.0.0.2:9787` | `10.0.0.3:9787` |
| `[security] cluster_secret` | **세 노드 동일** (긴 무작위 값) | | |
| `[cluster] cluster_id` | **세 노드 동일** | | |
| `[[cluster.peers]]` / `initial_voters` | **세 노드 동일** (1, 2, 3과 각 주소) | | |
| `[[security.users]]` | **세 노드 동일** | | |

- 사용자·역할은 설정 파일로 관리한다 (`superuser` / `readwrite` / `readonly`, 필요하면 `tables = [...]`로 접근 테이블 제한). 응용 프로그램에는 `readwrite` 계정을 쓰고 `superuser`는 관리용으로만 둔다.
- TLS: `cert_file`/`key_file`이 없으면 `data_dir/tls`에 자체 서명 인증서를 만든다. 운영에서는 인증서를 직접 지정한다.
- 노드 간 9787은 상호 인증(HMAC)만 하고 **암호화하지 않는다**. 9787은 클러스터 노드끼리만 통하도록 망/방화벽으로 막는다.
- 효과가 없는 옵션(`like_enabled = true`, `cluster_mtls = true` 등)은 조용히 무시하지 않고 기동을 거부한다.

```bash
# 방화벽 (firewalld 예): 9786 은 클라이언트 대역에, 9787 은 다른 노드에만 연다
sudo firewall-cmd --permanent --add-port=9786/tcp
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="10.0.0.2" port port="9787" protocol="tcp" accept'
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="10.0.0.3" port port="9787" protocol="tcp" accept'   # 노드 1 기준 — 노드마다 다른 두 노드를 연다
sudo firewall-cmd --reload
```

### 4. 기동과 확인

```bash
sudo systemctl enable --now jamsildb          # 세 노드 모두
sudo jamsild status --config /etc/jamsildb/jamsildb.toml
#   raft: role=leader term=… commit=… quorum=true ready=true     (Leader 에서만 ready=true)
#   peer 2 … connected=true match=…                              (Follower 의 match 가 commit 을 따라와야 함)
journalctl -u jamsildb -f                     # 로그 (JSON), /var/log/jamsildb/ 에도 기록
```

SQL로도 확인할 수 있다: `SELECT * FROM jamsil_catalog.cluster_nodes;`, `SELECT * FROM jamsil_catalog.replica_checksum;`(세 노드의 index·checksum이 같아야 한다).

## 클라이언트 연계

### 접속 정보

| 항목 | 값 |
|---|---|
| 호스트 / 포트 | **모든 노드**의 `ip:9786` |
| 데이터베이스 | `jamsil` |
| 사용자 / 비밀번호 | 설정 파일의 `[[security.users]]` 계정 |
| SSL | `require` (TLS 필수. 서버 인증서 검증은 기본값에서 하지 않음) |
| 드라이버 | 표준 PostgreSQL 드라이버 (pgjdbc 42.x, psycopg 3.x, DBeaver의 PostgreSQL) |

**접속 주소는 하나가 아니라 클러스터 전체를 적는다.** 어느 노드에 붙어도 읽기·쓰기가 되고, 한 노드만 적으면 그 노드가 죽을 때 서버 클러스터가 멀쩡해도 그 클라이언트는 장애가 된다.
다만 **끊긴 연결과 진행 중이던 트랜잭션은 복구되지 않는다.** 애플리케이션이 재연결하고 재시도해야 하며, 쓰기는 아래 "멱등 키"로 안전하게 재시도한다.

### 공통 규칙 (모든 언어)

(아래 예제는 `CREATE TABLE orders (id BIGINT PRIMARY KEY, customer TEXT, amount BIGINT)`가 있다고 가정한다.)

1. 모든 노드를 접속 주소에 나열하고, 연결 시간 제한(`connectTimeout`)과 응답 시간 제한(`socketTimeout`)을 설정한다. 응답 시간 제한이 없으면 Leader 교체·과반수 상실 중 무기한 대기할 수 있다.
2. 재시도할 오류: 연결 끊김 `08xxx` (`08007` = 커밋 결과 불명), 접속 실패, `57P03` (과반수 없음·선출 중), `40001` (직렬화 실패), `40P01`. 그 밖의 오류는 재시도하지 않는다.
3. **멱등 키:** 쓰기는 논리 작업(요청)마다 키를 한 번 만들고 재시도에 같은 키를 쓴다. 키는 다음 쓰기 문장 1건(트랜잭션이면 COMMIT)에 적용된다.
   ```sql
   SET jamsil.request_id = 'order-1001-6f1c…';        -- 요청마다 고유한 값
   INSERT INTO orders VALUES (1001, 'kim', 5000);      -- 응답을 못 받아 다시 보내도 한 번만 반영
   ```
   같은 키로 재시도하면 처음 결과(`RETURNING` 행, 영향 행 수)를 그대로 돌려준다. 같은 키에 다른 문장·파라미터를 쓰면 `22023`. 키는 서버가 지우지 않으므로 UUID처럼 고유하게 만든다.
4. 재시도되는 코드 안에 DB 밖의 부수 효과(메일 발송, 메시지 발행)를 두지 않는다.
5. 비밀번호를 코드·저장소에 쓰지 않는다 (환경변수/비밀 저장소).

### Java (JDBC)

**의존성** (Maven):

```xml
<dependency>
  <groupId>org.postgresql</groupId>
  <artifactId>postgresql</artifactId>
  <version>42.7.4</version>
</dependency>
```

**접속 URL** — 모든 노드를 쉼표로 나열한다:

```
jdbc:postgresql://10.0.0.1:9786,10.0.0.2:9786,10.0.0.3:9786/jamsil?sslmode=require&loadBalanceHosts=true&hostRecheckSeconds=2&connectTimeout=3&socketTimeout=30
```

**가장 단순한 사용** (재시도 없이 — 학습/도구용):

```java
Properties p = new Properties();
p.setProperty("user", System.getenv("JAMSIL_USER"));
p.setProperty("password", System.getenv("JAMSIL_PASSWORD"));
try (Connection c = DriverManager.getConnection(url, p);
     PreparedStatement s = c.prepareStatement("SELECT id, customer FROM orders WHERE amount > ?")) {
    s.setLong(1, 1000);
    try (ResultSet r = s.executeQuery()) { while (r.next()) System.out.println(r.getLong(1) + " " + r.getString(2)); }
}
```

**운영용 — 재연결·재시도·멱등 키를 갖춘 샘플** ([examples/jdbc/](examples/jdbc/), 외부 의존성 없음, 소스를 복사해 사용):

```java
try (JamsilClient db = JamsilClient.builder().hosts("10.0.0.1,10.0.0.2,10.0.0.3")
        .credentials(System.getenv("JAMSIL_USER"), System.getenv("JAMSIL_PASSWORD")).build()) {

    // 단일 문장 쓰기: 키는 논리 작업당 1개. 재시도돼도 한 번만 반영
    db.update(JamsilClient.newRequestKey("order-1001"),
              "INSERT INTO orders VALUES (?, ?, ?)", 1001L, "kim", 5000L);

    // 여러 문장 트랜잭션: 키가 COMMIT 에 적용된다
    // COMMIT 응답이 유실돼 본문이 다시 실행되면, 키가 이미 확정된 경우 23505 를 성공으로 처리한다(null 반환). 가능하면 본문도 재실행 가능하게 쓴다.
    db.write(JamsilClient.newRequestKey("order-1002"), true, c -> {
        try (PreparedStatement p = c.prepareStatement("INSERT INTO orders VALUES (?, ?, ?)")) {
            p.setLong(1, 1002); p.setString(2, "lee"); p.setLong(3, 7000); p.executeUpdate();
        }
        return null;
    });

    // 읽기: 끊기면 다른 노드로 재연결해 다시 실행
    long n = db.read(c -> { try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT count(*) FROM orders")) { r.next(); return r.getLong(1); } });
}
```

실행해 보기: `JAMSIL_USER=… JAMSIL_PASSWORD=… examples/jdbc/run.sh 10.0.0.1,10.0.0.2,10.0.0.3` — 자세한 규칙과 HikariCP 설정 예는 [examples/jdbc/README.md](examples/jdbc/README.md).
커넥션 풀을 써도 재시도와 멱등 키는 애플리케이션 몫이다(풀은 죽은 연결을 바꿔 줄 뿐 실패한 작업을 다시 실행하지 않는다).

### Python (psycopg 3)

```bash
pip install "psycopg[binary]"          # psycopg 3.x, Python 3.9+
```

**가장 단순한 사용**:

```python
import os, psycopg
with psycopg.connect(host="10.0.0.1", port=9786, dbname="jamsil", sslmode="require",
                     user=os.environ["JAMSIL_USER"], password=os.environ["JAMSIL_PASSWORD"],
                     connect_timeout=3, autocommit=True) as conn:
    for row in conn.execute("SELECT id, customer FROM orders WHERE amount > %s", (1000,)):
        print(row)
```

**운영용 샘플** ([examples/python/jamsil_client.py](examples/python/jamsil_client.py), 파일 하나, 복사해 사용): 노드를 섞어 차례로 접속하고, 끊기면 다른 노드로 재연결해 같은 멱등 키로 재시도한다.

```python
from jamsil_client import JamsilClient, new_request_key

with JamsilClient(["10.0.0.1", "10.0.0.2", "10.0.0.3"], os.environ["JAMSIL_USER"], os.environ["JAMSIL_PASSWORD"]) as db:
    # 단일 문장 쓰기 — 재시도돼도 한 번만 반영
    db.execute(new_request_key("order-1001"), "INSERT INTO orders VALUES (%s, %s, %s)", (1001, "kim", 5000))

    # RETURNING 결과도 재시도 시 그대로 재생된다
    amount = db.write(new_request_key("bump"), lambda c: c.execute(
        "UPDATE orders SET amount = amount + %s WHERE id = %s RETURNING amount", (100, 1001)).fetchone()[0])

    # 여러 문장 트랜잭션 — 키는 COMMIT 에 적용
    def place(c):
        c.execute("INSERT INTO orders VALUES (%s, %s, %s)", (1002, "lee", 7000))
    db.write(new_request_key("order-1002"), place, transaction=True)

    rows = db.read(lambda c: c.execute("SELECT id, customer, amount FROM orders ORDER BY id").fetchall())
```

실행: `JAMSIL_HOSTS=10.0.0.1,10.0.0.2,10.0.0.3 python examples/python/quickstart.py` — 설명과 장애 전환 데모는 [examples/python/README.md](examples/python/README.md).

### DBeaver

PostgreSQL 드라이버로 접속한다 (DBeaver의 PostgreSQL 탐색 질의 36개를 시험했다).

1. **Database → New Database Connection → PostgreSQL** (처음이면 드라이버 다운로드 안내를 수락).
2. **Main** 탭 — 한 노드만 쓸 때: *Connect by* **Host**, Host `10.0.0.1`, Port `9786`, Database `jamsil`, Username/Password 입력.
3. **노드 장애에도 끊기지 않게 하려면 모든 노드를 쓴다:** *Connect by* 를 **URL**로 바꾸고 JDBC URL에 모든 노드를 쉼표로 나열한다 (Host 칸에 쉼표 목록을 넣지 말고 URL 칸을 쓴다 — Host 칸은 별도 Port 값을 덧붙여 주소가 깨질 수 있다).
   ```
   jdbc:postgresql://10.0.0.1:9786,10.0.0.2:9786,10.0.0.3:9786/jamsil?sslmode=require&loadBalanceHosts=true&hostRecheckSeconds=2&connectTimeout=3&socketTimeout=30
   ```
   Username/Password는 *Authentication* 영역에 입력한다.
4. **SSL** — URL의 `sslmode=require`로 충분하다. Host 모드로 만들었다면 *SSL* 탭에서 **Use SSL**을 켜고 *SSL mode* 를 `require`로 둔다 (서버가 TLS를 필수로 요구한다). 메뉴 이름은 DBeaver 버전에 따라 조금 다를 수 있다.
5. **Test Connection** 후 저장. 연결하면 `jamsil` 데이터베이스와 `public` 스키마에서 테이블·컬럼·인덱스·제약을 탐색할 수 있고, SQL 편집기(Ctrl+Enter)로 질의한다.

사용 팁:
- DBeaver가 연결한 노드가 죽으면 연결이 끊긴다. *Invalidate/Reconnect*(우클릭 → Invalidate/Reconnect)로 URL에 적은 다른 노드에 다시 붙는다. 편집 중이던 트랜잭션은 롤백된다.
- 수동 커밋 모드(`Manual commit`)로 두면 트랜잭션이 열려 있는 동안 고정 Snapshot과 Writer를 잡고 있을 수 있다. 확인 후 곧 커밋/롤백한다 (유휴 트랜잭션은 `storage.idle_tx_timeout_ms` 후 해제).
- 시스템 정보 뷰: `jamsil_catalog.cluster_nodes`, `replication_status`, `storage_status`, `search_indexes`, `active_sessions`, `metrics`, `replica_checksum` (superuser 계정).
- 테이블에 PRIMARY KEY가 없으면 만들 수 없고(`JDB-5101`), TEXT 컬럼의 `LIKE`는 거부된다(`JDB-4102`) — 전문 검색은 `TEXT_MATCH`를 쓴다.

## SQL 사용 시 알아둘 점

```sql
-- 모든 테이블에는 PRIMARY KEY 가 필요하다
CREATE TABLE documents (id BIGINT PRIMARY KEY, title VARCHAR(200), content TEXT);

-- 전문 검색 (한국어 형태소 분석)
CREATE SEARCH INDEX idx_documents_content ON documents(content) WITH (analyzer = 'korean', scoring = 'bm25');
SELECT id, title, TEXT_SCORE(content) AS score
  FROM documents
 WHERE TEXT_MATCH(content, '금융 AND (주식 OR 채권)')
 ORDER BY TEXT_SCORE(content) DESC LIMIT 20;

-- 방금 쓴 문서까지 검색에 반영될 때까지 기다리려면
SELECT id FROM documents WHERE TEXT_MATCH(content, '"금융 투자"') WITH SEARCH CONSISTENCY COMMITTED;
```

- **지원:** SELECT/INSERT/UPDATE/DELETE/RETURNING/ON CONFLICT, JOIN, GROUP BY, 서브쿼리/CTE, 뷰, 인덱스, 제한된 ALTER TABLE, SAVEPOINT, Prepared Statement, `::` 캐스트,
  타입 SMALLINT/INTEGER/BIGINT/SERIAL, REAL/DOUBLE, BOOLEAN, CHAR/VARCHAR, TEXT(32MiB), BYTEA, DATE/TIME/TIMESTAMP/TIMESTAMPTZ(UTC), UUID, JSON/JSONB.
- **미지원/제한:** `DECIMAL/NUMERIC`(정확한 고정소수점 미구현), 시퀀스 함수, 트리거, 프로시저, TEMP 테이블, `COPY`, `LISTEN/NOTIFY`. DML 이후 같은 트랜잭션에서 DDL 불가. 시간대 변환 없음(UTC).
- **격리 수준:** 기본 READ COMMITTED. REPEATABLE READ/SERIALIZABLE은 첫 문장에서 접속 노드의 Snapshot을 고정하고, 첫 쓰기 시점에 그 뒤 다른 COMMIT이 있었으면 `40001`(재시도 대상).
  **고정 Snapshot 트랜잭션 안의 `TEXT_MATCH`/`TEXT_SCORE`는 `JDB-4105`(`0A000`)로 거부된다** — 검색은 트랜잭션 밖이나 READ COMMITTED에서 실행한다.
- **읽기 일관성:** 기본 `linearizable`(항상 최신 커밋). `SET jamsil.read_consistency = 'local'`은 접속한 노드의 현재 상태를 즉시 읽는다(뒤처질 수 있음).
- **TEXT `LIKE` 금지:** TEXT에서 파생된 모든 표현식에 적용된다. 패턴 검색이 필요한 값은 `VARCHAR(n)`으로 정의한다.
- 자세한 SQL 범위·권한·지표는 [docs/REFERENCE.md](docs/REFERENCE.md) §4~§6.

## 오류 코드와 재시도 가이드

서버 오류 메시지는 `JDB-xxxx` 코드를 담고, SQLSTATE는 표준 값을 쓴다.

| 코드 | SQLSTATE | 의미 | 클라이언트 동작 |
|---|---|---|---|
| `JDB-2001` | `57P03` | 과반수 없음 / Leader 선출 중 — 쓰기·linearizable 읽기 불가 | 재시도 (백오프) |
| `JDB-2002` | `40001` | 직렬화 실패 | 트랜잭션을 처음부터 재시도 |
| `JDB-2003` | `08007` | 커밋 결과 불명 (응답 유실·타임아웃) | **같은 멱등 키로** 재시도 |
| `JDB-4102` | | TEXT에 `LIKE` 사용 | 쿼리 수정 (`TEXT_MATCH` 또는 VARCHAR) |
| `JDB-4104` | `54000` | 검색 일치 문서가 `search.max_candidates` 초과 | 검색어를 좁힌다 |
| `JDB-4105` | `0A000` | RR/SERIALIZABLE 트랜잭션 안의 `TEXT_MATCH` | 트랜잭션 밖/READ COMMITTED에서 검색 |
| `JDB-5101` | | 복제 계약상 미지원 (PK 없음, NUMERIC 등) | 스키마/쿼리 수정 |
| `53200` | | 대형 메시지 메모리 예산 부족 | 잠시 후 재시도 |

## 시험

```bash
cargo test --workspace                       # Rust 단위/통합 시험
scripts/dev.sh standalone                    # 로컬 1노드 (127.0.0.1:9786, 계정 jamsil/jamsil — 개발 전용)
J="java -cp tests/jdbc/postgresql-42.7.4.jar"
$J tests/jdbc/JdbcConformance.java "jdbc:postgresql://127.0.0.1:9786/jamsil?sslmode=require" big   # JDBC 적합성 19항목 (+32MiB)
$J tests/jdbc/DbeaverCompat.java   "jdbc:postgresql://127.0.0.1:9786/jamsil?sslmode=require"   # DBeaver 탐색 질의 36개
tests/run-multihost-failover.sh 10.0.0.1,10.0.0.2,10.0.0.3   # 다중 호스트 장애 전환 (VM 3대, ssh+sudo systemctl 필요)
```

- 실제 VM 3대(Rocky 9.8)에서 Leader/Follower 강제 종료, 네트워크 분리, 2노드 중지, 전체 재기동, 데이터 삭제 노드 재구성, 정확한 장애 지점(fsync 직전/직후 등) 주입을 시험했다 — 결과와 방법은 [docs/REFERENCE.md](docs/REFERENCE.md) §8.
- 다중 호스트 장애 전환 시험(5건): 노드를 한 대씩 중지·재기동하며 쓰기를 계속해도 확정된 쓰기 472건이 3노드에 모두 있고 중복이 없다. 중지 후 첫 확정 쓰기까지 0.7~2.1초.
- 장애 주입 시험은 시험용 설정(`scripts/vm-acceptance.sh setup|restore`)을 적용하므로 **운영 노드에서 실행하지 않는다.**

## 알려진 제한

- 노드 간 9787은 인증만 하고 암호화하지 않는다 (mTLS 미구현) — 망을 분리한다.
- 쓰기 처리량은 클러스터 단일 Writer에 묶인다.
- `DECIMAL/NUMERIC`, 시퀀스 함수, 트리거, 프로시저, TEMP 테이블, `COPY`, `LISTEN/NOTIFY` 미지원. 시간대 변환 없음(UTC).
- 사용자·역할은 설정 파일로 관리한다 (`CREATE USER`/`GRANT` 미지원). `pg_catalog`는 JDBC/DBeaver가 쓰는 범위만 흉내 낸다.
- 결과 집합은 서버 메모리에 모아 전송한다 (세션당 512MiB 상한). TEXT 32MiB 동시 처리 시 `jamsil-sync` 메모리가 크게 늘 수 있다.
- 데이터를 지운 노드는 재기동 시 과반수에서 재구성되지만 **두 노드를 동시에 지우면 안 된다.**
- 설계 대비 판단(자체 Raft·자체 PG 와이어 구현 등)은 [docs/REFERENCE.md](docs/REFERENCE.md) §7.

## 저장소 구성

```
bin/                   6개 실행 파일 (jamsild, jamsil-sql, jamsil-store, jamsil-sync, jamsil-search, jamsil-log)
crates/                공통·IPC·SQL 변환·저장 엔진·PG 프로토콜·Raft·검색 엔진·관측
config/                standalone.toml, cluster.toml 설정 예시
packaging/, scripts/   RPM(eroundb.spec)·systemd, 로컬 실행(dev.sh)·RPM 빌드·VM 시험 스크립트
examples/jdbc/         Java 연계 샘플 (재시도·재연결·멱등 키)
examples/python/       Python(psycopg 3) 연계 샘플
tests/jdbc, tests/vm   JDBC 적합성·장애 주입·회귀·장애 전환 시험
docs/REFERENCE.md      구현 사양·정합성 계약·SQL 범위·시험·알려진 제한
docs/DESIGN.md         설계서
```

`tests/jdbc/postgresql-42.7.4.jar`는 시험용으로 포함한 PostgreSQL JDBC 드라이버(BSD-2-Clause, <https://jdbc.postgresql.org>)이며 erounDB의 코드가 아니다.

## 라이선스

[Apache License 2.0](LICENSE). `tests/jdbc/postgresql-42.7.4.jar`(PostgreSQL JDBC, BSD-2-Clause)와 Cargo로 받는 의존 크레이트는 각자의 라이선스를 따른다.
