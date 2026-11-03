# erounDB JDBC 연계 샘플

| 파일 | 설명 |
|---|---|
| `JamsilClient.java` | 운영 코드로 복사해 쓸 수 있는 얇은 클라이언트: 다중 호스트 URL, 재연결·재시도, 멱등 키 |
| `Quickstart.java` | 사용 예 (단일 문장 쓰기 / RETURNING / 다중 문장 트랜잭션 / 읽기) |
| `run.sh` | 컴파일·실행 (`javac` 또는 `jdk.compiler` 모듈, pgjdbc 42.x) |

```bash
export JAMSIL_USER=... JAMSIL_PASSWORD=...        # 코드·저장소에 비밀번호를 쓰지 않는다
examples/jdbc/run.sh 10.0.0.1,10.0.0.2,10.0.0.3
```

## 반드시 지킬 규칙

1. **URL에 모든 노드를 나열한다.** 한 노드만 쓰면 그 노드 장애가 곧 서비스 장애다. 어느 노드에 붙어도 읽기·쓰기가 된다.
2. **재시도 가능한 오류는 새 연결로 처음부터 다시 실행한다:** `08xxx`(연결 끊김·`08007` 커밋 결과 불명), `57P03`(과반수 없음·선출 중), `40001`(직렬화 실패), `40P01`.
   `JamsilClient.isRetryable(e)`가 이 판정이다. 그 외 오류(`23505` 제약 위반, `42xxx` 문법·권한, `22023` 키 오용 등)는 재시도하지 않는다.
3. **쓰기는 논리 작업마다 멱등 키를 한 번만 만들고 재시도에 같은 키를 쓴다.** 응답을 못 받은 쓰기도 정확히 한 번만 반영된다.
   - autocommit 단일 문장: 키가 그 문장에 적용되고 `RETURNING` 결과·영향 행 수까지 재생된다 (`write(key, false, …)`, `update(…)`).
   - 여러 문장 트랜잭션: 키가 COMMIT에 적용된다 (`write(key, true, …)`). 재실행 중의 문장 결과는 버려지므로 결과 재생이 필요하면 단일 문장으로 쓴다.
     COMMIT 응답이 유실된 뒤 재실행하면 `INSERT`가 서버의 COMMIT 시점 멱등 확인보다 먼저 PK 충돌(`23505`)로 실패한다. `write(…, true, …)`는 이때 같은 키가 이미 확정됐는지 확인해 그렇다면 `null`을 돌려주고(성공), 확정된 적 없는 진짜 중복이면 `23505`를 그대로 던진다. 이 확인은 오류 메시지에서 충돌한 테이블 이름을 얻을 수 있을 때만 동작하므로, 본문은 가능하면 `ON CONFLICT DO NOTHING`처럼 다시 실행해도 실패하지 않게 쓴다.
   - 키는 서버가 삭제하지 않는다. 요청마다 고유해야 하고, 같은 키에 다른 문장·파라미터를 쓰면 `22023`.
4. **재시도 본문은 DB 밖의 부수 효과를 만들지 않는다** (메일 발송, 메시지 발행 등). 실패한 시도의 본문이 다시 실행된다.
5. **`socketTimeout`을 설정한다.** Leader 교체·과반수 상실 중 응답이 오지 않을 때 연결을 끊고 재시도하기 위해서다 (샘플 기본 30초).
6. 고정 Snapshot(`REPEATABLE READ`/`SERIALIZABLE`) 트랜잭션에서는 `TEXT_MATCH`/`TEXT_SCORE`가 `JDB-4105`(0A000)로 거부된다 — 검색은 트랜잭션 밖이나 `READ COMMITTED`에서.

## 커넥션 풀을 쓸 때 (HikariCP 예, 참고 설정)

```java
HikariConfig c = new HikariConfig();
c.setJdbcUrl("jdbc:postgresql://10.0.0.1:9786,10.0.0.2:9786,10.0.0.3:9786/jamsil"
           + "?sslmode=require&loadBalanceHosts=true&hostRecheckSeconds=2&connectTimeout=3&socketTimeout=30");
c.setUsername(System.getenv("JAMSIL_USER")); c.setPassword(System.getenv("JAMSIL_PASSWORD"));
c.setConnectionTimeout(5_000);
c.setMaxLifetime(300_000);      // 주기적으로 새 연결 → 노드 복귀 후 부하가 다시 분산된다
c.setKeepaliveTime(30_000);
```

풀을 써도 위 2~4의 재시도·멱등 키는 애플리케이션 몫이다 (풀은 죽은 연결을 교체할 뿐 실패한 작업을 다시 실행하지 않는다).
Spring이면 `@Retryable`(대상 예외를 위 판정으로 제한)에 같은 요청 키를 넘기는 식으로 감싼다.

## 검증

`tests/run-multihost-failover.sh`가 이 클라이언트로 실제 VM 3대를 한 대씩 중지·재기동하며 재연결·무손실·무중복을 확인한다 (docs/REFERENCE.md §8.4).
