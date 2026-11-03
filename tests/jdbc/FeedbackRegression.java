// 회귀 시험(FeedbackRegression, RQ-01~RQ-13). 3개 엔드포인트에 직접 접속한다.
//   JAMSIL_USER/JAMSIL_PASSWORD           : superuser 계정
//   JAMSIL_RW_USER/JAMSIL_RW_PASSWORD     : role=readwrite, tables=["qa_acl"] 계정 (없으면 F12 생략)
//   JAMSIL_RO_USER/JAMSIL_RO_PASSWORD     : role=readonly 계정 (없으면 F12 생략)
//   JAMSIL_TEST_MAX_CANDIDATES            : 서버 search.max_candidates 값 (지정 시 F09 실행)
//   java -cp postgresql-42.7.4.jar FeedbackRegression.java 127.0.0.1:19786,127.0.0.1:29786,127.0.0.1:39786
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class FeedbackRegression {
    static String[] EP;
    static int pass = 0, fail = 0;
    static final List<String> failures = new ArrayList<>();
    static final String P = "qa_fb_";

    interface Body { void run() throws Exception; }

    static final String ONLY = System.getenv("JAMSIL_ONLY");

    static void test(String name, Body b) {
        if (ONLY != null && Arrays.stream(ONLY.split(",")).noneMatch(name::startsWith)) return;
        long t0 = System.currentTimeMillis();
        try {
            b.run();
            pass++;
            System.out.printf("PASS %-62s %6d ms%n", name, System.currentTimeMillis() - t0);
        } catch (Throwable e) {
            fail++;
            failures.add(name + ": " + e);
            System.out.printf("FAIL %-62s %s%n", name, e);
        }
    }

    static void check(boolean c, String m) { if (!c) throw new AssertionError(m); }
    static void eq(Object a, Object b) { if (!Objects.equals(a, b)) throw new AssertionError("expected <" + b + "> but was <" + a + ">"); }

    static Connection conn(int n, String user, String pw) throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", user);
        p.setProperty("password", pw);
        return DriverManager.getConnection("jdbc:postgresql://" + EP[n % EP.length] + "/jamsil?sslmode=require&connectTimeout=5&socketTimeout=180&ApplicationName=feedback-regression", p);
    }
    static Connection conn(int n) throws SQLException {
        return conn(n, System.getenv().getOrDefault("JAMSIL_USER", "jamsil"), System.getenv().getOrDefault("JAMSIL_PASSWORD", "jamsil"));
    }
    static void exec(Connection c, String sql) throws SQLException { try (Statement s = c.createStatement()) { s.execute(sql); } }
    static long num(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) { check(r.next(), "no row: " + sql); return r.getLong(1); }
    }
    static String str(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) { check(r.next(), "no row: " + sql); return r.getString(1); }
    }
    static String state(Connection c, String sql) {
        try { exec(c, sql); return "ok"; } catch (SQLException e) { return e.getSQLState(); }
    }
    static void expectState(Connection c, String sql, String want) {
        String got = state(c, sql);
        check(want.equals(got), "expected SQLSTATE " + want + " but got " + got + " for: " + sql);
    }

    public static void main(String[] args) throws Exception {
        EP = (args.length > 0 ? args[0] : "127.0.0.1:19786,127.0.0.1:29786,127.0.0.1:39786").split(",");
        try (Connection c = conn(0)) {
            for (String t : List.of("iso", "sp", "f", "acl", "acl_other", "idem", "big", "sp_ddl"))
                exec(c, "DROP TABLE IF EXISTS " + P + t);
            exec(c, "DROP TABLE IF EXISTS qa_acl");
            exec(c, "CREATE TABLE " + P + "iso (id BIGINT PRIMARY KEY, v INT)");
            exec(c, "INSERT INTO " + P + "iso VALUES (1, 0)");
            num(c, "SELECT count(*) FROM " + P + "iso");

            // ---------------------------------------------------------------- RQ-06
            for (String level : List.of("REPEATABLE READ", "SERIALIZABLE")) {
                int iso = level.equals("SERIALIZABLE") ? Connection.TRANSACTION_SERIALIZABLE : Connection.TRANSACTION_REPEATABLE_READ;
                test("F01 " + level + ": no non-repeatable read / phantom", () -> {
                    exec(c, "UPDATE " + P + "iso SET v = 0 WHERE id = 1");
                    exec(c, "DELETE FROM " + P + "iso WHERE id > 1");
                    try (Connection a = conn(1); Connection b = conn(2)) {
                        a.setTransactionIsolation(iso);
                        a.setAutoCommit(false);
                        eq(str(a, "SHOW transaction_isolation"), level.toLowerCase());
                        eq(num(a, "SELECT v FROM " + P + "iso WHERE id = 1"), 0L);
                        eq(num(a, "SELECT count(*) FROM " + P + "iso"), 1L);
                        exec(b, "UPDATE " + P + "iso SET v = 1 WHERE id = 1");
                        exec(b, "INSERT INTO " + P + "iso VALUES (2, 2)");
                        eq(num(a, "SELECT v FROM " + P + "iso WHERE id = 1"), 0L);
                        eq(num(a, "SELECT count(*) FROM " + P + "iso"), 1L);
                        a.commit();
                        eq(num(a, "SELECT v FROM " + P + "iso WHERE id = 1"), 1L);
                        a.commit();
                    }
                });
                test("F02 " + level + ": read->write after concurrent commit = 40001", () -> {
                    try (Connection a = conn(2); Connection b = conn(0)) {
                        a.setTransactionIsolation(iso);
                        a.setAutoCommit(false);
                        long v = num(a, "SELECT v FROM " + P + "iso WHERE id = 1");
                        exec(b, "UPDATE " + P + "iso SET v = v + 100 WHERE id = 1");
                        expectState(a, "UPDATE " + P + "iso SET v = " + (v + 1) + " WHERE id = 1", "40001");
                        a.rollback();
                        // 재시도는 성공 (lost update 없음)
                        long v2 = num(a, "SELECT v FROM " + P + "iso WHERE id = 1");
                        eq(v2, v + 100);
                        exec(a, "UPDATE " + P + "iso SET v = " + (v2 + 1) + " WHERE id = 1");
                        a.commit();
                    }
                    eq(num(c, "SELECT v FROM " + P + "iso WHERE id = 1") % 100, 2L);
                });
            }
            test("F03 READ COMMITTED sees each committed change", () -> {
                try (Connection a = conn(1); Connection b = conn(2)) {
                    a.setAutoCommit(false);
                    eq(str(a, "SHOW transaction_isolation"), "read committed");
                    long v = num(a, "SELECT v FROM " + P + "iso WHERE id = 1");
                    exec(b, "UPDATE " + P + "iso SET v = v + 1 WHERE id = 1");
                    eq(num(a, "SELECT v FROM " + P + "iso WHERE id = 1"), v + 1);
                    a.commit();
                }
            });
            test("F04 READ ONLY transactions and invalid settings", () -> {
                try (Connection a = conn(0)) {
                    exec(a, "BEGIN READ ONLY");
                    expectState(a, "INSERT INTO " + P + "iso VALUES (9, 9)", "25006");
                    exec(a, "ROLLBACK");
                    exec(a, "BEGIN");
                    exec(a, "SELECT 1");
                    expectState(a, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", "25001");
                    exec(a, "ROLLBACK");
                    expectState(a, "BEGIN ISOLATION LEVEL SNAPSHOT", "22023");
                    exec(a, "SET default_transaction_read_only = on");
                    expectState(a, "INSERT INTO " + P + "iso VALUES (9, 9)", "25006");
                    exec(a, "SET default_transaction_read_only = off");
                    a.setReadOnly(true);
                    a.setAutoCommit(false);
                    expectState(a, "DELETE FROM " + P + "iso", "25006");
                    a.rollback();
                }
            });

            // ---------------------------------------------------------------- RQ-07
            test("F05 ROLLBACK TO SAVEPOINT after error (rows + DDL) converges", () -> {
                try (Connection a = conn(1)) {
                    a.setAutoCommit(false);
                    exec(a, "CREATE TABLE " + P + "sp (id BIGINT PRIMARY KEY, n INT)");
                    a.commit();
                    exec(a, "INSERT INTO " + P + "sp VALUES (1, 1)");
                    Savepoint sp = a.setSavepoint("keep");
                    exec(a, "INSERT INTO " + P + "sp VALUES (2, 2)");
                    expectState(a, "INSERT INTO " + P + "sp VALUES (1, 9)", "23505");
                    expectState(a, "SELECT 1", "25P02");
                    a.rollback(sp);
                    exec(a, "INSERT INTO " + P + "sp VALUES (3, 3)");
                    a.commit();
                    // savepoint 이후 DDL 되돌림
                    Savepoint s2 = a.setSavepoint("ddl");
                    exec(a, "CREATE TABLE " + P + "sp_ddl (id BIGINT PRIMARY KEY)");
                    a.rollback(s2);
                    a.commit();
                }
                for (int h = 0; h < 3; h++) try (Connection n = conn(h)) {
                    eq(str(n, "SELECT string_agg(CAST(id AS TEXT), ',') FROM (SELECT id FROM " + P + "sp ORDER BY id) q"), "1,3");
                    expectState(n, "SELECT * FROM " + P + "sp_ddl", "42P01");
                }
            });

            // ---------------------------------------------------------------- RQ-08/09
            test("F06 TEXT_MATCH column scope, explicit field, TEXT_SCORE", () -> {
                exec(c, "CREATE TABLE " + P + "f (id BIGINT PRIMARY KEY, title TEXT, body TEXT)");
                exec(c, "INSERT INTO " + P + "f VALUES (1,'alpha','beta'),(2,'gamma','alpha'),(3,'alpha alpha','alpha')");
                exec(c, "CREATE SEARCH INDEX " + P + "f_ix ON " + P + "f(title, body) WITH (analyzer='standard')");
                for (int h = 0; h < 3; h++) try (Connection n = conn(h)) {
                    eq(str(n, "SELECT string_agg(CAST(id AS TEXT), ',') FROM (SELECT id FROM " + P + "f WHERE TEXT_MATCH(body,'alpha') ORDER BY id) q WITH SEARCH CONSISTENCY COMMITTED"), "2,3");
                    eq(str(n, "SELECT string_agg(CAST(id AS TEXT), ',') FROM (SELECT id FROM " + P + "f WHERE TEXT_MATCH(title,'alpha') ORDER BY id) q WITH SEARCH CONSISTENCY COMMITTED"), "1,3");
                    eq(num(n, "SELECT id FROM " + P + "f WHERE TEXT_MATCH(title,'alpha') ORDER BY TEXT_SCORE(title) DESC LIMIT 1 WITH SEARCH CONSISTENCY COMMITTED"), 3L);
                    eq(num(n, "SELECT count(*) FROM " + P + "f WHERE TEXT_MATCH(title,'alpha') AND TEXT_MATCH(body,'beta') WITH SEARCH CONSISTENCY COMMITTED"), 1L);
                    expectState(n, "SELECT id FROM " + P + "f WHERE TEXT_MATCH(body,'title:alpha') WITH SEARCH CONSISTENCY COMMITTED", "42601");
                }
            });
            String maxc = System.getenv("JAMSIL_TEST_MAX_CANDIDATES");
            if (maxc != null) test("F07 more matches than max_candidates fails explicitly (JDB-4104)", () -> {
                int limit = Integer.parseInt(maxc);
                exec(c, "DROP TABLE IF EXISTS " + P + "big");
                exec(c, "CREATE TABLE " + P + "big (id BIGINT PRIMARY KEY, body TEXT)");
                exec(c, "CREATE SEARCH INDEX " + P + "big_ix ON " + P + "big(body) WITH (analyzer='standard')");
                c.setAutoCommit(false);
                for (int base = 0; base <= limit; base += 1000) {
                    StringBuilder b = new StringBuilder("INSERT INTO " + P + "big VALUES ");
                    for (int i = base; i < Math.min(base + 1000, limit + 1); i++) b.append(i > base ? "," : "").append("(").append(i).append(",'needle')");
                    exec(c, b.toString());
                }
                c.commit();
                c.setAutoCommit(true);
                try (Statement s = c.createStatement()) {
                    s.executeQuery("SELECT count(*) FROM " + P + "big WHERE TEXT_MATCH(body,'needle') WITH SEARCH CONSISTENCY COMMITTED");
                    throw new AssertionError("incomplete result returned as success");
                } catch (SQLException e) {
                    check(e.getMessage().contains("JDB-4104"), e.toString());
                }
                exec(c, "DELETE FROM " + P + "big WHERE id = 0");
                eq(num(c, "SELECT count(*) FROM " + P + "big WHERE TEXT_MATCH(body,'needle') WITH SEARCH CONSISTENCY COMMITTED"), (long) limit);
            });

            // ---------------------------------------------------------------- RQ-12
            test("F08 required metrics exist and move", () -> {
                Map<String, Double> before = metrics(c);
                for (String n : List.of("jamsil_sql_duration_seconds_count", "jamsil_raft_fsync_duration_seconds_count",
                        "jamsil_storage_fsync_duration_seconds_count", "jamsil_storage_disk_free_bytes", "jamsil_mutation_payload_bytes_count",
                        "jamsil_large_text_inflight_bytes", "jamsil_search_query_duration_seconds_count", "jamsil_ipc_queue_depth",
                        "jamsil_ipc_backpressure_events_total"))
                    check(before.keySet().stream().anyMatch(k -> k.startsWith(n + "|")), "missing metric " + n);
                exec(c, "INSERT INTO " + P + "iso VALUES (100, 1)");
                num(c, "SELECT count(*) FROM " + P + "f WHERE TEXT_MATCH(body,'alpha')");
                Map<String, Double> after = metrics(c);
                for (String k : List.of("jamsil-sql|jamsil_sql_duration_seconds_count|kind=write", "jamsil-store|jamsil_mutation_payload_bytes_count|",
                        "jamsil-search|jamsil_search_query_duration_seconds_count|")) {
                    String key = k.substring(k.indexOf('|') + 1);
                    double b0 = before.getOrDefault(key, 0.0), a0 = after.getOrDefault(key, 0.0);
                    System.out.println("  " + key + " " + b0 + " -> " + a0);
                }
                check(after.getOrDefault("jamsil_sql_duration_seconds_count|kind=write", 0.0) > before.getOrDefault("jamsil_sql_duration_seconds_count|kind=write", 0.0), "sql duration count did not move");
                check(after.containsKey("jamsil_sql_duration_seconds|kind=read,quantile=0.99"), "p99 missing");
            });

            // ---------------------------------------------------------------- RQ-13
            String rwU = System.getenv("JAMSIL_RW_USER"), roU = System.getenv("JAMSIL_RO_USER");
            if (rwU != null && roU != null) test("F09 non-superuser roles enforced", () -> {
                exec(c, "CREATE TABLE qa_acl (id BIGINT PRIMARY KEY, v INT)");
                exec(c, "CREATE TABLE " + P + "acl_other (id BIGINT PRIMARY KEY, secret TEXT)");
                exec(c, "INSERT INTO " + P + "acl_other VALUES (1, 'x')");
                for (int h = 0; h < 3; h++) {
                    try (Connection rw = conn(h, rwU, System.getenv("JAMSIL_RW_PASSWORD"))) {
                        eq(str(rw, "SHOW is_superuser").toLowerCase(), "off");
                        exec(rw, "INSERT INTO qa_acl VALUES (" + (10 + h) + ", 1)");
                        exec(rw, "UPDATE qa_acl SET v = v + 1 WHERE id = " + (10 + h));
                        num(rw, "SELECT count(*) FROM qa_acl");
                        num(rw, "SELECT count(*) FROM pg_catalog.pg_class");
                        expectState(rw, "SELECT * FROM " + P + "acl_other", "42501");
                        expectState(rw, "INSERT INTO " + P + "acl_other VALUES (2, 'y')", "42501");
                        expectState(rw, "CREATE TABLE " + P + "acl_new (id BIGINT PRIMARY KEY)", "42501");
                        expectState(rw, "DROP TABLE qa_acl", "42501");
                        expectState(rw, "SELECT * FROM jamsil_catalog.active_sessions", "42501");
                        expectState(rw, "SELECT * FROM _jamsil_requests", "42501");
                        eq(num(rw, "SELECT CASE WHEN has_table_privilege('qa_acl','INSERT') THEN 1 ELSE 0 END"), 1L);
                        eq(num(rw, "SELECT CASE WHEN has_table_privilege('" + P + "acl_other','SELECT') THEN 1 ELSE 0 END"), 0L);
                        eq(str(rw, "SELECT current_user"), rwU);
                    }
                    try (Connection ro = conn(h, roU, System.getenv("JAMSIL_RO_PASSWORD"))) {
                        num(ro, "SELECT count(*) FROM qa_acl");
                        expectState(ro, "INSERT INTO qa_acl VALUES (99, 1)", "42501");
                        expectState(ro, "DELETE FROM qa_acl", "42501");
                        eq(num(ro, "SELECT CASE WHEN has_table_privilege('qa_acl','UPDATE') THEN 1 ELSE 0 END"), 0L);
                    }
                }
                eq(num(c, "SELECT count(*) FROM " + P + "acl_other"), 1L);
                eq(num(c, "SELECT sum(v) FROM qa_acl"), 6L);
            });

            // ---------------------------------------------------------------- RQ-01
            test("F10 64 concurrent retries of one request_id: one effect, same RETURNING", () -> {
                exec(c, "CREATE TABLE " + P + "idem (id BIGINT PRIMARY KEY, n INT)");
                exec(c, "INSERT INTO " + P + "idem VALUES (1, 0)");
                for (int h = 0; h < 3; h++) try (Connection n = conn(h)) { num(n, "SELECT count(*) FROM " + P + "idem"); }
                for (int threads : List.of(16, 64)) {
                    String key = "fb-" + UUID.randomUUID();
                    ExecutorService pool = Executors.newFixedThreadPool(threads);
                    CountDownLatch ready = new CountDownLatch(threads), go = new CountDownLatch(1);
                    List<Future<String>> fs = new ArrayList<>();
                    for (int k = 0; k < threads; k++) {
                        int host = k;
                        fs.add(pool.submit(() -> {
                            try (Connection a = conn(host)) {
                                exec(a, "SET jamsil.request_id = '" + key + "'");
                                ready.countDown();
                                go.await();
                                return String.valueOf(num(a, "UPDATE " + P + "idem SET n = n + 1 WHERE id = 1 RETURNING n"));
                            } catch (Exception e) { ready.countDown(); return "ERR " + e.getMessage(); }
                        }));
                    }
                    check(ready.await(60, TimeUnit.SECONDS), "not ready");
                    long before = num(c, "SELECT n FROM " + P + "idem WHERE id = 1");
                    go.countDown();
                    Set<String> outcomes = new TreeSet<>();
                    for (Future<String> f : fs) outcomes.add(f.get(180, TimeUnit.SECONDS));
                    pool.shutdown();
                    long after = num(c, "SELECT n FROM " + P + "idem WHERE id = 1");
                    System.out.println("  threads=" + threads + " effects=" + (after - before) + " distinct outcomes=" + outcomes);
                    eq(after - before, 1L);
                    eq(outcomes, Set.of(String.valueOf(after)));
                }
            });

            // ---------------------------------------------------------------- RQ-11
            test("F11 concurrent 32MiB writes: success or explicit 53200, budget returned", () -> {
                exec(c, "CREATE TABLE " + P + "big (id BIGINT PRIMARY KEY, body TEXT)");
                String body = "가".repeat(11_184_810) + "x"; // 33,554,431 bytes UTF-8
                int n = 6;
                ExecutorService pool = Executors.newFixedThreadPool(n);
                List<Future<String>> fs = new ArrayList<>();
                for (int k = 0; k < n; k++) {
                    int id = k;
                    fs.add(pool.submit(() -> {
                        try (Connection a = conn(id); PreparedStatement ps = a.prepareStatement("INSERT INTO " + P + "big VALUES (?, ?)")) {
                            ps.setLong(1, id);
                            ps.setString(2, body);
                            ps.executeUpdate();
                            return "ok";
                        } catch (SQLException e) { return e.getSQLState(); }
                    }));
                }
                Map<String, Integer> outcome = new TreeMap<>();
                for (Future<String> f : fs) outcome.merge(f.get(600, TimeUnit.SECONDS), 1, Integer::sum);
                pool.shutdown();
                System.out.println("  outcomes=" + outcome);
                // 허용 결과: 성공 / 메모리 예산 부족(53200) / 쓰기 잠금 대기 초과(55P03) / 결과 불명(08007, 재조회 필요)
                for (String k : outcome.keySet()) check(Set.of("ok", "53200", "55P03", "08007").contains(k), "unexpected outcome " + k);
                for (int h = 0; h < 3; h++) try (Connection x = conn(h)) {
                    // 결과 불명 요청은 Leader에서 아직 처리 중일 수 있으므로 반납될 때까지 기다린다.
                    Map<String, Double> m = metrics(x);
                    for (int w = 0; w < 120 && m.getOrDefault("jamsil_large_text_inflight_bytes|", 0.0) != 0.0; w++) {
                        Thread.sleep(1000);
                        m = metrics(x);
                    }
                    check(m.getOrDefault("jamsil_large_text_inflight_bytes|", 0.0) == 0.0, "budget not returned on " + EP[h] + ": " + m.get("jamsil_large_text_inflight_bytes|"));
                    System.out.println("  " + EP[h] + " peak=" + m.entrySet().stream().filter(e -> e.getKey().startsWith("jamsil_large_text_inflight_peak_bytes")).map(e -> e.getValue().longValue() >> 20).toList() + " MiB"
                            + " waits/rejects=" + m.entrySet().stream().filter(e -> e.getKey().startsWith("jamsil_ipc_backpressure_events_total|reason=budget")).map(e -> e.getValue().longValue()).toList());
                }
                long stored = num(c, "SELECT count(*) FROM " + P + "big");
                int ok = outcome.getOrDefault("ok", 0), unknown = outcome.getOrDefault("08007", 0);
                check(stored >= ok && stored <= ok + unknown, "stored=" + stored + " ok=" + ok + " unknown=" + unknown);
                for (int h = 0; h < 3; h++) try (Connection x = conn(h)) { eq(num(x, "SELECT count(*) FROM " + P + "big"), stored); }
            });
        } finally {
            try (Connection c = conn(0)) {
                for (String t : List.of("iso", "sp", "f", "acl_other", "idem", "big", "sp_ddl")) exec(c, "DROP TABLE IF EXISTS " + P + t);
                exec(c, "DROP TABLE IF EXISTS qa_acl");
            }
        }
        System.out.printf("%nRESULT: %d passed, %d failed%n", pass, fail);
        for (String f : failures) System.out.println("  - " + f);
        System.exit(fail == 0 ? 0 : 1);
    }

    /** name|labels → 합계 (같은 이름·레이블은 프로세스별 값을 더한다; 분위수는 최대값). */
    static Map<String, Double> metrics(Connection c) throws SQLException {
        Map<String, Double> m = new TreeMap<>();
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT process, name, labels, value FROM jamsil_catalog.metrics")) {
            while (r.next()) {
                String k = r.getString(2) + "|" + r.getString(3);
                double v = r.getDouble(4);
                if (k.contains("quantile")) m.merge(k, v, Math::max); else m.merge(k, v, Double::sum);
            }
        }
        return m;
    }
}
