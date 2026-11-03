// R2-03 회귀 시험: 고정 Snapshot(REPEATABLE READ/SERIALIZABLE) 트랜잭션의 TEXT_MATCH/TEXT_SCORE는
// 성공 결과 대신 항상 JDB-4105(SQLSTATE 0A000)로 거부되고, 그 밖의 검색과 고정 Snapshot 일반 조회는 그대로 동작해야 한다.
//   JAMSIL_USER/JAMSIL_PASSWORD : superuser 계정
//   java -cp postgresql-42.7.4.jar SnapshotSearchRegression.java 10.0.0.1:9786,10.0.0.2:9786,10.0.0.3:9786
import java.sql.*;
import java.util.*;

public class SnapshotSearchRegression {
    static String[] EP;
    static int pass = 0, fail = 0;
    static final List<String> failures = new ArrayList<>();
    static final String T = "qa_r203_docs", IX = "qa_r203_docs_ix";
    static final String COMMITTED = " WITH SEARCH CONSISTENCY COMMITTED";

    interface Body { void run() throws Exception; }

    static void test(String name, Body b) {
        long t0 = System.currentTimeMillis();
        try {
            b.run();
            pass++;
            System.out.printf("PASS %-70s %6d ms%n", name, System.currentTimeMillis() - t0);
        } catch (Throwable e) {
            fail++;
            failures.add(name + ": " + e);
            System.out.printf("FAIL %-70s %s%n", name, e);
        }
    }

    static void check(boolean c, String m) { if (!c) throw new AssertionError(m); }
    static void eq(Object a, Object b) { if (!Objects.equals(a, b)) throw new AssertionError("expected <" + b + "> but was <" + a + ">"); }

    static Connection conn(int n, boolean simple) throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", System.getenv().getOrDefault("JAMSIL_USER", "jamsil"));
        p.setProperty("password", System.getenv().getOrDefault("JAMSIL_PASSWORD", "jamsil"));
        if (simple) p.setProperty("preferQueryMode", "simple");
        return DriverManager.getConnection("jdbc:postgresql://" + EP[n % EP.length] + "/jamsil?sslmode=require&connectTimeout=5&socketTimeout=120&ApplicationName=r2-03-regression", p);
    }
    static void exec(Connection c, String sql) throws SQLException { try (Statement s = c.createStatement()) { s.execute(sql); } }
    static List<String> rows(Connection c, String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
            int n = r.getMetaData().getColumnCount();
            while (r.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= n; i++) b.append(i > 1 ? "|" : "").append(r.getString(i));
                out.add(b.toString());
            }
        }
        return out;
    }
    /** PreparedStatement(확장 프로토콜, 바인드 파라미터) 경로의 검색. */
    static List<String> prepared(Connection c, String sql, String q) throws SQLException {
        List<String> out = new ArrayList<>();
        try (PreparedStatement p = c.prepareStatement(sql)) {
            p.setString(1, q);
            try (ResultSet r = p.executeQuery()) { while (r.next()) out.add(r.getString(1)); }
        }
        return out;
    }
    static String lastMessage = "";
    /** 실패해야 하는 문장: "SQLSTATE[ JDB-4105]" 또는 "success" (메시지는 lastMessage에 남긴다). */
    static String error(Body b) {
        lastMessage = "";
        try { b.run(); } catch (SQLException e) {
            lastMessage = String.valueOf(e.getMessage()).split("\n")[0];
            return e.getSQLState() + (lastMessage.contains("JDB-4105") ? " JDB-4105" : "");
        } catch (Exception e) { lastMessage = String.valueOf(e); return "unexpected"; }
        return "success";
    }
    static void rejected(String what, Body b) {
        String got = error(b);
        check(got.equals("0A000 JDB-4105"), what + ": expected 0A000 JDB-4105 but got " + got + " (" + lastMessage + ")");
    }

    /** 현재 Committed 검색이 기대값이 될 때까지 기다린다 (타 세션 변경이 색인에 반영됨을 확인). */
    static void awaitSearch(Connection c, String q, List<String> want) throws Exception {
        List<String> got = null;
        for (int i = 0; i < 100; i++) {
            got = rows(c, "SELECT id FROM " + T + " WHERE TEXT_MATCH(body, '" + q + "') ORDER BY id" + COMMITTED);
            if (got.equals(want)) return;
            Thread.sleep(100);
        }
        throw new AssertionError("committed search for " + q + " never became " + want + " (last " + got + ")");
    }

    static void reset(Connection c) throws Exception {
        exec(c, "DELETE FROM " + T);
        exec(c, "INSERT INTO " + T + " VALUES (1, 'alpha', 1), (2, 'beta', 2)");
        awaitSearch(c, "alpha", List.of("1"));
        awaitSearch(c, "beta", List.of("2"));
    }

    /** 고정 Snapshot 트랜잭션에서 시험하는 검색 문장들 (조건·점수·SQL 필터·서브쿼리·바인드 파라미터). */
    interface Search { void run(Connection a) throws Exception; }
    static final Map<String, Search> FORMS = new LinkedHashMap<>();
    static {
        FORMS.put("TEXT_MATCH committed", a -> rows(a, "SELECT id FROM " + T + " WHERE TEXT_MATCH(body, 'alpha')" + COMMITTED));
        FORMS.put("TEXT_MATCH eventual", a -> rows(a, "SELECT id FROM " + T + " WHERE TEXT_MATCH(body, 'alpha')"));
        FORMS.put("TEXT_SCORE order", a -> rows(a, "SELECT id, TEXT_SCORE(body) FROM " + T + " WHERE TEXT_MATCH(body, 'alpha') ORDER BY TEXT_SCORE(body) DESC" + COMMITTED));
        FORMS.put("TEXT_MATCH + SQL filter", a -> rows(a, "SELECT id FROM " + T + " WHERE TEXT_MATCH(body, 'alpha OR beta') AND n > 1" + COMMITTED));
        FORMS.put("TEXT_MATCH in subquery", a -> rows(a, "SELECT count(*) FROM " + T + " WHERE id IN (SELECT id FROM " + T + " WHERE TEXT_MATCH(body, 'alpha'))" + COMMITTED));
        FORMS.put("prepared TEXT_MATCH($1)", a -> prepared(a, "SELECT id FROM " + T + " WHERE TEXT_MATCH(body, ?)", "alpha"));
    }

    public static void main(String[] args) throws Exception {
        EP = (args.length > 0 ? args[0] : "127.0.0.1:19786,127.0.0.1:29786,127.0.0.1:39786").split(",");
        try (Connection c = conn(0, false)) {
            exec(c, "DROP TABLE IF EXISTS " + T);
            exec(c, "CREATE TABLE " + T + " (id BIGINT PRIMARY KEY, body TEXT, n INT)");
            exec(c, "CREATE SEARCH INDEX " + IX + " ON " + T + "(body) WITH (analyzer = 'standard')");
            reset(c);
        }
        for (String level : List.of("REPEATABLE READ", "SERIALIZABLE")) {
            int iso = level.equals("SERIALIZABLE") ? Connection.TRANSACTION_SERIALIZABLE : Connection.TRANSACTION_REPEATABLE_READ;
            for (boolean simple : List.of(false, true)) {
                String tag = level + (simple ? " simple" : " extended");

                test("R01 " + tag + ": N01 reproduction — search rejected, not a wrong row", () -> {
                    try (Connection a = conn(1, simple); Connection b = conn(2, false)) {
                        reset(b);
                        a.setTransactionIsolation(iso); a.setAutoCommit(false);
                        String q = "SELECT id FROM " + T + " WHERE TEXT_MATCH(body, 'alpha')" + COMMITTED;
                        String first = error(() -> rows(a, q));
                        System.out.println("  first search in " + tag + ": " + first);
                        if (first.equals("success")) {
                            // 수정 전 동작 기록: 같은 Snapshot에서 검색 결과가 바뀌어 조건이 거짓인 행을 돌려준다 (N01)
                            List<String> before = rows(a, q);
                            exec(b, "UPDATE " + T + " SET body = CASE WHEN id = 1 THEN 'beta' ELSE 'alpha' END");
                            awaitSearch(b, "alpha", List.of("2"));
                            List<String> after = rows(a, q);
                            System.out.println("  same snapshot: TEXT_MATCH(body,'alpha') before=" + before + " after=" + after
                                + "; pinned bodies " + rows(a, "SELECT id || ':' || body FROM " + T + " ORDER BY id"));
                            a.rollback();
                        }
                        check(first.equals("0A000 JDB-4105"), "first search: " + first);
                        // 오류 뒤 트랜잭션은 실패 상태 → 다음 문장 25P02, ROLLBACK 후 재사용 가능
                        eq(error(() -> rows(a, "SELECT 1")), "25P02");
                        a.rollback();
                        eq(rows(a, "SELECT body FROM " + T + " WHERE id = 1"), List.of("alpha"));
                        exec(b, "UPDATE " + T + " SET body = CASE WHEN id = 1 THEN 'beta' ELSE 'alpha' END");
                        awaitSearch(b, "alpha", List.of("2"));
                        rejected(tag + " search after swap", () -> rows(a, q));
                        a.rollback();
                    }
                });

                test("R02 " + tag + ": other-session INSERT/UPDATE/DELETE, PK reinsert, index rebuild", () -> {
                    record Change(String name, String[] sql, String q, List<String> want) {}
                    List<Change> changes = List.of(
                        new Change("INSERT", new String[]{"INSERT INTO " + T + " VALUES (3, 'alpha', 3)"}, "alpha", List.of("1", "3")),
                        new Change("UPDATE swap", new String[]{"UPDATE " + T + " SET body = CASE WHEN id = 1 THEN 'beta' ELSE 'alpha' END"}, "alpha", List.of("2")),
                        new Change("DELETE", new String[]{"DELETE FROM " + T + " WHERE id = 1"}, "alpha", List.of()),
                        new Change("same PK reinsert", new String[]{"DELETE FROM " + T + " WHERE id = 1", "INSERT INTO " + T + " VALUES (1, 'alpha gamma', 10)"}, "gamma", List.of("1")),
                        new Change("index rebuild", new String[]{"DROP SEARCH INDEX " + IX, "CREATE SEARCH INDEX " + IX + " ON " + T + "(body) WITH (analyzer = 'standard')"}, "alpha", List.of("1")));
                    try (Connection a = conn(1, simple); Connection b = conn(2, false)) {
                        for (Change ch : changes) {
                            for (Map.Entry<String, Search> f : FORMS.entrySet()) {
                                reset(b);
                                a.setTransactionIsolation(iso); a.setAutoCommit(false);
                                // 일반 SELECT로 Snapshot 고정 → 타 세션 변경 → 색인 반영 확인
                                eq(rows(a, "SELECT id || ':' || body FROM " + T + " ORDER BY id"), List.of("1:alpha", "2:beta"));
                                for (String sql : ch.sql) exec(b, sql);
                                awaitSearch(b, ch.q, ch.want);
                                eq(rows(a, "SELECT id || ':' || body FROM " + T + " ORDER BY id"), List.of("1:alpha", "2:beta"));
                                rejected(tag + " after " + ch.name + ": " + f.getKey(), () -> f.getValue().run(a));
                                eq(error(() -> rows(a, "SELECT 1")), "25P02");
                                a.rollback();
                            }
                        }
                        // 새 트랜잭션은 최신 상태를 고정한다
                        a.setAutoCommit(false);
                        eq(rows(a, "SELECT id || ':' || body FROM " + T + " ORDER BY id"), List.of("1:alpha", "2:beta"));
                        a.rollback();
                    }
                });

                test("R03 " + tag + ": rejected also after a write and for DML with TEXT_MATCH", () -> {
                    try (Connection a = conn(1, simple); Connection b = conn(2, false)) {
                        reset(b);
                        a.setTransactionIsolation(iso); a.setAutoCommit(false);
                        exec(a, "UPDATE " + T + " SET n = n + 1 WHERE id = 1");
                        rejected(tag + " search after write", () -> rows(a, "SELECT id FROM " + T + " WHERE TEXT_MATCH(body, 'alpha')" + COMMITTED));
                        a.rollback();
                        a.setTransactionIsolation(iso);
                        rejected(tag + " UPDATE ... WHERE TEXT_MATCH", () -> exec(a, "UPDATE " + T + " SET n = 0 WHERE TEXT_MATCH(body, 'alpha')"));
                        a.rollback();
                        eq(rows(b, "SELECT n FROM " + T + " ORDER BY id"), List.of("1", "2"));
                    }
                });
            }

            test("R04 " + level + ": BEGIN ISOLATION LEVEL / SET TRANSACTION forms", () -> {
                try (Connection a = conn(0, true)) {
                    exec(a, "BEGIN ISOLATION LEVEL " + level);
                    rejected("BEGIN ISOLATION LEVEL " + level, () -> rows(a, "SELECT id FROM " + T + " WHERE TEXT_MATCH(body, 'alpha')"));
                    exec(a, "ROLLBACK");
                    exec(a, "BEGIN");
                    exec(a, "SET TRANSACTION ISOLATION LEVEL " + level);
                    rejected("SET TRANSACTION " + level, () -> rows(a, "SELECT id FROM " + T + " WHERE TEXT_MATCH(body, 'alpha')"));
                    exec(a, "ROLLBACK");
                    // 오류 후 복구: ROLLBACK TO SAVEPOINT로 트랜잭션을 계속하며 Snapshot도 유지된다
                    exec(a, "BEGIN ISOLATION LEVEL " + level);
                    exec(a, "SAVEPOINT s");
                    eq(rows(a, "SELECT count(*) FROM " + T), List.of("2"));
                    rejected("after SAVEPOINT " + level, () -> rows(a, "SELECT id FROM " + T + " WHERE TEXT_MATCH(body, 'alpha')"));
                    eq(error(() -> rows(a, "SELECT 1")), "25P02");
                    exec(a, "ROLLBACK TO SAVEPOINT s");
                    eq(rows(a, "SELECT count(*) FROM " + T), List.of("2"));
                    exec(a, "COMMIT");
                    // 세션 기본 격리 수준: 단일 문장 autocommit은 허용, 한 번에 보낸 여러 문장(암묵적 트랜잭션)은 거부
                    exec(a, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL " + level);
                    eq(rows(a, "SELECT id FROM " + T + " WHERE TEXT_MATCH(body, 'alpha')" + COMMITTED), List.of("1"));
                    rejected("implicit multi-statement " + level, () -> exec(a, "SELECT 1; SELECT id FROM " + T + " WHERE TEXT_MATCH(body, 'alpha')"));
                    exec(a, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED");
                    exec(a, "SELECT 1; SELECT id FROM " + T + " WHERE TEXT_MATCH(body, 'alpha')" + COMMITTED);
                }
            });
        }

        test("R05 search outside pinned snapshots still works (autocommit, READ COMMITTED)", () -> {
            try (Connection a = conn(1, false); Connection b = conn(2, false)) {
                reset(b);
                // autocommit (한 문장이 하나의 트랜잭션)
                eq(rows(a, "SELECT id FROM " + T + " WHERE TEXT_MATCH(body, 'alpha')" + COMMITTED), List.of("1"));
                List<String> scored = rows(a, "SELECT id, TEXT_SCORE(body) > 0 FROM " + T + " WHERE TEXT_MATCH(body, 'alpha OR beta') AND n > 1" + COMMITTED);
                eq(scored, List.of("2|t"));
                eq(prepared(a, "SELECT id FROM " + T + " WHERE TEXT_MATCH(body, ?) ORDER BY id", "alpha OR beta"), List.of("1", "2"));
                // 세션 기본 격리 수준이 REPEATABLE READ여도 autocommit 문장은 고정 Snapshot이 아니다
                exec(a, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ");
                eq(rows(a, "SELECT id FROM " + T + " WHERE TEXT_MATCH(body, 'alpha')" + COMMITTED), List.of("1"));
                exec(a, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED");
                // READ COMMITTED 트랜잭션: 문장마다 최신 Committed 상태
                a.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED); a.setAutoCommit(false);
                eq(rows(a, "SELECT id FROM " + T + " WHERE TEXT_MATCH(body, 'alpha')" + COMMITTED), List.of("1"));
                exec(b, "UPDATE " + T + " SET body = CASE WHEN id = 1 THEN 'beta' ELSE 'alpha' END");
                awaitSearch(b, "alpha", List.of("2"));
                eq(rows(a, "SELECT id || ':' || body FROM " + T + " WHERE TEXT_MATCH(body, 'alpha')" + COMMITTED), List.of("2:alpha"));
                a.commit();
            }
        });

        test("R06 plain pinned snapshot reads unaffected", () -> {
            try (Connection a = conn(1, false); Connection b = conn(2, false)) {
                reset(b);
                for (int iso : List.of(Connection.TRANSACTION_REPEATABLE_READ, Connection.TRANSACTION_SERIALIZABLE)) {
                    a.setAutoCommit(true); a.setTransactionIsolation(iso); a.setAutoCommit(false);
                    eq(rows(a, "SELECT count(*) FROM " + T), List.of("2"));
                    exec(b, "INSERT INTO " + T + " VALUES (7, 'delta', 7)");
                    exec(b, "UPDATE " + T + " SET n = 99 WHERE id = 1");
                    eq(rows(a, "SELECT count(*) FROM " + T), List.of("2"));
                    eq(rows(a, "SELECT n FROM " + T + " WHERE id = 1"), List.of("1"));
                    a.rollback();
                    exec(b, "DELETE FROM " + T + " WHERE id = 7");
                    exec(b, "UPDATE " + T + " SET n = 1 WHERE id = 1");
                }
            }
        });

        try (Connection c = conn(0, false)) {
            exec(c, "DROP TABLE IF EXISTS " + T);
            System.out.println("CLEANUP: " + T + " dropped");
        }
        System.out.printf("%nRESULT: %d passed, %d failed%n", pass, fail);
        for (String x : failures) System.out.println("  - " + x);
        System.exit(fail == 0 ? 0 : 1);
    }
}
