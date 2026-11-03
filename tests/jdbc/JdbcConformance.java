// erounDB JDBC 적합성 시험 (pgjdbc 42.7.4 고정). 설계서 14.2
//   java -cp postgresql-42.7.4.jar JdbcConformance.java "jdbc:postgresql://127.0.0.1:9786/jamsil?sslmode=require" [big]
import java.sql.*;
import java.util.*;

public class JdbcConformance {
    static int pass = 0, fail = 0;
    static List<String> failures = new ArrayList<>();

    interface Body { void run() throws Exception; }

    static void test(String name, Body b) {
        long t0 = System.nanoTime();
        try {
            b.run();
            pass++;
            System.out.printf("PASS %-55s %6d ms%n", name, (System.nanoTime() - t0) / 1_000_000);
        } catch (Throwable e) {
            fail++;
            failures.add(name + ": " + e);
            System.out.printf("FAIL %-55s %s%n", name, e);
        }
    }

    static void check(boolean c, String msg) { if (!c) throw new AssertionError(msg); }
    static void eq(Object a, Object b) { if (!Objects.equals(a, b)) throw new AssertionError("expected <" + b + "> but was <" + a + ">"); }

    static String sqlState(Body b) throws Exception {
        try { b.run(); } catch (SQLException e) { return e.getSQLState() + " " + e.getMessage(); }
        throw new AssertionError("expected SQLException");
    }

    static Connection connect(String url) throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", System.getenv().getOrDefault("JAMSIL_USER", "jamsil"));
        p.setProperty("password", System.getenv().getOrDefault("JAMSIL_PASSWORD", "jamsil"));
        return DriverManager.getConnection(url, p);
    }

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "jdbc:postgresql://127.0.0.1:9786/jamsil?sslmode=require";
        boolean big = args.length > 1 && args[1].equals("big");
        Connection c = connect(url);
        Statement s = c.createStatement();

        test("connect + server version", () -> {
            DatabaseMetaData md = c.getMetaData();
            check(md.getDatabaseProductVersion().startsWith("14"), md.getDatabaseProductVersion());
            ResultSet r = s.executeQuery("SELECT 1, 'a', true, 1.5::float8, now()");
            check(r.next(), "row");
            eq(r.getInt(1), 1);
            eq(r.getString(2), "a");
            eq(r.getBoolean(3), true);
            eq(r.getDouble(4), 1.5);
            check(r.getTimestamp(5) != null, "now()");
        });

        test("DDL create table / drop", () -> {
            s.execute("DROP TABLE IF EXISTS items");
            s.execute("DROP TABLE IF EXISTS documents");
            s.execute("CREATE TABLE documents (id BIGINT PRIMARY KEY, title VARCHAR(200), content TEXT, created TIMESTAMPTZ DEFAULT now())");
            s.execute("CREATE TABLE items (id BIGSERIAL PRIMARY KEY, sku VARCHAR(40) UNIQUE NOT NULL, qty INTEGER DEFAULT 0, price DOUBLE PRECISION, active BOOLEAN DEFAULT true, doc_id BIGINT REFERENCES documents(id))");
            s.execute("CREATE INDEX items_qty_idx ON items (qty, price)");
        });

        test("table without PK rejected (replication contract)", () -> {
            String st = sqlState(() -> s.execute("CREATE TABLE nopk (a INT)"));
            check(st.startsWith("0A000"), st);
            check(st.contains("JDB-5101"), st);
        });

        test("PreparedStatement insert/select (UTF-8 한글/emoji)", () -> {
            PreparedStatement ps = c.prepareStatement("INSERT INTO documents (id, title, content) VALUES (?, ?, ?)");
            String emoji = "가나다 👨‍👩‍👧‍👦 🇰🇷 ✔️ é";
            for (int i = 1; i <= 10; i++) {
                ps.setLong(1, i);
                ps.setString(2, "제목 " + i);
                ps.setString(3, i == 1 ? emoji : "본문 " + i);
                eq(ps.executeUpdate(), 1);
            }
            PreparedStatement q = c.prepareStatement("SELECT id, title, content FROM documents WHERE id = ?");
            q.setLong(1, 1);
            ResultSet r = q.executeQuery();
            check(r.next(), "row");
            eq(r.getString(3), emoji);
            eq(r.getString(2), "제목 1");
            // prepareThreshold(5) 이후 서버 측 prepared + binary 전송 경로
            for (int i = 0; i < 8; i++) {
                q.setLong(1, 1 + (i % 10));
                ResultSet rr = q.executeQuery();
                check(rr.next(), "row " + i);
                eq(rr.getLong(1), (long) (1 + (i % 10)));
            }
        });

        test("aggregates / join / group by / order / limit", () -> {
            s.executeUpdate("INSERT INTO items (sku, qty, price, doc_id) VALUES ('a', 5, 1.5, 1), ('b', 7, 2.0, 1), ('c', 1, 10.0, 2)");
            ResultSet r = s.executeQuery("SELECT d.id, count(*) AS n, sum(i.qty) AS q FROM documents d JOIN items i ON i.doc_id = d.id GROUP BY d.id HAVING count(*) >= 1 ORDER BY d.id LIMIT 10 OFFSET 0");
            check(r.next(), "row1");
            eq(r.getLong(1), 1L);
            eq(r.getLong(2), 2L);
            eq(r.getLong(3), 12L);
            check(r.next(), "row2");
            check(!r.next(), "only two");
            ResultSet r2 = s.executeQuery("SELECT id, sku, active FROM items WHERE sku = 'a'");
            check(r2.next(), "item");
            eq(r2.getBoolean(3), true);
            check(r2.getLong(1) > 0, "serial id");
        });

        test("UPDATE / DELETE row counts", () -> {
            eq(s.executeUpdate("UPDATE items SET qty = qty + 1 WHERE qty > 2"), 2);
            eq(s.executeUpdate("DELETE FROM items WHERE sku = 'c'"), 1);
        });

        test("constraint errors map to SQLSTATE", () -> {
            check(sqlState(() -> s.executeUpdate("INSERT INTO documents (id, title) VALUES (1, 'dup')")).startsWith("23505"), "unique");
            check(sqlState(() -> s.executeUpdate("INSERT INTO items (sku) VALUES (NULL)")).startsWith("23502"), "not null");
            check(sqlState(() -> s.executeUpdate("INSERT INTO items (sku, doc_id) VALUES ('zz', 999)")).startsWith("23503"), "fk");
            check(sqlState(() -> s.executeUpdate("INSERT INTO documents (id, title) VALUES (99, '" + "x".repeat(201) + "')")).startsWith("22001"), "varchar length");
            check(sqlState(() -> s.executeQuery("SELECT * FROM no_such_table")).startsWith("42P01"), "missing table");
            check(sqlState(() -> s.executeQuery("SELEC 1")).startsWith("42601"), "syntax");
        });

        test("explicit transaction commit / rollback", () -> {
            c.setAutoCommit(false);
            Statement t = c.createStatement();
            t.executeUpdate("INSERT INTO documents (id, title) VALUES (100, 'tx')");
            ResultSet r = t.executeQuery("SELECT count(*) FROM documents WHERE id = 100");
            r.next();
            eq(r.getLong(1), 1L); // 자기 트랜잭션의 쓰기는 보인다
            c.rollback();
            r = t.executeQuery("SELECT count(*) FROM documents WHERE id = 100");
            r.next();
            eq(r.getLong(1), 0L);
            t.executeUpdate("INSERT INTO documents (id, title) VALUES (101, 'tx2')");
            t.executeUpdate("UPDATE documents SET title = 'tx2b' WHERE id = 101");
            c.commit();
            c.setAutoCommit(true);
            r = s.executeQuery("SELECT title FROM documents WHERE id = 101");
            check(r.next(), "committed row");
            eq(r.getString(1), "tx2b");
        });

        test("error inside transaction aborts block (25P02)", () -> {
            c.setAutoCommit(false);
            Statement t = c.createStatement();
            t.executeUpdate("INSERT INTO documents (id, title) VALUES (102, 'x')");
            try { t.executeUpdate("INSERT INTO documents (id, title) VALUES (102, 'dup')"); } catch (SQLException e) { }
            String st = sqlState(() -> t.executeQuery("SELECT 1"));
            check(st.startsWith("25P02"), st);
            c.rollback();
            c.setAutoCommit(true);
            ResultSet r = s.executeQuery("SELECT count(*) FROM documents WHERE id = 102");
            r.next();
            eq(r.getLong(1), 0L);
        });

        test("JDBC batch in transaction", () -> {
            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement("INSERT INTO documents (id, title, content) VALUES (?, ?, ?)");
            for (int i = 1000; i < 1500; i++) {
                ps.setLong(1, i);
                ps.setString(2, "batch " + i);
                ps.setString(3, "본문 " + i);
                ps.addBatch();
            }
            int[] n = ps.executeBatch();
            eq(n.length, 500);
            c.commit();
            c.setAutoCommit(true);
            ResultSet r = s.executeQuery("SELECT count(*) FROM documents WHERE id >= 1000");
            r.next();
            eq(r.getLong(1), 500L);
        });

        test("TEXT LIKE rejected, VARCHAR LIKE ok", () -> {
            String st = sqlState(() -> s.executeQuery("SELECT id FROM documents WHERE content LIKE '%본문%'"));
            check(st.contains("JDB-4102"), st);
            ResultSet r = s.executeQuery("SELECT count(*) FROM documents WHERE title LIKE 'batch 10%'");
            r.next();
            eq(r.getLong(1), 100L);
            r = s.executeQuery("SELECT count(*) FROM documents WHERE title LIKE 'Batch 10%'"); // PG LIKE는 대소문자 구분
            r.next();
            eq(r.getLong(1), 0L);
            r = s.executeQuery("SELECT count(*) FROM documents WHERE title ILIKE 'Batch 100_'");
            r.next();
            eq(r.getLong(1), 10L);
        });

        test("DatabaseMetaData getTables/getColumns/getPrimaryKeys/getIndexInfo", () -> {
            DatabaseMetaData md = c.getMetaData();
            ResultSet t = md.getTables(null, "public", "%", new String[]{"TABLE"});
            Set<String> names = new HashSet<>();
            while (t.next()) names.add(t.getString("TABLE_NAME"));
            check(names.contains("documents") && names.contains("items"), "tables " + names);
            ResultSet col = md.getColumns(null, "public", "items", "%");
            Map<String, String> types = new LinkedHashMap<>();
            while (col.next()) types.put(col.getString("COLUMN_NAME"), col.getString("TYPE_NAME"));
            eq(String.join(",", types.keySet()), "id,sku,qty,price,active,doc_id");
            eq(types.get("sku"), "varchar");
            eq(types.get("qty"), "int4");
            ResultSet pk = md.getPrimaryKeys(null, "public", "documents");
            check(pk.next(), "pk");
            eq(pk.getString("COLUMN_NAME"), "id");
            ResultSet ix = md.getIndexInfo(null, "public", "items", false, false);
            Set<String> idx = new HashSet<>();
            while (ix.next()) if (ix.getString("INDEX_NAME") != null) idx.add(ix.getString("INDEX_NAME"));
            check(idx.contains("items_qty_idx"), "indexes " + idx);
            ResultSet sc = md.getSchemas();
            Set<String> schemas = new HashSet<>();
            while (sc.next()) schemas.add(sc.getString(1));
            check(schemas.contains("public"), "schemas " + schemas);
            ResultSet fk = md.getImportedKeys(null, "public", "items");
            check(fk.next(), "imported key");
            eq(fk.getString("PKTABLE_NAME"), "documents");
            eq(fk.getString("FKCOLUMN_NAME"), "doc_id");
            eq(fk.getString("PKCOLUMN_NAME"), "id");
            ResultSet ek = md.getExportedKeys(null, "public", "documents");
            check(ek.next(), "exported key");
            eq(ek.getString("FKTABLE_NAME"), "items");
            ResultSet br = md.getBestRowIdentifier(null, "public", "documents", DatabaseMetaData.bestRowSession, true);
            check(br.next(), "best row");
            eq(br.getString("COLUMN_NAME"), "id");
            ResultSet ui = md.getIndexInfo(null, "public", "items", true, false);
            Set<String> uidx = new HashSet<>();
            while (ui.next()) if (ui.getString("INDEX_NAME") != null) uidx.add(ui.getString("INDEX_NAME") + ":" + ui.getString("COLUMN_NAME"));
            check(uidx.contains("items_pkey:id"), "unique indexes " + uidx);
            check(md.getSQLKeywords() != null, "keywords");
            ResultSet tt = md.getTableTypes();
            check(tt.next(), "table types");
            ResultSet ti = md.getTypeInfo();
            check(ti.next(), "type info");
        });

        test("ResultSetMetaData types", () -> {
            ResultSet r = s.executeQuery("SELECT id, title, content, created FROM documents WHERE id = 1");
            ResultSetMetaData m = r.getMetaData();
            eq(m.getColumnTypeName(1), "int8");
            eq(m.getColumnTypeName(2), "varchar");
            eq(m.getColumnTypeName(3), "text");
            eq(m.getColumnTypeName(4), "timestamptz");
            check(r.next(), "row");
            check(r.getTimestamp(4) != null, "timestamp default");
        });

        test("jamsil_catalog system views", () -> {
            ResultSet r = s.executeQuery("SELECT node_id, role, sql_ready FROM jamsil_catalog.cluster_nodes ORDER BY node_id");
            check(r.next(), "node row");
            r = s.executeQuery("SELECT commit_index, applied_index FROM jamsil_catalog.replication_status");
            check(r.next(), "repl row");
            r = s.executeQuery("SELECT recovery_state FROM jamsil_catalog.storage_status");
            check(r.next(), "storage row");
            eq(r.getString(1), "ok");
            r = s.executeQuery("SELECT count(*) FROM jamsil_catalog.active_sessions");
            r.next();
            check(r.getLong(1) >= 1, "sessions");
        });

        test("idempotency key (jamsil.request_id)", () -> {
            // 실행마다 새 키: 이전 실행의 요청 이력과 섞이지 않는다 (RQ-15)
            String rid = "conf-" + java.util.UUID.randomUUID();
            s.execute("DELETE FROM documents WHERE id = 5000");
            s.execute("SET jamsil.request_id = '" + rid + "'");
            ResultSet r1 = s.executeQuery("INSERT INTO documents (id, title) VALUES (5000, 'once') RETURNING id, title");
            check(r1.next(), "first RETURNING row");
            eq(r1.getLong(1), 5000L);
            s.execute("SET jamsil.request_id = '" + rid + "'");
            // 재시도: 중복 실행 없이 기록된 RETURNING 결과를 그대로 돌려준다
            ResultSet r2 = s.executeQuery("INSERT INTO documents (id, title) VALUES (5000, 'once') RETURNING id, title");
            check(r2.next(), "replayed RETURNING row");
            eq(r2.getLong(1), 5000L);
            eq(r2.getString(2), "once");
            ResultSet cnt = s.executeQuery("SELECT count(*) FROM documents WHERE id = 5000");
            cnt.next();
            eq(cnt.getLong(1), 1L);
            // 같은 키를 다른 요청에 쓰면 거부
            s.execute("SET jamsil.request_id = '" + rid + "'");
            try {
                s.executeUpdate("INSERT INTO documents (id, title) VALUES (5001, 'other')");
                throw new AssertionError("key reuse for a different request must fail");
            } catch (SQLException e) {
                eq(e.getSQLState(), "22023");
            }
            ResultSet r = s.executeQuery("SELECT raft_index, tag FROM _jamsil_requests WHERE request_id = '" + rid + "'");
            check(r.next(), "request recorded");
            eq(r.getString(2), "INSERT 0 1");
        });

        test("ALTER TABLE add/rename column, view", () -> {
            s.execute("ALTER TABLE items ADD COLUMN note VARCHAR(50)");
            s.execute("ALTER TABLE items RENAME COLUMN note TO memo");
            s.execute("CREATE VIEW active_items AS SELECT id, sku FROM items WHERE active");
            ResultSet r = s.executeQuery("SELECT count(*) FROM active_items");
            r.next();
            check(r.getLong(1) >= 1, "view rows");
            s.execute("DROP VIEW active_items");
        });

        test("CancelRequest / setQueryTimeout / statement_timeout", () -> {
            Statement st = c.createStatement();
            new Thread(() -> { try { Thread.sleep(300); st.cancel(); } catch (Exception e) { } }).start();
            long t0 = System.nanoTime();
            String a = sqlState(() -> st.executeQuery("SELECT pg_sleep(5)"));
            check(a.startsWith("57014"), a);
            check((System.nanoTime() - t0) / 1_000_000 < 3000, "cancel took too long");
            Statement st2 = c.createStatement();
            st2.setQueryTimeout(1);
            String b = sqlState(() -> st2.executeQuery("SELECT pg_sleep(4)"));
            check(b.startsWith("57014"), b);
            s.execute("SET statement_timeout = 500");
            String d = sqlState(() -> s.executeQuery("SELECT pg_sleep(3)"));
            check(d.startsWith("57014"), d);
            s.execute("SET statement_timeout = 0");
            ResultSet r = s.executeQuery("SELECT 42");  // 세션은 계속 사용 가능
            r.next();
            eq(r.getInt(1), 42);
        });

        test("TEXT_MATCH: Korean Boolean/phrase/BM25 + committed consistency", () -> {
            s.execute("DROP TABLE IF EXISTS news");
            s.execute("CREATE TABLE news (id BIGINT PRIMARY KEY, title VARCHAR(200), body TEXT, status VARCHAR(10))");
            String noIdx = sqlState(() -> s.executeQuery("SELECT id FROM news WHERE TEXT_MATCH(body, '금융')"));
            check(noIdx.contains("JDB-4101"), noIdx);
            s.execute("CREATE SEARCH INDEX idx_news_body ON news(body) WITH (analyzer = 'korean', scoring = 'bm25')");
            PreparedStatement ps = c.prepareStatement("INSERT INTO news (id, title, body, status) VALUES (?, ?, ?, ?)");
            String[] bodies = {
                "금융 시장에서 주식 투자가 늘었다",
                "채권 금리가 하락하고 금융 투자 심리가 회복되었다",
                "인공지능 기술이 산업을 바꾼다",
                "금융 투자 금융 투자 금융 투자",
                "날씨가 맑고 따뜻하다"
            };
            for (int i = 0; i < bodies.length; i++) {
                ps.setLong(1, i + 1); ps.setString(2, "t" + (i + 1)); ps.setString(3, bodies[i]); ps.setString(4, i == 1 ? "hidden" : "open");
                ps.executeUpdate();
            }
            // committed: 방금 COMMIT한 문서가 반드시 보인다
            ResultSet r = s.executeQuery("SELECT id FROM news WHERE TEXT_MATCH(body, '금융 AND (주식 OR 채권)') ORDER BY id WITH SEARCH CONSISTENCY COMMITTED");
            List<Long> ids = new ArrayList<>();
            while (r.next()) ids.add(r.getLong(1));
            eq(ids, List.of(1L, 2L));
            r = s.executeQuery("SELECT id FROM news WHERE TEXT_MATCH(body, '\"금융 투자\"') ORDER BY id WITH SEARCH CONSISTENCY COMMITTED");
            ids.clear(); while (r.next()) ids.add(r.getLong(1));
            eq(ids, List.of(2L, 4L));
            // BM25 순위 + 추가 SQL 조건 + 파라미터 바인딩
            s.execute("SET jamsil.search_consistency = 'committed'");
            PreparedStatement q = c.prepareStatement("SELECT id, title, TEXT_SCORE(body) AS score FROM news WHERE TEXT_MATCH(body, ?) AND status = 'open' ORDER BY TEXT_SCORE(body) DESC LIMIT 20");
            q.setString(1, "투자");
            r = q.executeQuery();
            check(r.next(), "hit");
            eq(r.getLong(1), 4L);
            check(r.getDouble(3) > 0, "score");
            check(r.next(), "second hit");
            eq(r.getLong(1), 1L);  // id 2는 status 조건으로 제외
            check(!r.next(), "only two");
            // UPDATE/DELETE 반영
            s.executeUpdate("UPDATE news SET body = '완전히 다른 내용' WHERE id = 1");
            s.executeUpdate("DELETE FROM news WHERE id = 3");
            r = s.executeQuery("SELECT count(*) FROM news WHERE TEXT_MATCH(body, '주식 OR 인공지능')");
            r.next();
            eq(r.getLong(1), 0L);
            r = s.executeQuery("SELECT id FROM news WHERE TEXT_MATCH(body, '다른')");
            check(r.next() && r.getLong(1) == 1L, "updated doc searchable");
            r = s.executeQuery("SELECT index_id, state, docs FROM jamsil_catalog.search_indexes WHERE index_id = 'idx_news_body'");
            check(r.next(), "search index status");
            eq(r.getLong(3), 4L);
            s.execute("SET jamsil.search_consistency = 'eventual'");
        });

        if (big) {
            test("32MiB TEXT exact boundary via JDBC bind", () -> {
                s.execute("DROP TABLE IF EXISTS bigtext");
                s.execute("CREATE TABLE bigtext (id INT PRIMARY KEY, body TEXT)");
                // 한글(3B) + 조합 이모지(18B) 혼합 단위를 반복하고 남는 바이트는 ASCII로 채워 정확히 33,554,432 bytes
                String unit = "가나다라마바사아자차카타파하👨‍👩‍👧";
                int unitBytes = unit.getBytes("UTF-8").length;
                int reps = (33_554_432 - 100) / unitBytes;
                StringBuilder sb = new StringBuilder(33_554_432);
                for (int i = 0; i < reps; i++) sb.append(unit);
                int rest = 33_554_432 - reps * unitBytes;
                for (int i = 0; i < rest; i++) sb.append('a');
                String exact = sb.toString();
                eq(exact.getBytes("UTF-8").length, 33_554_432);
                PreparedStatement ps = c.prepareStatement("INSERT INTO bigtext (id, body) VALUES (?, ?)");
                ps.setInt(1, 1);
                ps.setString(2, exact);
                eq(ps.executeUpdate(), 1);
                ResultSet r = s.executeQuery("SELECT octet_length(body), body FROM bigtext WHERE id = 1");
                check(r.next(), "row");
                eq(r.getLong(1), 33_554_432L);
                eq(r.getString(2), exact);
                ps.setInt(1, 2);
                ps.setString(2, exact + "b");
                String st = sqlState(() -> ps.executeUpdate());
                check(st.contains("JDB-3101"), st);
                ResultSet r2 = s.executeQuery("SELECT count(*) FROM bigtext");
                r2.next();
                eq(r2.getLong(1), 1L);
                // 32MiB UPDATE
                PreparedStatement up = c.prepareStatement("UPDATE bigtext SET body = ? WHERE id = 1");
                up.setString(1, exact.replace('a', 'b'));
                eq(up.executeUpdate(), 1);
            });
        }

        s.close();
        c.close();
        System.out.printf("%nRESULT: %d passed, %d failed%n", pass, fail);
        for (String f : failures) System.out.println("  - " + f);
        System.exit(fail == 0 ? 0 : 1);
    }
}
