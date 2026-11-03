// T-13 검색 프로세스 중단·재시작, T-14 검색 인덱스 삭제 후 RDB에서 재생성 (standalone)
import java.sql.*;
import java.util.*;

public class SearchRecovery {
    static String root;
    static Connection conn() throws SQLException {
        Properties p = new Properties(); p.setProperty("user", System.getenv().getOrDefault("JAMSIL_USER", "jamsil")); p.setProperty("password", System.getenv().getOrDefault("JAMSIL_PASSWORD", "jamsil"));
        return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:9786/jamsil?sslmode=require", p);
    }
    static String sh(String c) throws Exception {
        Process p = new ProcessBuilder("bash", "-c", c).redirectErrorStream(true).start();
        String o = new String(p.getInputStream().readAllBytes()); p.waitFor(); return o.trim();
    }
    static long searchCount(String q) throws Exception {
        for (int i = 0; i < 120; i++) {
            try (Connection c = conn(); Statement s = c.createStatement()) {
                ResultSet r = s.executeQuery("SELECT count(*) FROM docs WHERE TEXT_MATCH(body, '" + q + "') WITH SEARCH CONSISTENCY COMMITTED");
                r.next();
                return r.getLong(1);
            } catch (SQLException e) {
                Thread.sleep(500); // 검색 프로세스 재시작 중
            }
        }
        throw new AssertionError("search unavailable");
    }
    public static void main(String[] a) throws Exception {
        root = a[0];
        String pidOf = "pgrep -f 'jamsil-search --config " + root + "/dev-data/standalone/node.toml'";
        try (Connection c = conn(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS docs");
            s.execute("CREATE TABLE docs (id BIGINT PRIMARY KEY, title VARCHAR(100), body TEXT)");
            s.execute("CREATE SEARCH INDEX idx_docs ON docs(body, title) WITH (analyzer = 'korean')");
            c.setAutoCommit(false);
            PreparedStatement ps = c.prepareStatement("INSERT INTO docs VALUES (?, ?, ?)");
            for (int i = 0; i < 100; i++) { ps.setLong(1, i); ps.setString(2, "제목" + i); ps.setString(3, "한국어 형태소 분석 검색 " + i); ps.addBatch(); }
            ps.executeBatch(); c.commit();
        }
        if (searchCount("형태소") != 100) throw new AssertionError("initial");
        System.out.println("PASS initial index: 100 docs");

        // T-13: 검색 프로세스 강제 종료 중에도 SQL 쓰기는 계속, 재시작 후 Outbox 재처리
        sh("kill -9 $(" + pidOf + ")");
        try (Connection c = conn(); PreparedStatement ps = c.prepareStatement("INSERT INTO docs VALUES (?, ?, ?)")) {
            for (int i = 100; i < 150; i++) { ps.setLong(1, i); ps.setString(2, "제목" + i); ps.setString(3, "한국어 형태소 분석 검색 " + i); ps.executeUpdate(); }
        }
        long n = searchCount("형태소");
        if (n != 150) throw new AssertionError("after search restart: " + n);
        System.out.println("PASS T-13 search SIGKILL + restart: outbox replayed, 150 docs");

        // T-14: 인덱스 파일 삭제 → 재색인
        sh("kill -9 $(" + pidOf + "); rm -rf " + root + "/dev-data/standalone/data/search/indexes " + root + "/dev-data/standalone/data/search/checkpoints");
        n = searchCount("형태소");
        if (n != 150) throw new AssertionError("after index deletion: " + n);
        System.out.println("PASS T-14 index directory deleted: rebuilt from RDB, 150 docs");
        // 다중 컬럼 검색 (title)
        if (searchCount("title:제목7") < 1) throw new AssertionError("title field");
        System.out.println("PASS multi-field search (title:)");
    }
}
