// Raft Log 부족(압축) 상태의 Follower가 Snapshot 설치 후 Catch-up 하는지 확인 (설계서 6.7, T-14 일부)
//   JAMSIL_SNAPSHOT_THRESHOLD=200 JAMSIL_SNAPSHOT_KEEP=20 scripts/dev.sh cluster
import java.nio.file.*;
import java.sql.*;
import java.util.*;

public class SnapshotCatchup {
    static String root;
    static Connection conn(int n) throws SQLException {
        Properties p = new Properties(); p.setProperty("user", System.getenv().getOrDefault("JAMSIL_USER", "jamsil")); p.setProperty("password", System.getenv().getOrDefault("JAMSIL_PASSWORD", "jamsil"));
        return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:" + (n * 10000 + 9786) + "/jamsil?sslmode=require", p);
    }
    static String sh(String c) throws Exception {
        Process p = new ProcessBuilder("bash", "-c", c).redirectErrorStream(true).start();
        String o = new String(p.getInputStream().readAllBytes()); p.waitFor(); return o.trim();
    }
    static int leader() throws Exception {
        for (int a = 0; a < 100; a++) {
            for (int n = 1; n <= 3; n++) {
                String s = sh(root + "/target/release/jamsild status --config " + root + "/dev-data/node" + n + "/node.toml | grep raft");
                if (s.contains("role=leader") && s.contains("ready=true")) return n;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("no leader");
    }
    static String checksum(int n) throws Exception {
        try (Connection c = conn(n); Statement s = c.createStatement()) {
            s.executeQuery("SELECT count(*) FROM snap").close();
            ResultSet r = s.executeQuery("SELECT applied_index, checksum FROM jamsil_catalog.replica_checksum");
            r.next(); return r.getLong(1) + "/" + r.getString(2);
        }
    }
    public static void main(String[] a) throws Exception {
        root = a[0];
        int l = leader();
        int f = l % 3 + 1;
        try (Connection c = conn(l); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS snap");
            s.execute("CREATE TABLE snap (id BIGINT PRIMARY KEY, body TEXT)");
            s.execute("CREATE SEARCH INDEX idx_snap ON snap(body) WITH (analyzer = 'korean')");
        }
        sh("kill -9 $(cat " + root + "/dev-data/node" + f + "/jamsild.pid)");
        Thread.sleep(1000);
        System.out.println("killed follower " + f + "; writing 600 autocommit rows");
        try (Connection c = conn(l); PreparedStatement ps = c.prepareStatement("INSERT INTO snap VALUES (?, ?)")) {
            for (int i = 0; i < 600; i++) { ps.setLong(1, i); ps.setString(2, "스냅샷 문서 " + i + " 금융"); ps.executeUpdate(); }
        }
        String st = sh(root + "/target/release/jamsild status --config " + root + "/dev-data/node" + l + "/node.toml | grep raft");
        System.out.println("leader: " + st);
        if (!st.matches(".*snapshot=[1-9].*")) throw new AssertionError("leader did not snapshot");
        sh(root + "/scripts/dev.sh start node" + f);
        for (int i = 0; i < 120; i++) {
            Thread.sleep(500);
            try {
                String a1 = checksum(l), a2 = checksum(f);
                if (a1.equals(a2)) {
                    System.out.println("converged: " + a1);
                    String log = sh("grep -h 'snapshot' " + root + "/dev-data/node" + f + "/log/cluster.log " + root + "/dev-data/node" + f + "/log/system.log | tail -3");
                    System.out.println(log);
                    if (!log.contains("installed snapshot")) throw new AssertionError("follower caught up without snapshot install?");
                    // 재색인: Snapshot 설치 후 follower 검색 인덱스도 RDB에서 재생성돼야 한다
                    try (Connection c = conn(f); Statement s = c.createStatement()) {
                        ResultSet r = s.executeQuery("SELECT count(*) FROM snap WHERE TEXT_MATCH(body, '금융') WITH SEARCH CONSISTENCY COMMITTED");
                        r.next();
                        if (r.getLong(1) != 600) throw new AssertionError("search on follower after snapshot: " + r.getLong(1));
                    }
                    System.out.println("RESULT: snapshot catch-up + search rebuild OK");
                    return;
                }
            } catch (SQLException e) { }
        }
        throw new AssertionError("follower did not converge");
    }
}
