// 압축된 Raft 로그 상태의 Follower 가 실제 네트워크로 Snapshot 을 받아 따라잡는지 확인 (VM 3대).
// 전제: [cluster] snapshot_threshold_entries=200, snapshot_keep_entries=20 으로 설정된 클러스터.
import java.sql.*;
import java.util.*;

public class SnapshotCatchupVm {
    static final String[] HOSTS = System.getenv().getOrDefault("JAMSIL_HOSTS", "192.168.56.100,192.168.56.101,192.168.56.102").split(",");
    static final String SSH_USER = System.getenv().getOrDefault("JAMSIL_SSH_USER", "daniel");
    static Properties creds() {
        Properties p = new Properties();
        p.setProperty("user", System.getenv().getOrDefault("JAMSIL_USER", "jamsil"));
        p.setProperty("password", System.getenv().getOrDefault("JAMSIL_PASSWORD", "jamsil"));
        return p;
    }
    static Connection conn(String h) throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://" + h + ":9786/jamsil?sslmode=require&connectTimeout=5&socketTimeout=120", creds());
    }
    static String ssh(String h, String cmd) throws Exception {
        Process p = new ProcessBuilder("ssh", "-o", "BatchMode=yes", SSH_USER + "@" + h, cmd).redirectErrorStream(true).start();
        String o = new String(p.getInputStream().readAllBytes()); p.waitFor(); return o.trim();
    }
    static String leader() throws Exception {
        for (String h : HOSTS) {
            try (Connection c = conn(h); Statement s = c.createStatement()) {
                ResultSet r = s.executeQuery("SELECT a.address FROM jamsil_catalog.cluster_nodes a WHERE a.node_id = (SELECT leader_id FROM jamsil_catalog.cluster_nodes WHERE address LIKE '" + h + ":%')");
                if (r.next()) return r.getString(1).split(":")[0];
            } catch (SQLException e) { }
        }
        throw new AssertionError("no leader");
    }
    static String checksum(String h) throws Exception {
        try (Connection c = conn(h); Statement s = c.createStatement()) {
            s.executeQuery("SELECT count(*) FROM snap").close();
            ResultSet r = s.executeQuery("SELECT applied_index, checksum FROM jamsil_catalog.replica_checksum");
            r.next(); return r.getLong(1) + "/" + r.getString(2);
        }
    }
    public static void main(String[] a) throws Exception {
        String l = leader();
        String f = null;
        for (String h : HOSTS) if (!h.equals(l)) { f = h; break; }
        System.out.println("leader=" + l + " follower(to be stopped)=" + f);
        try (Connection c = conn(l); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS snap");
            s.execute("CREATE TABLE snap (id BIGINT PRIMARY KEY, body TEXT)");
            s.execute("CREATE SEARCH INDEX idx_snap ON snap(body) WITH (analyzer = 'korean')");
        }
        ssh(f, "sudo systemctl kill -s KILL jamsildb; sudo systemctl stop jamsildb");
        System.out.println("follower stopped; writing 600 autocommit rows");
        try (Connection c = conn(l); PreparedStatement ps = c.prepareStatement("INSERT INTO snap VALUES (?, ?)")) {
            for (int i = 0; i < 600; i++) { ps.setLong(1, i); ps.setString(2, "스냅샷 문서 " + i + " 금융"); ps.executeUpdate(); }
        }
        try (Connection c = conn(l); Statement s = c.createStatement()) {
            ResultSet r = s.executeQuery("SELECT snapshot_index, commit_index FROM jamsil_catalog.replication_status LIMIT 1");
            r.next();
            System.out.println("leader snapshot_index=" + r.getLong(1) + " commit_index=" + r.getLong(2));
            if (r.getLong(1) == 0) throw new AssertionError("leader did not take a snapshot (threshold config?)");
        }
        long t0 = System.nanoTime();
        ssh(f, "sudo systemctl start jamsildb --no-block");
        for (int i = 0; i < 240; i++) {
            Thread.sleep(500);
            try {
                String a1 = checksum(l), a2 = checksum(f);
                if (a1.equals(a2)) {
                    System.out.printf("converged in %d ms: %s%n", (System.nanoTime() - t0) / 1_000_000, a1);
                    String log = ssh(f, "sudo journalctl -u jamsildb --since '-5min' --no-pager -o cat | grep -E 'received snapshot|installed snapshot' | cut -c1-200");
                    System.out.println(log);
                    if (!log.contains("installed snapshot")) throw new AssertionError("caught up without snapshot install");
                    try (Connection c = conn(f); Statement s = c.createStatement()) {
                        ResultSet r = s.executeQuery("SELECT count(*) FROM snap WHERE TEXT_MATCH(body, '금융') WITH SEARCH CONSISTENCY COMMITTED");
                        r.next();
                        if (r.getLong(1) != 600) throw new AssertionError("search on follower after snapshot: " + r.getLong(1));
                    }
                    System.out.println("RESULT: snapshot catch-up over the network + search rebuild OK");
                    return;
                }
            } catch (SQLException e) { }
        }
        throw new AssertionError("follower did not converge");
    }
}
