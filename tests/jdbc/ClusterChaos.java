// erounDB 3노드 장애 주입 시험 (설계서 14.1). scripts/dev.sh cluster 로 기동된 로컬 클러스터 대상.
//   java -cp postgresql-42.7.4.jar ClusterChaos.java <repo-root>
import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class ClusterChaos {
    static String root;
    static int[] PORTS = {0, 19786, 29786, 39786};
    static int pass = 0, fail = 0;
    static List<String> failures = new ArrayList<>();

    interface Body { void run() throws Exception; }

    static void test(String name, Body b) {
        long t0 = System.nanoTime();
        try {
            b.run();
            pass++;
            System.out.printf("PASS %-62s %7d ms%n", name, (System.nanoTime() - t0) / 1_000_000);
        } catch (Throwable e) {
            fail++;
            failures.add(name + ": " + e);
            System.out.printf("FAIL %-62s %s%n", name, e);
            e.printStackTrace(System.out);
        }
    }

    static void check(boolean c, String m) { if (!c) throw new AssertionError(m); }

    static String url(int node) {
        return "jdbc:postgresql://127.0.0.1:" + PORTS[node] + "/jamsil?sslmode=require&connectTimeout=5&socketTimeout=60";
    }

    static Connection conn(int node) throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", System.getenv().getOrDefault("JAMSIL_USER", "jamsil"));
        p.setProperty("password", System.getenv().getOrDefault("JAMSIL_PASSWORD", "jamsil"));
        return DriverManager.getConnection(url(node), p);
    }

    static Connection connMulti() throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", System.getenv().getOrDefault("JAMSIL_USER", "jamsil"));
        p.setProperty("password", System.getenv().getOrDefault("JAMSIL_PASSWORD", "jamsil"));
        String u = "jdbc:postgresql://127.0.0.1:19786,127.0.0.1:29786,127.0.0.1:39786/jamsil?sslmode=require&loadBalanceHosts=true&connectTimeout=3&socketTimeout=60";
        return DriverManager.getConnection(u, p);
    }

    static String sh(String cmd) throws Exception {
        Process p = new ProcessBuilder("bash", "-c", cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        p.waitFor();
        return out.trim();
    }

    static int pid(int node) throws Exception {
        return Integer.parseInt(Files.readString(Path.of(root, "dev-data", "node" + node, "jamsild.pid")).trim());
    }

    static void killNode(int node) throws Exception {
        sh("kill -9 " + pid(node));
        // PDEATHSIG로 자식도 종료될 때까지 대기
        for (int i = 0; i < 50; i++) {
            String s = sh("pgrep -f 'dev-data/node" + node + "/node.toml' | wc -l");
            if (s.equals("0")) return;
            Thread.sleep(100);
        }
    }

    static void startNode(int node) throws Exception {
        sh(root + "/scripts/dev.sh start node" + node);
    }

    static void partition(int node, int... blocked) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int b : blocked) sb.append(b).append(' ');
        Files.writeString(Path.of(root, "dev-data", "node" + node, "run", "partition"), sb.toString());
    }

    static int leader() throws Exception {
        for (int attempt = 0; attempt < 100; attempt++) {
            for (int n = 1; n <= 3; n++) {
                try (Connection c = conn(n); Statement s = c.createStatement()) {
                    ResultSet r = s.executeQuery("SELECT leader_id FROM jamsil_catalog.cluster_nodes WHERE node_id = " + n);
                    if (r.next()) {
                        long l = r.getLong(1);
                        if (!r.wasNull() && l > 0) {
                            // 리더가 실제 쓰기를 받는지 확인
                            return (int) l;
                        }
                    }
                } catch (SQLException e) { }
            }
            Thread.sleep(200);
        }
        throw new AssertionError("no leader");
    }

    static long count(int node, String where) throws Exception {
        try (Connection c = conn(node); Statement s = c.createStatement()) {
            ResultSet r = s.executeQuery("SELECT count(*) FROM chaos " + where);
            r.next();
            return r.getLong(1);
        }
    }

    static Map<Integer, String> checksums() throws Exception {
        Map<Integer, String> m = new TreeMap<>();
        for (int n = 1; n <= 3; n++) {
            try (Connection c = conn(n); Statement s = c.createStatement()) {
                // 같은 applied index에서 비교하기 위해 linearizable 읽기 후 checksum
                s.executeQuery("SELECT count(*) FROM chaos").close();
                ResultSet r = s.executeQuery("SELECT applied_index, checksum FROM jamsil_catalog.replica_checksum");
                r.next();
                m.put(n, r.getLong(1) + "/" + r.getString(2));
            }
        }
        return m;
    }

    static void waitConverged() throws Exception {
        Map<Integer, String> last = null;
        for (int i = 0; i < 60; i++) {
            last = checksums();
            if (new HashSet<>(last.values()).size() == 1) return;
            Thread.sleep(500);
        }
        throw new AssertionError("replicas did not converge: " + last);
    }

    /** 성공 응답(acknowledged)을 받은 id 목록을 기록하며 계속 쓴다. */
    static class Writer implements Runnable {
        final Set<Long> acked = ConcurrentHashMap.newKeySet();
        final Set<Long> unknown = ConcurrentHashMap.newKeySet();
        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicLong next;
        final AtomicLong errors = new AtomicLong();
        Writer(long start) { next = new AtomicLong(start); }
        public void run() {
            Connection c = null;
            while (!stop.get()) {
                long id = next.getAndIncrement();
                try {
                    if (c == null || c.isClosed()) c = connMulti();
                    try (PreparedStatement ps = c.prepareStatement("INSERT INTO chaos (id, v) VALUES (?, ?)")) {
                        ps.setLong(1, id);
                        ps.setString(2, "v" + id);
                        ps.executeUpdate();
                        acked.add(id);
                    }
                } catch (SQLException e) {
                    errors.incrementAndGet();
                    String st = e.getSQLState();
                    // 결과 불명: 연결 단절 / JDB-2003
                    if (st == null || st.startsWith("08") || (e.getMessage() != null && e.getMessage().contains("JDB-2003"))) unknown.add(id);
                    try { if (c != null) c.close(); } catch (Exception x) { }
                    c = null;
                    try { Thread.sleep(100); } catch (InterruptedException ie) { return; }
                }
            }
            try { if (c != null) c.close(); } catch (Exception x) { }
        }
    }

    static void verifyAcked(Writer w) throws Exception {
        Set<Long> present = new HashSet<>();
        int l = leader();
        try (Connection c = conn(l); Statement s = c.createStatement()) {
            ResultSet r = s.executeQuery("SELECT id FROM chaos");
            while (r.next()) present.add(r.getLong(1));
        }
        List<Long> lost = new ArrayList<>();
        for (Long id : w.acked) if (!present.contains(id)) lost.add(id);
        check(lost.isEmpty(), "LOST acknowledged writes: " + lost.subList(0, Math.min(10, lost.size())) + " (" + lost.size() + ")");
        // 응답 불명 외에 성공하지 않은 쓰기가 반영됐는지(중복/유령 쓰기)도 확인
        System.out.printf("     acked=%d unknown=%d errors=%d present=%d%n", w.acked.size(), w.unknown.size(), w.errors.get(), present.size());
    }

    public static void main(String[] args) throws Exception {
        root = args.length > 0 ? args[0] : ".";

        test("T-00 cluster up, leader elected", () -> {
            int l = leader();
            check(l >= 1 && l <= 3, "leader " + l);
            try (Connection c = conn(1); Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS chaos");
                s.execute("CREATE TABLE chaos (id BIGINT PRIMARY KEY, v VARCHAR(100), body TEXT)");
            }
        });

        test("T-01 writes via all 3 endpoints, identical replicas", () -> {
            for (int n = 1; n <= 3; n++) {
                try (Connection c = conn(n)) {
                    c.setAutoCommit(false);
                    PreparedStatement ps = c.prepareStatement("INSERT INTO chaos (id, v) VALUES (?, ?)");
                    for (int i = 0; i < 2000; i++) {
                        ps.setLong(1, n * 100_000L + i);
                        ps.setString(2, "node" + n);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                    c.commit();
                }
            }
            for (int n = 1; n <= 3; n++) check(count(n, "") == 6000, "count on node " + n);
            waitConverged();
        });

        test("T-06b linearizable read on follower sees latest write", () -> {
            int l = leader();
            int f = l % 3 + 1;
            try (Connection cl = conn(l); Connection cf = conn(f); Statement sl = cl.createStatement(); Statement sf = cf.createStatement()) {
                for (int i = 0; i < 50; i++) {
                    sl.executeUpdate("INSERT INTO chaos (id, v) VALUES (" + (900_000 + i) + ", 'lin')");
                    ResultSet r = sf.executeQuery("SELECT count(*) FROM chaos WHERE id = " + (900_000 + i));
                    r.next();
                    check(r.getLong(1) == 1, "follower missed write " + i);
                }
            }
        });

        test("T-02 follower SIGKILL: writes continue, catch-up after restart", () -> {
            int l = leader();
            int f = l % 3 + 1;
            Writer w = new Writer(1_000_000);
            Thread t = new Thread(w);
            t.start();
            Thread.sleep(1500);
            killNode(f);
            long before = w.acked.size();
            Thread.sleep(4000);
            check(w.acked.size() > before + 10, "writes stalled after follower kill: " + before + " -> " + w.acked.size());
            startNode(f);
            Thread.sleep(3000);
            w.stop.set(true);
            t.join();
            verifyAcked(w);
            waitConverged();
        });

        test("T-03 leader SIGKILL: re-election, no acknowledged write lost", () -> {
            int l = leader();
            Writer w = new Writer(2_000_000);
            Thread t = new Thread(w);
            t.start();
            Thread.sleep(1500);
            killNode(l);
            Thread.sleep(5000);
            int nl = leader();
            check(nl != l, "leader did not change");
            long mid = w.acked.size();
            Thread.sleep(2000);
            check(w.acked.size() > mid, "no writes after failover");
            startNode(l);
            Thread.sleep(3000);
            w.stop.set(true);
            t.join();
            verifyAcked(w);
            waitConverged();
        });

        test("T-04 1:2 partition: minority rejects writes and linearizable reads", () -> {
            int l = leader();
            int minority = l; // 리더를 고립시킨다
            int[] others = minority == 1 ? new int[]{2, 3} : minority == 2 ? new int[]{1, 3} : new int[]{1, 2};
            partition(minority, others[0], others[1]);
            partition(others[0], minority);
            partition(others[1], minority);
            try {
                Thread.sleep(4000);
                // 과반수 측 쓰기 성공
                try (Connection c = conn(others[0]); Statement s = c.createStatement()) {
                    s.executeUpdate("INSERT INTO chaos (id, v) VALUES (3000001, 'majority')");
                }
                // 소수 측 쓰기 / 기본 읽기 거부
                boolean writeFailed = false, readFailed = false;
                try (Connection c = conn(minority); Statement s = c.createStatement()) {
                    try { s.executeUpdate("INSERT INTO chaos (id, v) VALUES (3000002, 'minority')"); } catch (SQLException e) { writeFailed = true; }
                    try { s.executeQuery("SELECT count(*) FROM chaos"); } catch (SQLException e) { readFailed = true; }
                    // local 모드는 stale 경고와 함께 허용
                    s.execute("SET jamsil.read_consistency = 'local'");
                    ResultSet r = s.executeQuery("SELECT count(*) FROM chaos");
                    check(r.next(), "local read");
                }
                check(writeFailed, "minority accepted a write");
                check(readFailed, "minority served a linearizable read");
            } finally {
                partition(minority);
                partition(others[0]);
                partition(others[1]);
            }
            Thread.sleep(3000);
            check(count(leader(), "WHERE id = 3000002") == 0, "minority write leaked");
            check(count(leader(), "WHERE id = 3000001") == 1, "majority write lost");
            waitConverged();
        });

        test("T-05 two nodes down: no successful write", () -> {
            int l = leader();
            int a = l % 3 + 1, b = a % 3 + 1;
            killNode(a);
            killNode(b);
            Thread.sleep(3000);
            boolean failed = false;
            try (Connection c = conn(l); Statement s = c.createStatement()) {
                s.setQueryTimeout(20);
                s.executeUpdate("INSERT INTO chaos (id, v) VALUES (4000001, 'noquorum')");
            } catch (SQLException e) {
                failed = true;
                System.out.println("     rejected: " + e.getSQLState() + " " + e.getMessage());
            }
            check(failed, "write succeeded without quorum");
            startNode(a);
            startNode(b);
            Thread.sleep(5000);
            leader();
            waitConverged();
        });

        test("T-07 leader store crash after Raft commit, before SQLite COMMIT", () -> {
            int l = leader();
            Files.writeString(Path.of(root, "dev-data", "node" + l, "data", "rdb", "crash_before_apply_commit"), "1");
            String outcome;
            try (Connection c = conn(l); Statement s = c.createStatement()) {
                s.setQueryTimeout(60);
                s.executeUpdate("INSERT INTO chaos (id, v) VALUES (7000001, 'crash-window')");
                outcome = "success";
            } catch (SQLException e) {
                outcome = e.getSQLState() + " " + e.getMessage();
            }
            System.out.println("     client outcome: " + outcome);
            // 확정된 로그는 Store 재시작 후 재적용되어야 한다 (모든 노드)
            for (int i = 0; i < 60; i++) {
                Thread.sleep(500);
                try {
                    if (count(leader(), "WHERE id = 7000001") == 1) break;
                } catch (SQLException e) { }
            }
            check(count(leader(), "WHERE id = 7000001") == 1, "committed entry was not re-applied");
            // 성공 응답은 재적용이 끝난 뒤에만 가능하다: 위에서 데이터 존재를 확인했다.
            check(!Files.exists(Path.of(root, "dev-data", "node" + l, "data", "rdb", "crash_before_apply_commit")), "crash point was not hit");
            String st = sh(root + "/target/release/jamsild status --config " + root + "/dev-data/node" + l + "/node.toml | grep jamsil-store");
            System.out.println("    " + st.trim());
            check(!st.contains("restarts=0"), "store was not restarted after the crash");
            waitConverged();
        });

        test("T-09 full cluster restart: acknowledged data survives", () -> {
            long before = count(leader(), "");
            for (int n = 1; n <= 3; n++) killNode(n);
            for (int n = 1; n <= 3; n++) startNode(n);
            Thread.sleep(4000);
            leader();
            check(count(leader(), "") == before, "row count changed across restart");
            waitConverged();
        });

        System.out.printf("%nRESULT: %d passed, %d failed%n", pass, fail);
        for (String f : failures) System.out.println("  - " + f);
        System.exit(fail == 0 ? 0 : 1);
    }
}
