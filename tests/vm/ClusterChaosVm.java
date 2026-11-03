// erounDB 3노드 VM 장애 주입 시험 (설계서 14.1). 실제 VM 3대, SSH + systemctl + nft 사용.
//   set -a; . ~/.jamsil_test; set +a
//   scripts/vm-fault-injection.sh enable
//   java -cp tests/jdbc/postgresql-42.7.4.jar tests/vm/ClusterChaosVm.java
// 환경변수: JAMSIL_HOSTS(콤마), JAMSIL_USER, JAMSIL_PASSWORD, JAMSIL_SSH_USER
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class ClusterChaosVm {
    static final String[] HOSTS = System.getenv().getOrDefault("JAMSIL_HOSTS", "192.168.56.100,192.168.56.101,192.168.56.102").split(",");
    static final String SSH_USER = System.getenv().getOrDefault("JAMSIL_SSH_USER", "daniel");
    static final Map<Integer, String> NODE_HOST = new TreeMap<>(); // node_id -> host
    static int pass = 0, fail = 0;
    static List<String> failures = new ArrayList<>();

    interface Body { void run() throws Exception; }

    static final String ONLY = System.getenv("JAMSIL_ONLY"); // 예: T-04 (이름에 포함된 시험만 실행)

    static void test(String name, Body b) {
        if (ONLY != null && !name.contains(ONLY)) return;
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
            try { healAll(); } catch (Exception x) { }
        }
    }

    static void check(boolean c, String m) { if (!c) throw new AssertionError(m); }

    static Properties creds() {
        Properties p = new Properties();
        p.setProperty("user", System.getenv().getOrDefault("JAMSIL_USER", "jamsil"));
        p.setProperty("password", System.getenv().getOrDefault("JAMSIL_PASSWORD", "jamsil"));
        return p;
    }

    static Connection conn(String host) throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://" + host + ":9786/jamsil?sslmode=require&connectTimeout=5&socketTimeout=60", creds());
    }

    static Connection connMulti() throws SQLException {
        StringBuilder h = new StringBuilder();
        for (String x : HOSTS) h.append(h.length() > 0 ? "," : "").append(x).append(":9786");
        return DriverManager.getConnection("jdbc:postgresql://" + h + "/jamsil?sslmode=require&loadBalanceHosts=true&connectTimeout=3&socketTimeout=60", creds());
    }

    static String ssh(String host, String cmd, String stdin) throws Exception {
        Process p = new ProcessBuilder("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", SSH_USER + "@" + host, cmd).redirectErrorStream(true).start();
        if (stdin != null) p.getOutputStream().write(stdin.getBytes(StandardCharsets.UTF_8));
        p.getOutputStream().close();
        String out = new String(p.getInputStream().readAllBytes());
        p.waitFor();
        return out.trim();
    }

    static String ssh(String host, String cmd) throws Exception { return ssh(host, cmd, null); }

    /** SIGKILL 후 자동 재시작을 막기 위해 stop 한다 (장애 = 노드가 내려가 있음). */
    static void killNode(String host) throws Exception {
        ssh(host, "sudo systemctl kill -s KILL jamsildb; sudo systemctl stop jamsildb");
    }

    static void startNode(String host) throws Exception { ssh(host, "sudo systemctl start jamsildb --no-block"); }

    static void healAll() throws Exception {
        for (String h : HOSTS) {
            ssh(h, "sudo nft delete table inet jtest 2>/dev/null; sudo systemctl start jamsildb --no-block");
        }
    }

    /** host가 peers 와의 9787 통신(양방향)을 차단한다. */
    static void partition(String host, String... peers) throws Exception {
        ssh(host, "sudo nft delete table inet jtest 2>/dev/null; true");
        if (peers.length == 0) return;
        String set = String.join(", ", peers);
        String rules = "table inet jtest {\n"
            + " chain in { type filter hook input priority -10; policy accept;\n"
            + "   ip saddr { " + set + " } tcp dport 9787 drop\n   ip saddr { " + set + " } tcp sport 9787 drop\n }\n"
            + " chain out { type filter hook output priority -10; policy accept;\n"
            + "   ip daddr { " + set + " } tcp dport 9787 drop\n   ip daddr { " + set + " } tcp sport 9787 drop\n }\n}\n";
        String r = ssh(host, "sudo nft -f -", rules);
        check(r.isEmpty(), "nft failed: " + r);
    }

    static void discover() throws Exception {
        for (String h : HOSTS) {
            try (Connection c = conn(h); Statement s = c.createStatement()) {
                ResultSet r = s.executeQuery("SELECT node_id, address FROM jamsil_catalog.cluster_nodes");
                while (r.next()) NODE_HOST.put(r.getInt(1), r.getString(2).split(":")[0]);
                if (NODE_HOST.size() == 3) return;
            } catch (SQLException e) { }
        }
        throw new AssertionError("cannot discover cluster nodes: " + NODE_HOST);
    }

    /** 응답하는 노드들이 일치해서 보고하는 Leader 의 호스트. */
    static String leader() throws Exception {
        for (int attempt = 0; attempt < 150; attempt++) {
            Map<String, Integer> votes = new HashMap<>();
            for (String h : HOSTS) {
                try (Connection c = conn(h); Statement s = c.createStatement()) {
                    ResultSet r = s.executeQuery("SELECT leader_id FROM jamsil_catalog.cluster_nodes WHERE address LIKE '" + h + ":%'");
                    if (r.next()) {
                        int l = r.getInt(1);
                        if (!r.wasNull() && NODE_HOST.containsKey(l)) votes.merge(NODE_HOST.get(l), 1, Integer::sum);
                    }
                } catch (SQLException e) { }
            }
            for (var e : votes.entrySet()) if (e.getValue() >= 2) return e.getKey();
            Thread.sleep(300);
        }
        throw new AssertionError("no leader");
    }

    static String follower(String leader) {
        for (String h : HOSTS) if (!h.equals(leader)) return h;
        throw new IllegalStateException();
    }

    static long count(String host, String where) throws Exception {
        try (Connection c = conn(host); Statement s = c.createStatement()) {
            ResultSet r = s.executeQuery("SELECT count(*) FROM chaos " + where);
            r.next();
            return r.getLong(1);
        }
    }

    static Map<String, String> checksums() throws Exception {
        Map<String, String> m = new TreeMap<>();
        for (String h : HOSTS) {
            try (Connection c = conn(h); Statement s = c.createStatement()) {
                s.executeQuery("SELECT count(*) FROM chaos").close(); // linearizable read → applied_index 확보
                ResultSet r = s.executeQuery("SELECT applied_index, checksum FROM jamsil_catalog.replica_checksum");
                r.next();
                m.put(h, r.getLong(1) + "/" + r.getString(2));
            }
        }
        return m;
    }

    static void waitConverged() throws Exception {
        Map<String, String> last = null;
        for (int i = 0; i < 120; i++) {
            try {
                last = checksums();
                if (new HashSet<>(last.values()).size() == 1) return;
            } catch (SQLException e) { last = Map.of("error", e.getMessage()); }
            Thread.sleep(500);
        }
        throw new AssertionError("replicas did not converge: " + last);
    }

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
        try (Connection c = conn(leader()); Statement s = c.createStatement()) {
            ResultSet r = s.executeQuery("SELECT id FROM chaos");
            while (r.next()) present.add(r.getLong(1));
        }
        List<Long> lost = new ArrayList<>();
        for (Long id : w.acked) if (!present.contains(id)) lost.add(id);
        check(lost.isEmpty(), "LOST acknowledged writes: " + lost.subList(0, Math.min(10, lost.size())) + " (" + lost.size() + ")");
        System.out.printf("     acked=%d unknown=%d errors=%d present=%d%n", w.acked.size(), w.unknown.size(), w.errors.get(), present.size());
    }

    public static void main(String[] args) throws Exception {
        discover();
        System.out.println("nodes: " + NODE_HOST + "  leader: " + leader());
        healAll();

        test("T-00 cluster up, leader elected, chaos table", () -> {
            try (Connection c = conn(HOSTS[0]); Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS chaos");
                s.execute("CREATE TABLE chaos (id BIGINT PRIMARY KEY, v VARCHAR(100), body TEXT)");
            }
        });

        test("T-01 writes via all 3 endpoints, identical replicas", () -> {
            int n = 0;
            for (String h : HOSTS) {
                n++;
                try (Connection c = conn(h)) {
                    c.setAutoCommit(false);
                    PreparedStatement ps = c.prepareStatement("INSERT INTO chaos (id, v) VALUES (?, ?)");
                    for (int i = 0; i < 2000; i++) {
                        ps.setLong(1, n * 100_000L + i);
                        ps.setString(2, "node " + h);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                    c.commit();
                }
            }
            for (String h : HOSTS) check(count(h, "") == 6000, "count on " + h);
            waitConverged();
        });

        test("T-06b linearizable read on follower sees latest write", () -> {
            String l = leader(), f = follower(l);
            try (Connection cl = conn(l); Connection cf = conn(f); Statement sl = cl.createStatement(); Statement sf = cf.createStatement()) {
                for (int i = 0; i < 100; i++) {
                    sl.executeUpdate("INSERT INTO chaos (id, v) VALUES (" + (900_000 + i) + ", 'lin')");
                    ResultSet r = sf.executeQuery("SELECT count(*) FROM chaos WHERE id = " + (900_000 + i));
                    r.next();
                    check(r.getLong(1) == 1, "follower missed write " + i);
                }
            }
        });

        test("T-02 follower SIGKILL: writes continue, catch-up after restart", () -> {
            String f = follower(leader());
            Writer w = new Writer(1_000_000);
            Thread t = new Thread(w);
            t.start();
            Thread.sleep(2000);
            killNode(f);
            long before = w.acked.size();
            Thread.sleep(6000);
            check(w.acked.size() > before + 10, "writes stalled after follower kill: " + before + " -> " + w.acked.size());
            startNode(f);
            Thread.sleep(8000);
            w.stop.set(true);
            t.join();
            verifyAcked(w);
            waitConverged();
        });

        test("T-03 leader SIGKILL: re-election, no acknowledged write lost", () -> {
            String l = leader();
            Writer w = new Writer(2_000_000);
            Thread t = new Thread(w);
            t.start();
            Thread.sleep(2000);
            long t0 = System.nanoTime();
            killNode(l);
            String nl;
            long failoverMs;
            long mid = w.acked.size();
            while (true) {
                Thread.sleep(200);
                if (w.acked.size() > mid + 3) { failoverMs = (System.nanoTime() - t0) / 1_000_000; break; }
                if ((System.nanoTime() - t0) / 1_000_000 > 60_000) throw new AssertionError("no writes 60s after leader kill");
            }
            nl = leader();
            check(!nl.equals(l), "leader did not change");
            System.out.println("     first acknowledged write after leader kill: " + failoverMs + " ms (incl. ssh/stop overhead)");
            startNode(l);
            Thread.sleep(8000);
            w.stop.set(true);
            t.join();
            verifyAcked(w);
            waitConverged();
        });

        test("T-04 1:2 partition: minority rejects writes and linearizable reads", () -> {
            String minority = leader();
            List<String> others = new ArrayList<>();
            for (String h : HOSTS) if (!h.equals(minority)) others.add(h);
            partition(minority, others.get(0), others.get(1));
            partition(others.get(0), minority);
            partition(others.get(1), minority);
            try {
                Thread.sleep(6000);
                try (Connection c = conn(others.get(0)); Statement s = c.createStatement()) {
                    s.executeUpdate("INSERT INTO chaos (id, v) VALUES (3000001, 'majority')");
                }
                boolean writeFailed = false, readFailed = false, localOk = false;
                try (Connection c = conn(minority); Statement s = c.createStatement()) {
                    s.setQueryTimeout(30);
                    try { s.executeUpdate("INSERT INTO chaos (id, v) VALUES (3000002, 'minority')"); } catch (SQLException e) { writeFailed = true; System.out.println("     minority write: " + e.getSQLState() + " " + e.getMessage().split("\n")[0]); }
                    try { s.executeQuery("SELECT count(*) FROM chaos"); } catch (SQLException e) { readFailed = true; }
                    s.execute("SET jamsil.read_consistency = 'local'");
                    ResultSet r = s.executeQuery("SELECT count(*) FROM chaos");
                    localOk = r.next();
                }
                check(writeFailed, "minority accepted a write");
                check(readFailed, "minority served a linearizable read");
                check(localOk, "local read on minority should be allowed");
            } finally {
                for (String h : HOSTS) partition(h);
            }
            Thread.sleep(6000);
            String l = leader();
            check(count(l, "WHERE id = 3000002") == 0, "minority write leaked");
            check(count(l, "WHERE id = 3000001") == 1, "majority write lost");
            waitConverged();
        });

        test("T-05 two nodes down: no successful write", () -> {
            String l = leader();
            List<String> others = new ArrayList<>();
            for (String h : HOSTS) if (!h.equals(l)) others.add(h);
            killNode(others.get(0));
            killNode(others.get(1));
            Thread.sleep(5000);
            boolean failed = false;
            try (Connection c = conn(l); Statement s = c.createStatement()) {
                s.setQueryTimeout(40);
                s.executeUpdate("INSERT INTO chaos (id, v) VALUES (4000001, 'noquorum')");
            } catch (SQLException e) {
                failed = true;
                System.out.println("     rejected: " + e.getSQLState() + " " + e.getMessage().split("\n")[0]);
            }
            check(failed, "write succeeded without quorum");
            startNode(others.get(0));
            startNode(others.get(1));
            Thread.sleep(10000);
            leader();
            check(count(leader(), "WHERE id = 4000001") == 0, "no-quorum write appeared after recovery");
            waitConverged();
        });

        test("T-07 leader store crash after Raft commit, before SQLite COMMIT", () -> {
            String l = leader();
            ssh(l, "sudo install -o jamsildb -g jamsildb -m 0600 /dev/null /var/lib/jamsildb/rdb/crash_before_apply_commit");
            String outcome;
            try (Connection c = conn(l); Statement s = c.createStatement()) {
                s.setQueryTimeout(60);
                s.executeUpdate("INSERT INTO chaos (id, v) VALUES (7000001, 'crash-window')");
                outcome = "success";
            } catch (SQLException e) {
                outcome = e.getSQLState() + " " + e.getMessage().split("\n")[0];
            }
            System.out.println("     client outcome: " + outcome);
            for (int i = 0; i < 90; i++) {
                Thread.sleep(500);
                try { if (count(leader(), "WHERE id = 7000001") == 1) break; } catch (SQLException e) { }
            }
            check(count(leader(), "WHERE id = 7000001") == 1, "committed entry was not re-applied");
            check(ssh(l, "test -e /var/lib/jamsildb/rdb/crash_before_apply_commit && echo present || echo consumed").equals("consumed"), "crash point was not hit");
            String ev = ssh(l, "sudo journalctl -u jamsildb --since '-3min' --no-pager | grep -c 'jamsil-store exited'");
            System.out.println("     store restarts logged on " + l + ": " + ev);
            check(!ev.equals("0"), "store was not restarted after the crash");
            waitConverged();
        });

        test("T-09 full cluster restart: acknowledged data survives", () -> {
            long before = count(leader(), "");
            for (String h : HOSTS) killNode(h);
            for (String h : HOSTS) startNode(h);
            Thread.sleep(8000);
            leader();
            check(count(leader(), "") == before, "row count changed across restart");
            waitConverged();
        });

        for (String h : HOSTS) partition(h);
        System.out.printf("%nRESULT: %d passed, %d failed%n", pass, fail);
        for (String f : failures) System.out.println("  - " + f);
        System.exit(fail == 0 ? 0 : 1);
    }
}
