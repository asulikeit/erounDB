// erounDB 정확한 장애 지점 시험 (설계서 14.1 T-06/T-08/T-09/T-10/T-11/T-12/T-15/T-16).
// 실제 VM 3대에서 SSH + systemctl + 시험 전용 장애 표식(JAMSIL_FAULT_INJECTION=1)을 사용한다.
//   set -a; . ~/.jamsil_test; set +a
//   scripts/vm-fault-injection.sh enable
//   java -cp tests/jdbc/postgresql-42.7.4.jar tests/vm/FaultPointsVm.java   (JAMSIL_ONLY=T-08 로 일부만)
//   scripts/vm-fault-injection.sh disable
// 장애 표식 (crates/jamsil-common/src/fault.rs):
//   raft/crash_before_raft_fsync, raft/fail_raft_fsync, raft/crash_after_commit_ack, raft/crash_mid_large_append,
//   rdb/crash_after_apply_commit, rdb/sqlite_full
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class FaultPointsVm {
    static final String[] HOSTS = System.getenv().getOrDefault("JAMSIL_HOSTS", "192.168.56.100,192.168.56.101,192.168.56.102").split(",");
    static final String SSH_USER = System.getenv().getOrDefault("JAMSIL_SSH_USER", "daniel");
    static final String DATA = "/var/lib/jamsildb";
    static final Map<Integer, String> NODE_HOST = new TreeMap<>();
    static final String ONLY = System.getenv("JAMSIL_ONLY");
    static int pass = 0, fail = 0;
    static final List<String> failures = new ArrayList<>();

    interface Body { void run() throws Exception; }

    static void test(String name, Body b) {
        if (ONLY != null && Arrays.stream(ONLY.split(",")).noneMatch(name::contains)) return;
        long t0 = System.nanoTime();
        System.out.println("---- " + name);
        try {
            b.run();
            pass++;
            System.out.printf("PASS %-66s %7d ms%n", name, (System.nanoTime() - t0) / 1_000_000);
        } catch (Throwable e) {
            fail++;
            failures.add(name + ": " + e);
            System.out.printf("FAIL %-66s %s%n", name, e);
            e.printStackTrace(System.out);
        } finally {
            try { cleanupMarkers(); healAll(); } catch (Exception x) { System.out.println("  heal failed: " + x); }
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
        return DriverManager.getConnection("jdbc:postgresql://" + host + ":9786/jamsil?sslmode=require&connectTimeout=5&socketTimeout=120", creds());
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

    static void marker(String host, String rel) throws Exception {
        ssh(host, "sudo install -o jamsildb -g jamsildb -m 0600 /dev/null " + DATA + "/" + rel);
    }
    static boolean markerPresent(String host, String rel) throws Exception {
        return ssh(host, "sudo test -e " + DATA + "/" + rel + " && echo present || echo gone").equals("present");
    }
    static void cleanupMarkers() throws Exception {
        for (String h : HOSTS)
            ssh(h, "sudo rm -f " + DATA + "/raft/crash_before_raft_fsync " + DATA + "/raft/fail_raft_fsync " + DATA + "/raft/crash_after_commit_ack "
                + DATA + "/raft/crash_mid_large_append " + DATA + "/rdb/crash_after_apply_commit " + DATA + "/rdb/sqlite_full " + DATA + "/rdb/crash_before_apply_commit");
    }
    static void killNode(String host) throws Exception { ssh(host, "sudo systemctl kill -s KILL jamsildb; sudo systemctl stop jamsildb"); }
    static void startNode(String host) throws Exception { ssh(host, "sudo systemctl start jamsildb --no-block"); }
    static void healAll() throws Exception { for (String h : HOSTS) ssh(h, "sudo systemctl start jamsildb --no-block"); }

    static String journalCount(String host, String since, String pattern) throws Exception {
        return ssh(host, "sudo journalctl -u jamsildb --since '" + since + "' --no-pager -o cat | grep -c -E '" + pattern + "' || true");
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

    static String leader() throws Exception {
        for (int attempt = 0; attempt < 200; attempt++) {
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
    static List<String> followers(String leader) {
        List<String> f = new ArrayList<>();
        for (String h : HOSTS) if (!h.equals(leader)) f.add(h);
        return f;
    }

    static long count(String host, String where) throws Exception {
        try (Connection c = conn(host); Statement s = c.createStatement()) {
            ResultSet r = s.executeQuery("SELECT count(*) FROM fault " + where);
            r.next();
            return r.getLong(1);
        }
    }

    static Map<String, String> checksums() throws Exception {
        Map<String, String> m = new TreeMap<>();
        for (String h : HOSTS) {
            try (Connection c = conn(h); Statement s = c.createStatement()) {
                s.executeQuery("SELECT count(*) FROM fault").close();
                ResultSet r = s.executeQuery("SELECT applied_index, checksum FROM jamsil_catalog.replica_checksum");
                r.next();
                m.put(h, r.getLong(1) + "/" + r.getString(2));
            }
        }
        return m;
    }

    static void waitConverged() throws Exception {
        Map<String, String> last = null;
        // 따라잡지 못한 노드의 조회는 최대 ~30초 대기하므로 반복 횟수가 아니라 경과 시간으로 제한한다.
        long deadline = System.nanoTime() + 300_000_000_000L;
        while (System.nanoTime() < deadline) {
            try {
                last = checksums();
                if (new HashSet<>(last.values()).size() == 1) { System.out.println("  converged: " + last.values().iterator().next()); return; }
            } catch (SQLException e) { last = Map.of("error", String.valueOf(e.getMessage())); }
            Thread.sleep(500);
        }
        throw new AssertionError("replicas did not converge: " + last);
    }

    static String insert(String host, long id, String v) {
        try (Connection c = conn(host); Statement s = c.createStatement()) {
            s.setQueryTimeout(90);
            s.executeUpdate("INSERT INTO fault (id, v) VALUES (" + id + ", '" + v + "')");
            return "success";
        } catch (SQLException e) {
            return e.getSQLState() + " " + String.valueOf(e.getMessage()).split("\n")[0];
        }
    }

    /** 오프라인(노드 중지 상태) SQLite 파일 직접 검사: applied_index, 해당 id 행 존재 여부. */
    static String offlineState(String host, long id) throws Exception {
        String py = "import sqlite3\n"
            + "c=sqlite3.connect('file:" + DATA + "/rdb/main.sqlite?mode=ro', uri=True)\n"
            + "a=c.execute('SELECT applied_index FROM _jamsil_state').fetchone()[0]\n"
            + "n=c.execute('SELECT count(*) FROM fault WHERE id=" + id + "').fetchone()[0]\n"
            + "q=c.execute('PRAGMA quick_check').fetchone()[0]\n"
            + "print(a, n, q)\n";
        return ssh(host, "sudo python3 -", py);
    }

    static String sha256(String host, String sql) throws Exception {
        try (Connection c = conn(host); Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
            if (!r.next()) return "no-row";
            byte[] b = r.getString(1).getBytes(StandardCharsets.UTF_8);
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(b));
        }
    }

    public static void main(String[] args) throws Exception {
        discover();
        System.out.println("nodes=" + NODE_HOST + " leader=" + leader());
        try (Connection c = conn(leader()); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS fault");
            s.execute("CREATE TABLE fault (id BIGINT PRIMARY KEY, v TEXT)");
        }
        for (String h : HOSTS) count(h, "");

        test("T-06 leader crash after Raft append, before log fsync: no success ACK", () -> {
            String l = leader();
            marker(l, "raft/crash_before_raft_fsync");
            String out = insert(l, 6000001, "t06");
            System.out.println("  client outcome: " + out);
            check(!out.equals("success"), "write acknowledged although the leader crashed before its log fsync");
            check(!markerPresent(l, "raft/crash_before_raft_fsync"), "fault point not reached");
            check(!journalCount(l, "-3min", "jamsil-sync exited|jamsil-sync.*(signal|abort)").equals("0"), "jamsil-sync crash not observed");
            String nl = leader();
            System.out.println("  leader after crash: " + nl + "; row present=" + count(nl, "WHERE id = 6000001") + " (either is valid: client saw no success)");
            waitConverged();
        });

        test("T-08 store crash right after SQLite COMMIT + torn WAL tail: row/applied_index atomic", () -> {
            String l = leader();
            String f = followers(l).get(0);
            marker(f, "rdb/crash_after_apply_commit");
            String dbBefore = ssh(f, "sudo stat -c %y " + DATA + "/rdb/main.sqlite");
            String out = insert(l, 8000001, "t08");
            System.out.println("  client outcome (via leader): " + out);
            check(out.equals("success"), "quorum write failed: " + out);
            for (int i = 0; i < 40 && markerPresent(f, "rdb/crash_after_apply_commit"); i++) Thread.sleep(250);
            check(!markerPresent(f, "rdb/crash_after_apply_commit"), "fault point not reached on follower");
            killNode(f);
            String committed = offlineState(f, 8000001);
            System.out.println("  follower after crash (applied, row, quick_check): " + committed);
            // WAL 끝 512바이트 절단 = COMMIT 기록 도중 Crash로 마지막 프레임이 찢어진 상태.
            // 단, 그 사이 Checkpoint가 DB 파일에 반영했다면 실제 Crash로는 생길 수 없는 상태이므로 절단하지 않는다.
            boolean checkpointed = !dbBefore.equals(ssh(f, "sudo stat -c %y " + DATA + "/rdb/main.sqlite"));
            System.out.println("  checkpoint after commit: " + checkpointed);
            if (!checkpointed)
                ssh(f, "sudo sh -c 'w=" + DATA + "/rdb/main.sqlite-wal; s=$(stat -c %s $w); [ $s -gt 4096 ] && truncate -s $((s-512)) $w; stat -c %s $w'");
            String torn = offlineState(f, 8000001);
            System.out.println("  follower after torn WAL tail (applied, row, quick_check): " + torn);
            String[] t = torn.split(" ");
            long applied = Long.parseLong(t[0]);
            long row = Long.parseLong(t[1]);
            check(t[2].equals("ok"), "SQLite corruption: " + torn);
            long entryIdx = Long.parseLong(committed.split(" ")[0]);
            check((row == 1 && applied >= entryIdx) || (row == 0 && applied < entryIdx), "row and applied_index disagree: " + torn + " entry=" + entryIdx);
            startNode(f);
            waitConverged();
            check(count(f, "WHERE id = 8000001") == 1, "row missing after recovery");
        });

        test("T-09 leader crash right after ACK: next leader has the commit", () -> {
            String l = leader();
            marker(l, "raft/crash_after_commit_ack");
            String out = insert(l, 9000001, "t09");
            System.out.println("  client outcome: " + out);
            check(out.equals("success"), "write not acknowledged: " + out);
            Thread.sleep(1500);
            check(!markerPresent(l, "raft/crash_after_commit_ack"), "fault point not reached");
            check(!journalCount(l, "-3min", "jamsil-sync exited|jamsil-sync.*(signal|abort)").equals("0"), "jamsil-sync crash not observed");
            String nl = leader();
            System.out.println("  leader after crash: " + nl);
            for (String h : HOSTS) {
                long n = -1;
                for (int i = 0; i < 60; i++) { try { n = count(h, "WHERE id = 9000001"); if (n == 1) break; } catch (SQLException e) { } Thread.sleep(500); }
                check(n == 1, "acknowledged row missing on " + h);
            }
            waitConverged();
        });

        test("T-10 canceled/timed-out writes and leader loss: retry with request_id is exactly-once", () -> {
            // (a) statement_timeout 으로 쓰기 도중 취소 → 결과 불명 → 같은 키로 재시도
            String l = leader();
            String big = "x".repeat(4 << 20);
            String key = "t10-" + UUID.randomUUID();
            String first;
            try (Connection c = conn(followers(l).get(0)); Statement s = c.createStatement()) {
                s.execute("SET statement_timeout = 5");
                s.execute("SET jamsil.request_id = '" + key + "'");
                s.executeUpdate("INSERT INTO fault (id, v) VALUES (10000001, '" + big + "')");
                first = "success";
            } catch (SQLException e) { first = e.getSQLState(); }
            System.out.println("  first attempt: " + first);
            try (Connection c = conn(HOSTS[2]); Statement s = c.createStatement()) {
                s.execute("SET jamsil.request_id = '" + key + "'");
                s.executeUpdate("INSERT INTO fault (id, v) VALUES (10000001, '" + big + "')");
            }
            check(count(leader(), "WHERE id = 10000001") == 1, "retry duplicated or lost the write");
            // (b) 쓰기 부하 중 Leader SIGKILL → 실패한 요청을 같은 키로 다른 엔드포인트에 재시도
            int n = 200;
            ExecutorService pool = Executors.newFixedThreadPool(8);
            AtomicInteger retried = new AtomicInteger();
            List<Future<Long>> fs = new ArrayList<>();
            String rid = UUID.randomUUID().toString();
            for (int i = 0; i < n; i++) {
                long id = 10100000L + i;
                fs.add(pool.submit(() -> {
                    String k = "t10b-" + rid + "-" + id;
                    for (int attempt = 0; attempt < 60; attempt++) {
                        try (Connection c = conn(HOSTS[(int) ((id + attempt) % HOSTS.length)]); Statement s = c.createStatement()) {
                            s.setQueryTimeout(60);
                            s.execute("SET jamsil.request_id = '" + k + "'");
                            try (ResultSet r = s.executeQuery("INSERT INTO fault (id, v) VALUES (" + id + ", 'b') RETURNING id")) { r.next(); return r.getLong(1); }
                        } catch (SQLException e) {
                            retried.incrementAndGet();
                            Thread.sleep(500);
                        }
                    }
                    return -1L;
                }));
                if (i == n / 3) { Thread.sleep(200); killNode(leader()); }
            }
            Set<Long> got = new TreeSet<>();
            for (Future<Long> f : fs) got.add(f.get(10, TimeUnit.MINUTES));
            pool.shutdown();
            healAll();
            System.out.println("  retries=" + retried.get() + " distinct returned ids=" + got.size());
            check(!got.contains(-1L), "some requests never succeeded");
            check(got.size() == n, "RETURNING mismatch");
            leader();
            waitConverged();
            check(count(leader(), "WHERE id >= 10100000 AND id < 10200000") == n, "exactly-once violated");
        });

        test("T-11 leader crash after sending part of a 32MiB AppendEntries", () -> {
            String l = leader();
            marker(l, "raft/crash_mid_large_append");
            String body = "가".repeat(11_184_810) + "y";
            String out;
            try (Connection c = conn(l); PreparedStatement ps = c.prepareStatement("INSERT INTO fault (id, v) VALUES (11000001, ?)")) {
                ps.setQueryTimeout(120);
                ps.setString(1, body);
                ps.executeUpdate();
                out = "success";
            } catch (SQLException e) { out = e.getSQLState() + " " + String.valueOf(e.getMessage()).split("\n")[0]; }
            System.out.println("  client outcome: " + out);
            check(!out.equals("success"), "write acknowledged although the leader died mid-replication");
            check(!markerPresent(l, "raft/crash_mid_large_append"), "fault point not reached");
            leader();
            waitConverged();
            String want = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body.getBytes(StandardCharsets.UTF_8)));
            for (String h : HOSTS) {
                long n = count(h, "WHERE id = 11000001");
                String sum = n == 1 ? sha256(h, "SELECT v FROM fault WHERE id = 11000001") : "absent";
                System.out.println("  " + h + ": " + sum);
                check(n == 0 || sum.equals(want), "partial 32MiB value on " + h);
            }
        });

        test("T-12 32MiB committed, one node lost (data wiped), rebuilt from quorum", () -> {
            String l = leader();
            String body = "나".repeat(11_184_810) + "z";
            try (Connection c = conn(l); PreparedStatement ps = c.prepareStatement("INSERT INTO fault (id, v) VALUES (12000001, ?)")) {
                ps.setString(1, body);
                ps.executeUpdate();
            }
            String want = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body.getBytes(StandardCharsets.UTF_8)));
            String f = followers(l).get(1);
            killNode(f);
            ssh(f, "sudo rm -rf " + DATA + "/rdb " + DATA + "/raft " + DATA + "/search");
            startNode(f);
            leader();
            waitConverged();
            check(sha256(f, "SELECT v FROM fault WHERE id = 12000001").equals(want), "32MiB value not rebuilt on wiped node");
            System.out.println("  rebuilt node " + f + " SHA-256 matches");
        });

        test("T-15 fsync failure / disk full: no durable ACK, no corruption", () -> {
            String l = leader();
            List<String> fl = followers(l);
            // (a) Follower 한 대의 Raft fsync 실패: 과반수(Leader+다른 Follower)로 계속 성공
            marker(fl.get(0), "raft/fail_raft_fsync");
            check(insert(l, 15000001, "a").equals("success"), "quorum write failed with one bad follower");
            ssh(fl.get(0), "sudo rm -f " + DATA + "/raft/fail_raft_fsync");
            // (b) Leader Raft fsync 실패: 성공 응답 없음
            l = leader();
            marker(l, "raft/fail_raft_fsync");
            String out = insert(l, 15000002, "b");
            System.out.println("  leader fsync failure outcome: " + out);
            check(!out.equals("success"), "acknowledged without a durable leader log");
            ssh(l, "sudo rm -f " + DATA + "/raft/fail_raft_fsync");
            l = leader();
            // (c) Leader SQLite 디스크 Full: 쓰기 실패(53100), 성공 응답 없음
            marker(l, "rdb/sqlite_full");
            String bigv = "f".repeat(2 << 20);
            out = insert(l, 15000003, bigv);
            System.out.println("  leader disk-full outcome: " + out);
            check(!out.equals("success"), "acknowledged on a full disk");
            ssh(l, "sudo rm -f " + DATA + "/rdb/sqlite_full");
            // (d) Follower SQLite 디스크 Full: 해당 노드는 격리, 나머지는 계속, 해소 후 재기동으로 복구
            l = leader();
            String f = followers(l).get(0);
            marker(f, "rdb/sqlite_full");
            check(insert(l, 15000004, bigv).equals("success"), "quorum write failed while one follower disk is full");
            Thread.sleep(2000);
            String st;
            try (Connection c = conn(f); Statement s = c.createStatement()) {
                s.executeQuery("SELECT count(*) FROM fault").close();
                st = "readable";
            } catch (SQLException e) { st = e.getSQLState() + " " + String.valueOf(e.getMessage()).split("\n")[0]; }
            System.out.println("  full follower read: " + st);
            ssh(f, "sudo rm -f " + DATA + "/rdb/sqlite_full");
            ssh(f, "sudo systemctl restart jamsildb");
            leader();
            waitConverged();
            for (String h : HOSTS) {
                String q = ssh(h, "sudo python3 -c \"import sqlite3; print(sqlite3.connect('file:" + DATA + "/rdb/main.sqlite?mode=ro', uri=True).execute('PRAGMA quick_check').fetchone()[0])\"");
                check(q.equals("ok"), "quick_check on " + h + ": " + q);
            }
            check(count(leader(), "WHERE id IN (15000001, 15000004)") == 2, "acknowledged writes lost");
        });

        test("T-16 schema version conflict: node is isolated, not silently diverged", () -> {
            String l = leader();
            String f = followers(l).get(0);
            killNode(f);
            ssh(f, "sudo python3 -", "import sqlite3\nc=sqlite3.connect('" + DATA + "/rdb/main.sqlite')\nc.execute('UPDATE _jamsil_state SET schema_version = schema_version + 7')\nc.commit()\n");
            startNode(f);
            Thread.sleep(8000);
            check(insert(l, 16000001, "after-conflict").equals("success"), "cluster stopped accepting writes");
            String st = "readable";
            for (int i = 0; i < 40; i++) {
                try (Connection c = conn(f); Statement s = c.createStatement()) {
                    s.executeQuery("SELECT count(*) FROM fault").close();
                    st = "readable";
                } catch (SQLException e) { st = e.getSQLState() + " " + String.valueOf(e.getMessage()).split("\n")[0]; break; }
                Thread.sleep(500);
            }
            System.out.println("  conflicting node read: " + st);
            check(!st.equals("readable"), "node with conflicting schema version kept serving");
            String why = ssh(f, "sudo journalctl -u jamsildb --since '-3min' --no-pager -o cat | grep -m1 -i 'schema version mismatch' | cut -c1-200");
            System.out.println("  " + why);
            check(why.contains("schema version mismatch"), "mismatch not reported");
            // 복구: 노드 데이터 재구성 (정상 절차: 해당 노드 rdb 삭제 후 재기동 → 과반수에서 재구성)
            killNode(f);
            ssh(f, "sudo rm -rf " + DATA + "/rdb " + DATA + "/raft " + DATA + "/search");
            startNode(f);
            leader();
            waitConverged();
        });

        System.out.printf("%nRESULT: %d passed, %d failed%n", pass, fail);
        for (String x : failures) System.out.println("  - " + x);
        System.exit(fail == 0 ? 0 : 1);
    }
}
