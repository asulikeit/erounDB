// 다중 호스트 URL 장애 전환 시험 (VM 3대, ssh + systemctl). 샘플 클라이언트(examples/jdbc/JamsilClient)를 그대로 시험한다.
//   JAMSIL_USER/JAMSIL_PASSWORD : superuser 계정,  JAMSIL_SSH_USER : VM ssh 계정(기본 daniel, 비밀번호 없는 sudo 필요)
//   tests/run-multihost-failover.sh 10.0.0.1,10.0.0.2,10.0.0.3
// 시험 후 모든 노드는 기동 상태로 원복되고, 시험 테이블 qa_mh_failover 는 삭제된다.
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class MultiHostFailover {
    static String[] HOSTS;                      // ip 목록(순서 = 노드 1,2,3)
    static String HOSTS_CSV;
    static final String SSH_USER = System.getenv().getOrDefault("JAMSIL_SSH_USER", "daniel");
    static final String T = "qa_mh_failover";
    static int pass, fail;
    interface Body { void run() throws Exception; }

    static void test(String name, Body b) {
        long t = System.nanoTime();
        try { b.run(); pass++; System.out.printf("PASS %s (%d ms)%n", name, (System.nanoTime() - t) / 1_000_000); }
        catch (Throwable e) { fail++; System.out.println("FAIL " + name + " | " + e); }
    }
    static void check(boolean c, String m) { if (!c) throw new AssertionError(m); }

    // ---- VM 제어 ----
    static String ssh(String host, String cmd) throws Exception {
        Process p = new ProcessBuilder("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", SSH_USER + "@" + host, cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        p.waitFor(60, TimeUnit.SECONDS);
        return out;
    }
    static void stopNode(String h) throws Exception { ssh(h, "sudo systemctl stop jamsildb"); System.out.println("  [stop] " + h); }
    static void startNode(String h) throws Exception { ssh(h, "sudo systemctl start jamsildb --no-block"); System.out.println("  [start] " + h); }
    static String status(String h) throws Exception { return ssh(h, "sudo jamsild status --config /etc/jamsildb/jamsildb.toml 2>&1 | grep '^raft:'"); }
    static String leader() throws Exception {
        for (String h : HOSTS) { String s = status(h); if (s.contains("role=leader") && s.contains("quorum=true")) return h; }
        return null;
    }
    /** 모든 노드가 기동하고 Raft가 따라잡을 때까지 대기. */
    static void awaitHealthy(int sec) throws Exception { awaitHealthy(sec, true); }
    /** @param sameCommit 쓰기가 계속되는 동안에는 노드별 commit 이 항상 조금씩 달라 false 로 호출한다. */
    static void awaitHealthy(int sec, boolean sameCommit) throws Exception {
        long end = System.currentTimeMillis() + sec * 1000L;
        while (System.currentTimeMillis() < end) {
            boolean ok = true; Set<String> commits = new HashSet<>();
            for (String h : HOSTS) {
                String s = status(h);
                if (!s.contains("quorum=true")) { ok = false; break; }
                int i = s.indexOf("commit="); commits.add(s.substring(i, s.indexOf(' ', i)));
            }
            if (ok && (!sameCommit || commits.size() == 1) && leader() != null) return;
            Thread.sleep(1000);
        }
        throw new AssertionError("cluster did not become healthy in " + sec + "s");
    }
    static void startAllAndWait() throws Exception {
        for (String h : HOSTS) startNode(h);
        awaitHealthy(120);
    }

    // ---- 클라이언트 ----
    static JamsilClient client(int attempts) {
        return JamsilClient.builder().hosts(HOSTS_CSV.replaceAll("([^,]+)", "$1:9786"))
                .credentials(System.getenv("JAMSIL_USER"), System.getenv("JAMSIL_PASSWORD"))
                .applicationName("mh-failover").maxAttempts(attempts).build();
    }
    static Connection direct(String host) throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", System.getenv("JAMSIL_USER")); p.setProperty("password", System.getenv("JAMSIL_PASSWORD"));
        return DriverManager.getConnection("jdbc:postgresql://" + host + ":9786/jamsil?sslmode=require&connectTimeout=3&socketTimeout=30", p);
    }
    static long count(JamsilClient db) throws SQLException {
        return db.read(c -> { try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT count(*) FROM " + T)) { r.next(); return r.getLong(1); } });
    }
    static final AtomicLong NEXT_ID = new AtomicLong(1);
    static void insertOne(JamsilClient db, long id) throws SQLException {
        // PK 충돌(23505)이 나면 재시도가 중복 반영됐다는 뜻 → 시험 실패로 드러난다(ON CONFLICT 를 일부러 쓰지 않음).
        db.update(JamsilClient.newRequestKey("mh-" + id), "INSERT INTO " + T + " VALUES (?, ?)", id, "w");
    }

    /** 백그라운드에서 쉬지 않고 쓰는 작성기. 확정 응답을 받은 id 와 시각을 기록한다. */
    static final class Writer implements AutoCloseable {
        final JamsilClient db = client(40);
        final List<Long> ackIds = Collections.synchronizedList(new ArrayList<>());
        final List<Long> ackAt = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger retried = new AtomicInteger();
        volatile Throwable error; volatile boolean run = true;
        final Thread th = new Thread(() -> {
            try {
                while (run) {
                    long id = NEXT_ID.getAndIncrement();
                    insertOne(db, id);
                    retried.addAndGet(db.retries);
                    ackIds.add(id); ackAt.add(System.currentTimeMillis());
                    Thread.sleep(40);
                }
            } catch (Throwable e) { error = e; }
        }, "writer");
        Writer() { th.start(); }
        int acks() { return ackIds.size(); }
        /** 시각 t0 이후 첫 확정 응답까지 걸린 시간(ms). */
        long gapAfter(long t0, int timeoutSec) throws Exception {
            long end = System.currentTimeMillis() + timeoutSec * 1000L;
            while (System.currentTimeMillis() < end) {
                synchronized (ackAt) { for (long t : ackAt) if (t > t0) return t - t0; }
                if (error != null) throw new AssertionError("writer failed: " + error);
                Thread.sleep(50);
            }
            throw new AssertionError("no acknowledged write within " + timeoutSec + "s after node stop");
        }
        long maxGap(long from) {
            long max = 0, prev = -1;
            synchronized (ackAt) { for (long t : ackAt) { if (t < from) continue; if (prev > 0) max = Math.max(max, t - prev); prev = t; } }
            return max;
        }
        @Override public void close() throws Exception { run = false; th.join(60_000); db.close(); }
    }

    public static void main(String[] a) throws Exception {
        HOSTS_CSV = a.length > 0 ? a[0] : System.getenv("JAMSIL_HOSTS");
        HOSTS = HOSTS_CSV.split(",");
        check(HOSTS.length == 3, "three hosts required");
        System.out.println("hosts=" + HOSTS_CSV + " leader=" + leader());
        awaitHealthy(60);
        try (JamsilClient setup = client(5)) {
            setup.write("mh-ddl-" + UUID.randomUUID(), false, c -> { try (Statement s = c.createStatement()) { s.execute("DROP TABLE IF EXISTS " + T); s.execute("CREATE TABLE " + T + " (id BIGINT PRIMARY KEY, v TEXT)"); } return null; });
        }
        try {
            test("M01 multi-host URL spreads new connections over all three nodes", () -> {
                Map<String, Integer> seen = new TreeMap<>();
                try (JamsilClient db = client(3)) {
                    for (int i = 0; i < 30; i++) try (JamsilClient d = client(3)) { seen.merge(d.connectedTo(), 1, Integer::sum); }
                }
                System.out.println("  connections per node: " + seen);
                check(seen.size() == 3, "expected all three nodes to receive connections: " + seen);
            });

            test("M02 existing connection survives loss of its own node (reconnects elsewhere)", () -> {
                try (JamsilClient db = client(20)) {
                    insertOne(db, NEXT_ID.getAndIncrement());
                    String host = db.connectedTo().split(":")[0];
                    stopNode(host);
                    try {
                        long id = NEXT_ID.getAndIncrement();
                        insertOne(db, id);                                // 끊긴 연결 → 다른 노드로 재연결 후 같은 키로 재시도
                        String now = db.connectedTo().split(":")[0];
                        System.out.println("  was " + host + " now " + now + " retries=" + db.retries);
                        check(!now.equals(host), "client still pointing at the stopped node");
                        check(count(db) >= 2, "rows missing");
                    } finally { startNode(host); awaitHealthy(120); }
                }
            });

            test("M03 single-host URL to a stopped node fails while multi-host URL still connects", () -> {
                String victim = HOSTS[1];
                stopNode(victim);
                try {
                    boolean singleFailed;
                    try (Connection c = direct(victim)) { singleFailed = false; } catch (SQLException e) { singleFailed = true; System.out.println("  single-host: " + e.getSQLState() + " " + e.getMessage().split("\n")[0]); }
                    check(singleFailed, "single-host connection to a stopped node unexpectedly worked");
                    int ok = 0;
                    for (int i = 0; i < 20; i++) try (JamsilClient d = client(3)) { check(!d.connectedTo().startsWith(victim), "connected to stopped node"); ok++; }
                    check(ok == 20, "multi-host connects: " + ok);
                    System.out.println("  multi-host: 20/20 connections succeeded without " + victim);
                } finally { startNode(victim); awaitHealthy(120); }
            });

            // 한 대씩 차례로 중지(Leader → 나머지 Follower 둘): 쓰기 작성기가 멈추지 않고, 한 건도 유실·중복되지 않아야 한다.
            test("M04 rolling stop of each node (leader first) under continuous writes", () -> {
                try (Writer w = new Writer()) {
                    Thread.sleep(2000);
                    check(w.acks() > 10, "writer not progressing before test");
                    String lead = leader();
                    List<String> order = new ArrayList<>(List.of(lead));
                    for (String h : HOSTS) if (!h.equals(lead)) order.add(h);
                    for (String h : order) {
                        stopNode(h);
                        int before = w.acks(); long t0 = System.currentTimeMillis();   // 중지 명령이 끝난 시점부터 측정
                        long gap = w.gapAfter(t0, 60);
                        System.out.println("  stopped " + h + (h.equals(lead) ? " (leader)" : "") + ": first ack " + gap + " ms after stop completed; retries so far=" + w.retried.get());
                        check(gap < 30_000, "write outage too long: " + gap + " ms");
                        Thread.sleep(1500);
                        check(w.acks() > before + 3, "writer stalled with two nodes left");
                        startNode(h); awaitHealthy(120, false);
                        Thread.sleep(1000);
                    }
                    w.close();
                    check(w.error == null, "writer failed: " + w.error);
                    System.out.println("  acked=" + w.acks() + " max gap=" + w.maxGap(0) + " ms retried=" + w.retried.get());
                    verifyAllNodes(w.ackIds);
                }
            });

            test("M05 two nodes down: connect works, writes refused (retryable), then recover exactly once", () -> {
                String keep = HOSTS[2], d1 = HOSTS[0], d2 = HOSTS[1];
                stopNode(d1); stopNode(d2);
                try (JamsilClient quick = client(2)) {
                    Thread.sleep(8000);                                    // check-quorum 으로 남은 노드가 쓰기를 거부할 때까지
                    long id = NEXT_ID.getAndIncrement();
                    SQLException e = null;
                    try { insertOne(quick, id); } catch (SQLException ex) { e = ex; }
                    check(e != null, "write succeeded without quorum");
                    System.out.println("  no-quorum write: " + e.getSQLState() + " " + e.getMessage().split("\n")[0] + " retryable=" + JamsilClient.isRetryable(e));
                    check(JamsilClient.isRetryable(e), "no-quorum error must be retryable by the client");
                    // 같은 키로 계속 재시도하는 작성 1건을 먼저 띄우고, 그 사이 노드를 한 대 올린다 → 정확히 한 번 반영.
                    try (JamsilClient patient = client(60)) {
                        String key = JamsilClient.newRequestKey("mh-recover");
                        long rid = NEXT_ID.getAndIncrement();
                        Future<Integer> f = Executors.newSingleThreadExecutor().submit(() -> patient.update(key, "INSERT INTO " + T + " VALUES (?, ?)", rid, "recover"));
                        Thread.sleep(5000);
                        check(!f.isDone(), "write finished before quorum was restored");
                        startNode(d1);
                        check(f.get(90, TimeUnit.SECONDS) == 1, "recovered write result");
                        System.out.println("  pending write completed after quorum returned, client retries=" + patient.retries);
                        startNode(d2);
                        awaitHealthy(120);
                        verifyAllNodes(List.of(rid));
                    }
                } finally { startAllAndWait(); }
            });
        } finally {
            try { startAllAndWait(); } catch (Throwable e) { System.out.println("WARNING: cluster restore failed: " + e); }
            try (JamsilClient db = client(10)) { db.write("mh-drop-" + UUID.randomUUID(), false, c -> { try (Statement s = c.createStatement()) { s.execute("DROP TABLE IF EXISTS " + T); } return null; }); System.out.println("CLEANUP: " + T + " removed"); }
            catch (Throwable e) { System.out.println("CLEANUP FAILED: " + e); }
        }
        System.out.printf("RESULT: %d passed, %d failed%n", pass, fail);
        System.exit(fail == 0 ? 0 : 1);
    }

    /** 확정 응답을 받은 모든 id 가 3개 노드 모두에 정확히 한 번씩 있는지 확인. */
    static void verifyAllNodes(List<Long> acked) throws Exception {
        Set<Long> want = new HashSet<>(acked);
        for (String h : HOSTS) {
            Set<Long> have = new HashSet<>(); long rows = 0;
            try (Connection c = direct(h); Statement s = c.createStatement()) {
                s.execute("SET jamsil.read_consistency = 'linearizable'");
                try (ResultSet r = s.executeQuery("SELECT id FROM " + T)) { while (r.next()) { have.add(r.getLong(1)); rows++; } }
            }
            Set<Long> lost = new HashSet<>(want); lost.removeAll(have);
            check(lost.isEmpty(), h + ": acknowledged writes lost: " + lost.size() + " e.g. " + lost.stream().limit(5).toList());
            check(rows == have.size(), h + ": duplicate rows");
        }
        System.out.println("  verified " + want.size() + " acknowledged writes on all 3 nodes, no loss/duplicates");
    }
}
