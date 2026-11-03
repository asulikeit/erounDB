// 동시 세션 쓰기 직렬화 시험: java -cp postgresql-42.7.4.jar Concurrency.java <port,port,...> [threads] [ops]
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class Concurrency {
    static String[] eps;
    static Connection conn(int idx) throws SQLException {
        Properties p = new Properties(); p.setProperty("user", System.getenv().getOrDefault("JAMSIL_USER", "jamsil")); p.setProperty("password", System.getenv().getOrDefault("JAMSIL_PASSWORD", "jamsil"));
        return DriverManager.getConnection("jdbc:postgresql://" + eps[idx] + "/jamsil?sslmode=require&socketTimeout=120", p);
    }
    public static void main(String[] a) throws Exception {
        // 인자: host:port[,host:port...] (포트만 쓰면 127.0.0.1)
        eps = Arrays.stream(a[0].split(",")).map(x -> x.contains(":") ? x : "127.0.0.1:" + x).toArray(String[]::new);
        int[] ports = java.util.stream.IntStream.range(0, eps.length).toArray();
        int threads = a.length > 1 ? Integer.parseInt(a[1]) : 16;
        int ops = a.length > 2 ? Integer.parseInt(a[2]) : 30;
        try (Connection c = conn(ports[0]); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS conc");
            s.execute("CREATE TABLE conc (id BIGINT PRIMARY KEY, t INT, v INT)");
            s.execute("DROP TABLE IF EXISTS counter");
            s.execute("CREATE TABLE counter (id INT PRIMARY KEY, n BIGINT)");
            s.execute("INSERT INTO counter VALUES (1, 0)");
        }
        AtomicLong committedRows = new AtomicLong(), increments = new AtomicLong(), errors = new AtomicLong();
        ExecutorService ex = Executors.newFixedThreadPool(threads);
        long t0 = System.nanoTime();
        List<Future<?>> fs = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            fs.add(ex.submit(() -> {
                try (Connection c = conn(ports[tid % ports.length])) {
                    for (int i = 0; i < ops; i++) {
                        long id = tid * 1_000_000L + i;
                        try {
                            if (i % 3 == 0) {
                                // 명시적 트랜잭션: 2행 + 카운터 증가, 일부는 롤백
                                c.setAutoCommit(false);
                                try (Statement s = c.createStatement()) {
                                    s.executeUpdate("INSERT INTO conc VALUES (" + id + ", " + tid + ", 1)");
                                    s.executeUpdate("INSERT INTO conc VALUES (" + (id + 500_000) + ", " + tid + ", 1)");
                                    s.executeUpdate("UPDATE counter SET n = n + 1 WHERE id = 1");
                                }
                                if (i % 9 == 0) { c.rollback(); } else { c.commit(); committedRows.addAndGet(2); increments.incrementAndGet(); }
                                c.setAutoCommit(true);
                            } else {
                                try (Statement s = c.createStatement()) {
                                    s.executeUpdate("INSERT INTO conc VALUES (" + id + ", " + tid + ", 1)");
                                    committedRows.incrementAndGet();
                                }
                            }
                        } catch (SQLException e) {
                            errors.incrementAndGet();
                            System.out.println("error: " + e.getSQLState() + " " + e.getMessage());
                            try { c.rollback(); c.setAutoCommit(true); } catch (SQLException x) { }
                        }
                    }
                } catch (SQLException e) { errors.incrementAndGet(); System.out.println("conn error: " + e.getMessage()); }
                return null;
            }));
        }
        for (Future<?> f : fs) f.get(15, TimeUnit.MINUTES);
        ex.shutdown();
        double secs = (System.nanoTime() - t0) / 1e9;
        try (Connection c = conn(ports[0]); Statement s = c.createStatement()) {
            ResultSet r = s.executeQuery("SELECT count(*) FROM conc"); r.next(); long rows = r.getLong(1);
            r = s.executeQuery("SELECT n FROM counter WHERE id = 1"); r.next(); long n = r.getLong(1);
            System.out.printf("threads=%d ops=%d time=%.1fs commits/s=%.1f errors=%d%n", threads, threads * ops, secs, (threads * ops) / secs, errors.get());
            System.out.printf("rows: expected %d actual %d | counter: expected %d actual %d%n", committedRows.get(), rows, increments.get(), n);
            boolean ok = rows == committedRows.get() && n == increments.get() && errors.get() == 0;
            System.out.println(ok ? "RESULT: OK" : "RESULT: MISMATCH");
            System.exit(ok ? 0 : 1);
        }
    }
}
