// 간단한 지연/처리량 측정: java -cp postgresql-42.7.4.jar Bench.java <url> [n]
import java.sql.*;
import java.util.*;

public class Bench {
    static Connection conn(String url) throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", System.getenv().getOrDefault("JAMSIL_USER", "jamsil"));
        p.setProperty("password", System.getenv().getOrDefault("JAMSIL_PASSWORD", "jamsil"));
        return DriverManager.getConnection(url, p);
    }

    static void report(String name, long[] lat) {
        Arrays.sort(lat);
        long sum = 0;
        for (long l : lat) sum += l;
        System.out.printf("%-34s n=%5d  avg=%7.2f ms  p50=%6.2f  p95=%6.2f  p99=%6.2f  ops/s=%8.1f%n", name, lat.length,
            sum / 1e6 / lat.length, lat[lat.length / 2] / 1e6, lat[(int) (lat.length * 0.95)] / 1e6,
            lat[(int) (lat.length * 0.99)] / 1e6, lat.length / (sum / 1e9));
    }

    public static void main(String[] a) throws Exception {
        String url = a[0];
        int n = a.length > 1 ? Integer.parseInt(a[1]) : 500;
        try (Connection c = conn(url); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS bench");
            s.execute("CREATE TABLE bench (id BIGINT PRIMARY KEY, v VARCHAR(100), n INT)");
            long[] lat = new long[n];
            PreparedStatement ins = c.prepareStatement("INSERT INTO bench (id, v, n) VALUES (?, ?, ?)");
            for (int i = 0; i < n; i++) {
                long t = System.nanoTime();
                ins.setLong(1, i); ins.setString(2, "value " + i); ins.setInt(3, i % 100);
                ins.executeUpdate();
                lat[i] = System.nanoTime() - t;
            }
            report("autocommit INSERT", lat);
            PreparedStatement sel = c.prepareStatement("SELECT v FROM bench WHERE id = ?");
            for (int i = 0; i < n; i++) {
                long t = System.nanoTime();
                sel.setLong(1, i);
                ResultSet r = sel.executeQuery();
                r.next();
                lat[i] = System.nanoTime() - t;
            }
            report("PK SELECT (linearizable)", lat);
            s.execute("SET jamsil.read_consistency = 'local'");
            for (int i = 0; i < n; i++) {
                long t = System.nanoTime();
                sel.setLong(1, i);
                ResultSet r = sel.executeQuery();
                r.next();
                lat[i] = System.nanoTime() - t;
            }
            report("PK SELECT (local)", lat);
            s.execute("SET jamsil.read_consistency = 'linearizable'");
            c.setAutoCommit(false);
            long[] tl = new long[n];
            for (int i = 0; i < n; i++) {
                long t = System.nanoTime();
                ins.setLong(1, 1_000_000 + i); ins.setString(2, "tx " + i); ins.setInt(3, 1);
                ins.executeUpdate();
                tl[i] = System.nanoTime() - t;
            }
            long t0 = System.nanoTime();
            c.commit();
            report("INSERT inside explicit txn", tl);
            System.out.printf("%-34s %.2f ms%n", "COMMIT of " + n + " rows", (System.nanoTime() - t0) / 1e6);
            c.setAutoCommit(true);
        }
    }
}
