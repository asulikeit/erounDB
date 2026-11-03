import java.sql.*;
import java.util.*;
public class Probe {
    public static void main(String[] a) throws Exception {
        Properties p = new Properties(); p.setProperty("user", System.getenv().getOrDefault("JAMSIL_USER", "jamsil")); p.setProperty("password", System.getenv().getOrDefault("JAMSIL_PASSWORD", "jamsil"));
        try (Connection c = DriverManager.getConnection(a[0], p); Statement s = c.createStatement()) {
            for (String q : new String[]{"SHOW server_version", "SELECT 1", "SET jamsil.read_consistency = 'local'", "SELECT 1"}) {
                for (int w = 0; w < 50; w++) s.execute(q);
                long t = System.nanoTime();
                for (int i = 0; i < 300; i++) s.execute(q);
                System.out.printf("%-45s %.3f ms%n", q, (System.nanoTime() - t) / 1e6 / 300);
            }
        }
    }
}
