// 간단한 SQL 실행기: java -cp postgresql-42.7.4.jar Sql.java <port> "<sql>" ["<sql>" ...]
import java.sql.*;
import java.util.*;
public class Sql {
    public static void main(String[] a) throws Exception {
        Properties p = new Properties(); p.setProperty("user", System.getenv().getOrDefault("JAMSIL_USER", "jamsil")); p.setProperty("password", System.getenv().getOrDefault("JAMSIL_PASSWORD", "jamsil"));
        try (Connection c = DriverManager.getConnection("jdbc:postgresql://" + (a[0].contains(":") ? a[0] : "127.0.0.1:" + a[0]) + "/jamsil?sslmode=require", p); Statement s = c.createStatement()) {
            for (int i = 1; i < a.length; i++) {
                System.out.println("> " + a[i]);
                try {
                    if (s.execute(a[i])) {
                        ResultSet r = s.getResultSet(); ResultSetMetaData m = r.getMetaData();
                        while (r.next()) {
                            StringBuilder sb = new StringBuilder();
                            for (int k = 1; k <= m.getColumnCount(); k++) sb.append(k > 1 ? " | " : "").append(r.getString(k));
                            System.out.println(sb);
                        }
                    } else System.out.println("OK " + s.getUpdateCount());
                    for (SQLWarning w = s.getWarnings(); w != null; w = w.getNextWarning()) System.out.println("NOTICE " + w.getMessage());
                    s.clearWarnings();
                } catch (SQLException e) { System.out.println("ERROR " + e.getSQLState() + " " + e.getMessage()); }
            }
        }
    }
}
