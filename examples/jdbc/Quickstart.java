// JamsilClient 사용 예. 환경변수: JAMSIL_HOSTS(쉼표 구분, 모든 노드), JAMSIL_USER, JAMSIL_PASSWORD
//   examples/jdbc/run.sh 10.0.0.1,10.0.0.2,10.0.0.3
import java.sql.*;

public class Quickstart {
    public static void main(String[] args) throws Exception {
        String hosts = args.length > 0 ? args[0] : System.getenv("JAMSIL_HOSTS");
        try (JamsilClient db = JamsilClient.builder().hosts(hosts)
                .credentials(System.getenv("JAMSIL_USER"), System.getenv("JAMSIL_PASSWORD"))
                .applicationName("quickstart").build()) {

            // 스키마(예제 전용 테이블)
            db.write(JamsilClient.newRequestKey("ddl"), false, c -> {
                try (Statement s = c.createStatement()) {
                    s.execute("CREATE TABLE IF NOT EXISTS sample_orders (id BIGINT PRIMARY KEY, customer TEXT, amount BIGINT)");
                    s.execute("CREATE TABLE IF NOT EXISTS sample_stock (sku TEXT PRIMARY KEY, qty BIGINT)");
                }
                return null;
            });

            // 1) 단일 문장 쓰기 — 키는 논리 작업당 1개. 재시도되어도 한 번만 반영.
            String k1 = JamsilClient.newRequestKey("order-1001");
            int n = db.update(k1, "INSERT INTO sample_orders VALUES (?, ?, ?) ON CONFLICT (id) DO NOTHING", 1001L, "kim", 5000L);
            System.out.println("insert affected=" + n);

            // 2) RETURNING 결과가 필요한 쓰기 — autocommit 단일 문장은 결과까지 재생된다.
            long amount = db.write(JamsilClient.newRequestKey("order-1001-bump"), false, c -> {
                try (PreparedStatement p = c.prepareStatement("UPDATE sample_orders SET amount = amount + ? WHERE id = ? RETURNING amount")) {
                    p.setLong(1, 100); p.setLong(2, 1001);
                    try (ResultSet r = p.executeQuery()) { r.next(); return r.getLong(1); }
                }
            });
            System.out.println("amount after bump=" + amount);

            // 3) 여러 문장 트랜잭션 — 본문은 재시도 시 처음부터 다시 실행되므로 DB 밖의 부수 효과를 두지 않는다.
            String k2 = JamsilClient.newRequestKey("order-1002");
            db.write(k2, true, c -> {
                try (PreparedStatement o = c.prepareStatement("INSERT INTO sample_orders VALUES (?, ?, ?)");
                     PreparedStatement s = c.prepareStatement("INSERT INTO sample_stock VALUES (?, ?) ON CONFLICT (sku) DO UPDATE SET qty = sample_stock.qty - 1")) {
                    o.setLong(1, 1002); o.setString(2, "lee"); o.setLong(3, 7000); o.executeUpdate();
                    s.setString(1, "SKU-1"); s.setLong(2, 99); s.executeUpdate();
                }
                return null;
            });

            // 4) 읽기 — 끊기면 다른 노드로 자동 재연결.
            db.read(c -> {
                try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT id, customer, amount FROM sample_orders ORDER BY id")) {
                    while (r.next()) System.out.printf("order %d %s %d%n", r.getLong(1), r.getString(2), r.getLong(3));
                }
                return null;
            });

            // 5) 정리(예제 테이블 삭제)
            db.write(JamsilClient.newRequestKey("cleanup"), false, c -> {
                try (Statement s = c.createStatement()) { s.execute("DROP TABLE IF EXISTS sample_orders"); s.execute("DROP TABLE IF EXISTS sample_stock"); }
                return null;
            });
            System.out.println("done (retries in last op: " + db.retries + ")");
        }
    }
}
