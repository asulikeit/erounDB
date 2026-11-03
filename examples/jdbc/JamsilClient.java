// erounDB JDBC 연계 샘플 (JDK 17+, pgjdbc 42.x). 운영 코드에 그대로 복사해 쓸 수 있도록 외부 의존성이 없다.
//
// 핵심 규칙
//  1) URL에 클러스터 노드를 모두 나열한다 (한 노드만 쓰면 그 노드 장애가 곧 서비스 장애).
//  2) 연결이 끊기거나 일시 오류(08xxx, 57P03, 40001, 40P01)가 나면 새 연결로 처음부터 다시 시도한다.
//  3) 쓰기는 논리 작업(요청)마다 멱등 키(jamsil.request_id)를 한 번 만들고, 재시도에도 같은 키를 쓴다.
//     → 응답이 유실된 상태(JDB-2003 / 08007 / 연결 끊김)에서 재시도해도 효과는 정확히 한 번이다.
//  4) 비밀번호는 코드에 쓰지 않는다 (환경변수/비밀 저장소에서 주입).
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

public final class JamsilClient implements AutoCloseable {
    /** 트랜잭션 본문. 재시도되므로 본문 안에서 외부에 부수 효과를 만들지 않는다. */
    @FunctionalInterface public interface Work<T> { T run(Connection c) throws SQLException; }

    public static final class Builder {
        private final List<String> hosts = new ArrayList<>();
        private String database = "jamsil", user, password, appName = "jamsil-client";
        private int connectTimeoutSec = 3, socketTimeoutSec = 30, maxAttempts = 8;
        private long baseBackoffMs = 200, maxBackoffMs = 3000;
        /** "host:port" 를 노드 수만큼 추가. 포트 생략 시 9786. */
        public Builder host(String hostPort) { hosts.add(hostPort.contains(":") ? hostPort : hostPort + ":9786"); return this; }
        public Builder hosts(String commaSeparated) { for (String h : commaSeparated.split(",")) if (!h.isBlank()) host(h.trim()); return this; }
        public Builder database(String d) { database = d; return this; }
        public Builder credentials(String u, String p) { user = u; password = p; return this; }
        public Builder applicationName(String n) { appName = n; return this; }
        public Builder connectTimeoutSec(int s) { connectTimeoutSec = s; return this; }
        /** 한 문장이 이 시간 안에 응답하지 않으면 연결을 끊고 재시도한다(Leader 교체 중 무기한 대기 방지). */
        public Builder socketTimeoutSec(int s) { socketTimeoutSec = s; return this; }
        public Builder maxAttempts(int n) { maxAttempts = n; return this; }
        public JamsilClient build() {
            if (hosts.isEmpty()) throw new IllegalStateException("at least one host is required (list ALL cluster nodes)");
            return new JamsilClient(this);
        }
    }
    public static Builder builder() { return new Builder(); }

    private static final Pattern SAFE_KEY = Pattern.compile("[A-Za-z0-9._:-]{1,128}");

    private final Builder cfg;
    private final String url;
    private Connection conn;
    /** 마지막 시도에서 재시도가 발생한 횟수(시험/관측용). */
    public int retries;

    private JamsilClient(Builder b) {
        this.cfg = b;
        // loadBalanceHosts: 연결 시 노드 순서를 섞어 부하 분산. hostRecheckSeconds: 장애 노드를 건너뛰는 시간.
        // 어느 노드에 붙어도 읽기·쓰기 모두 가능하다(쓰기는 서버가 Leader로 전달) → targetServerType 은 기본값(any).
        this.url = "jdbc:postgresql://" + String.join(",", b.hosts) + "/" + b.database
                + "?sslmode=require&loadBalanceHosts=true&hostRecheckSeconds=2&tcpKeepAlive=true"
                + "&connectTimeout=" + b.connectTimeoutSec + "&socketTimeout=" + b.socketTimeoutSec
                + "&ApplicationName=" + b.appName;
    }

    public String url() { return url; }

    /** 현재 연결이 실제로 붙어 있는 노드 주소(관측/디버그용, pgjdbc 내부 API 사용). 연결이 없으면 새로 연다. */
    public String connectedTo() throws SQLException {
        Connection c = connection();
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT 1")) { r.next(); }
        return hostOf(c);
    }
    public static String hostOf(Connection c) throws SQLException {
        org.postgresql.util.HostSpec h = c.unwrap(org.postgresql.core.BaseConnection.class).getQueryExecutor().getHostSpec();
        return h.getHost() + ":" + h.getPort();
    }

    private Connection connection() throws SQLException {
        if (conn == null || conn.isClosed()) {
            Properties p = new Properties();
            p.setProperty("user", cfg.user);
            p.setProperty("password", cfg.password);
            conn = DriverManager.getConnection(url, p);
        }
        return conn;
    }

    private void discard() {
        try { if (conn != null) conn.close(); } catch (SQLException ignore) { }
        conn = null;
    }

    /** 새 연결/재시도로 해결될 수 있는 오류인가. */
    public static boolean isRetryable(SQLException e) {
        for (SQLException x = e; x != null; x = x.getNextException()) {
            String s = x.getSQLState();
            if (s != null && (s.startsWith("08")                 // 연결 오류, 08007 = 커밋 결과 불명(JDB-2003)
                    || s.equals("57P01") || s.equals("57P03")    // 종료 중 / 지금 연결 불가(JDB-2001: 과반수 없음·Leader 선출 중)
                    || s.equals("40001") || s.equals("40P01")))  // 직렬화 실패(JDB-2002 포함) / 교착
                return true;
            if (x.getCause() instanceof java.io.IOException) return true;
        }
        return false;
    }
    private static boolean isConnectionLevel(SQLException e) {
        String s = e.getSQLState();
        return s == null || s.startsWith("08") || s.startsWith("57P") || e.getCause() instanceof java.io.IOException;
    }

    /** 멱등 키 생성: 논리 작업마다 한 번 만들어 재시도에 재사용한다. */
    public static String newRequestKey(String prefix) { return prefix + "-" + UUID.randomUUID(); }

    private static void setRequestId(Connection c, String key) throws SQLException {
        if (!SAFE_KEY.matcher(key).matches()) throw new IllegalArgumentException("unsafe request key");
        try (Statement s = c.createStatement()) { s.execute("SET jamsil.request_id = '" + key + "'"); }
    }

    /**
     * 읽기 전용 작업(멱등 키 불필요). 끊기면 다른 노드로 재연결해 다시 실행한다.
     * 기본 일관성은 linearizable 이라 항상 최신 커밋을 읽는다.
     */
    public <T> T read(Work<T> work) throws SQLException {
        return retrying(null, false, work);
    }

    /**
     * 쓰기 작업 1건(단일 문장 또는 한 트랜잭션).
     * @param requestKey 논리 작업당 1개. 재시도해도 같은 키가 쓰이므로 정확히 한 번만 반영된다.
     * @param transaction true면 BEGIN…COMMIT 으로 묶고 COMMIT 효과가 멱등이다. 이미 확정된 트랜잭션을 재시도하다 PK 충돌
     *                    (23505)이 나면 확정된 것으로 보고 {@code null} 을 돌려준다(본문의 결과값은 없다). false면 autocommit 단일 문장이며
     *                    RETURNING 결과까지 재생된다(결과가 필요하면 이쪽).
     */
    public <T> T write(String requestKey, boolean transaction, Work<T> work) throws SQLException {
        return retrying(requestKey, transaction, work);
    }

    /** 단일 문장 INSERT/UPDATE/DELETE 편의 메서드. 영향 행 수를 돌려준다(재시도 시에도 처음 값). */
    public int update(String requestKey, String sql, Object... params) throws SQLException {
        return write(requestKey, false, c -> {
            try (PreparedStatement p = c.prepareStatement(sql)) {
                for (int i = 0; i < params.length; i++) p.setObject(i + 1, params[i]);
                return p.executeUpdate();
            }
        });
    }

    private <T> T retrying(String key, boolean tx, Work<T> work) throws SQLException {
        SQLException last = null;
        retries = 0;
        for (int attempt = 1; attempt <= cfg.maxAttempts; attempt++) {
            Connection c = null;
            try {
                c = connection();
                if (key != null) {
                    if (tx) {
                        c.setAutoCommit(false);
                        T out = work.run(c);
                        setRequestId(c, key);       // COMMIT 에 적용되는 멱등 키
                        c.commit();
                        return out;
                    }
                    c.setAutoCommit(true);
                    setRequestId(c, key);           // 바로 다음 쓰기 문장 1건에 적용
                    return work.run(c);
                }
                c.setAutoCommit(true);
                return work.run(c);
            } catch (SQLException e) {
                last = e;
                if (c != null && tx) { try { c.rollback(); } catch (SQLException ignore) { } }
                // 이전 시도의 COMMIT 은 성공했는데 응답만 유실된 경우: 재실행한 INSERT 가 COMMIT 시점의 멱등 확인보다
                // 먼저 PK 충돌(23505)로 실패한다. 같은 키가 이미 확정됐는지 확인해 그렇다면 성공으로 처리한다.
                if (tx && key != null && "23505".equals(e.getSQLState()) && alreadyCommitted(key, e)) return null;
                if (!isRetryable(e)) throw e;
                if (isConnectionLevel(e)) discard();   // 죽은 연결은 버리고 다음 시도에서 다른 노드로 접속
                retries++;
                if (attempt == cfg.maxAttempts) break;
                sleep(backoff(attempt));
            }
        }
        throw last;
    }

    private static final java.util.regex.Pattern DUP_TABLE =
            java.util.regex.Pattern.compile("UNIQUE constraint failed: ([A-Za-z_][A-Za-z0-9_]*)\\.");

    /**
     * 이 키로 트랜잭션이 이미 확정됐는지 확인한다. 서버는 COMMIT 의 키를 "문장 전체 지문"과 대조하므로, 다른 내용의
     * 트랜잭션을 같은 키로 COMMIT 해 보면 확정된 키는 22023 으로 거절된다. 키는 쓰기 문장이 Writer 를 잡을 때 적용되므로
     * 확인용 문장도 쓰기여야 한다 → 방금 충돌한 테이블(오류 메시지에서 얻는다. 이 사용자가 쓸 수 있는 테이블)에 행을
     * 바꾸지 않는 {@code DELETE ... WHERE 1=0} 을 쓴다. 확정되지 않은 키였다면 이 확인이 키를 소모하지만, 호출자는
     * 원래 오류(진짜 중복)를 그대로 받고 작업을 포기하므로 문제없다. 테이블을 알 수 없으면 확인하지 않는다(false).
     */
    private boolean alreadyCommitted(String key, SQLException dup) {
        java.util.regex.Matcher m = DUP_TABLE.matcher(String.valueOf(dup.getMessage()));
        if (!m.find()) return false;
        try {
            Connection c = connection();
            try {
                c.setAutoCommit(false);
                try (Statement s = c.createStatement()) { s.execute("DELETE FROM " + m.group(1) + " WHERE 1 = 0"); }
                setRequestId(c, key);
                c.commit();
                return false;
            } catch (SQLException e) {
                try { c.rollback(); } catch (SQLException ignore) { }
                return "22023".equals(e.getSQLState());
            }
        } catch (SQLException e) {
            return false;
        }
    }

    private long backoff(int attempt) {
        long d = Math.min(cfg.maxBackoffMs, cfg.baseBackoffMs << Math.min(attempt - 1, 10));
        return d / 2 + ThreadLocalRandom_nextLong(d / 2 + 1);   // 지터
    }
    private static long ThreadLocalRandom_nextLong(long bound) { return java.util.concurrent.ThreadLocalRandom.current().nextLong(bound); }
    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); throw new IllegalStateException(ie); }
    }

    @Override public void close() { discard(); }
}
