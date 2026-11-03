# erounDB

> **erounDB** — a distributed relational database written in Rust: PostgreSQL wire protocol, SQLite storage, 3-node Raft synchronous replication, Korean full-text search.

[Korean README.md](README-ko.md)

A distributed relational database built with Rust. By supporting the **PostgreSQL wire protocol**, you can connect immediately using existing clients such as DBeaver, standard PostgreSQL JDBC, and psycopg. Data is stored in **SQLite**, and with **3-node Raft synchronous replication**, writes are not lost even if a node goes down. It supports morphological analysis **full-text search** (Tantivy + Lindera) for Korean and English, and **TEXT up to 32MiB**.

> **Status: 0.1.0 (PoC)** — Passed 3-node VM failure tests (Leader/Follower forced termination, network partition, 2-node stop, full restart), but please check the [Known Limitations](#known-limitations) before production use.
> For implementation specifications, test methods, and results, refer to [docs/REFERENCE.md](docs/REFERENCE.md). For design documents, see [docs/DESIGN.md](docs/DESIGN.md).
>
> **About the name:** The project (distribution) name is erounDB, but the executable files (`jamsild` …), crates, configuration paths (`/etc/jamsildb`), services (`jamsildb.service`), default DB/account (`jamsil`), and SQL extensions (`jamsil_catalog`, `jamsil.request_id`) retain the initial code name `jamsil`. `jamsil*` in the documentation and examples are not typos.

## Table of Contents

1. [Features](#features)
2. [Architecture](#architecture)
3. [Quick Start (Local)](#quick-start-local)
4. [Build and Deployment (Rocky Linux 9, 3-Node)](#build-and-deployment-rocky-linux-9-3-node)
5. [Client Integration](#client-integration) — [Connection Info](#connection-info) · [Java](#java-jdbc) · [Python](#python-psycopg-3) · [DBeaver](#dbeaver)
6. [SQL Usage Notes](#sql-usage-notes)
7. [Error Codes and Retry Guide](#error-codes-and-retry-guide)
8. [Testing](#testing)
9. [Known Limitations](#known-limitations)
10. [Repository Structure](#repository-structure)
11. [License](#license)

## Features

| | |
|---|---|
| Connection | PostgreSQL v3 protocol (TCP **9786**), SCRAM-SHA-256 authentication, TLS required (default) |
| Replication | Custom Raft implementation, 3-node synchronous replication (TCP **9787**). Returns success only after a majority fsync → Acknowledged writes are never lost |
| Availability | Write resumes within ~2.4 seconds after Leader termination (3-node test). Read/write available by connecting to any node (writes are automatically forwarded to the Leader) |
| Consistency | Default read is linearizable, READ COMMITTED / REPEATABLE READ / SERIALIZABLE |
| Idempotent Retries | `SET jamsil.request_id` — Retry writes with lost responses safely; applied exactly once |
| TEXT | Up to 32MiB, `LIKE` prohibited (`JDB-4102`), `TEXT_MATCH`/`TEXT_SCORE` full-text search (Korean/English analyzer, BM25) |
| Processes | Request, Storage, Sync, Search, and Log separated into 6 OS processes (supervised by `jamsild`) |

## Architecture

```text
Client ──PG v3(9786, TLS)──▶ jamsil-sql ──UDS──▶ jamsil-sync ──Raft(9787)──▶ Other Nodes
                                │                    │ (Forwarded to Leader if Follower)
                                └──UDS──▶ jamsil-store (SQLite Writer/Reader, Apply)
                                              └─Outbox─▶ jamsil-search (Tantivy)
jamsild (Supervisor) · jamsil-log (Log Collector)
```

All writes are serialized through a single Writer across the cluster (SQLite single Writer). Reads are handled by the replica of the connected node.

## Quick Start (Local)

Prerequisites: Linux (developed and tested on Rocky Linux 9), Rust stable (`rust-toolchain.toml`), `gcc` (for bundled SQLite build), JDK 17+ (for Java samples). `protoc` is not required.

```bash
git clone https://github.com/<owner>/erounDB.git && cd erounDB
cargo build --release          # Requires internet for the first build to download the Lindera ko-dic dictionary, which takes time
scripts/dev.sh cluster         # 3 Nodes: SQL 127.0.0.1:19786 / 29786 / 39786 (Raft 19787 / 29787 / 39787)
# 1 Node only: scripts/dev.sh standalone   (127.0.0.1:9786)
scripts/dev.sh stop            # Stop the cluster
scripts/dev.sh clean           # Stop and delete dev-data
```

The account for the development cluster is `jamsil` / `jamsil` (**Development only**). Connection URL:

```
jdbc:postgresql://127.0.0.1:19786,127.0.0.1:29786,127.0.0.1:39786/jamsil?sslmode=require&loadBalanceHosts=true
```

## Build and Deployment (Rocky Linux 9, 3-Node)

### 1. Build

```bash
sudo dnf install -y gcc rpm-build git
# Rust: https://rustup.rs (rust-toolchain.toml specifies stable)
cargo test --workspace                 # Unit/Integration tests
cargo build --release                  # target/release/{jamsild,jamsil-sql,jamsil-store,jamsil-sync,jamsil-search,jamsil-log}
RELEASE=8 scripts/build-rpm.sh         # RPM for distribution (thin LTO) + manifest
```

Outputs: `~/rpmbuild/RPMS/x86_64/eroundb-0.1.0-8.el9.x86_64.rpm`, `dist/eroundb-0.1.0-8.manifest` (Mapping table for Build ID, source hash, binary/RPM SHA-256).
`jamsild --version` outputs `<version> (build <git>+src.<source hash>)`, and the same value appears in `SELECT version()`. After deployment, you can verify that the installed binaries come from this source by comparing the manifest with `sha256sum /usr/bin/jamsil*`.

### 2. Installation (Per Node)

```bash
sudo rpm -Uvh eroundb-0.1.0-8.el9.x86_64.rpm       # Installs binaries, systemd unit, jamsildb account, /etc/jamsildb
ls /etc/jamsildb/                                    # jamsildb.toml (1-node default), cluster.toml.example
```

### 3. 3-Node Cluster Configuration

Copy `/etc/jamsildb/cluster.toml.example` to `/etc/jamsildb/jamsildb.toml` on all three nodes and edit it. The only values that differ per node are `node_id` and `cluster_bind`.

```bash
sudo cp /etc/jamsildb/cluster.toml.example /etc/jamsildb/jamsildb.toml
jamsild hash-password '<Service Account Password>'   # → SCRAM-SHA-256$4096:... (paste this into scram_secret)
openssl rand -base64 48                              # → cluster_secret (Same across all three nodes)
sudo vi /etc/jamsildb/jamsildb.toml
```

| Config | Node 1 | Node 2 | Node 3 |
|---|---|---|---|
| `[server] node_id` | 1 | 2 | 3 |
| `[network] cluster_bind` | `10.0.0.1:9787` | `10.0.0.2:9787` | `10.0.0.3:9787` |
| `[security] cluster_secret` | **Same for all 3 nodes** (Long random value) | | |
| `[cluster] cluster_id` | **Same for all 3 nodes** | | |
| `[[cluster.peers]]` / `initial_voters` | **Same for all 3 nodes** (1, 2, 3 and respective addresses) | | |
| `[[security.users]]` | **Same for all 3 nodes** | | |

- Users and roles are managed via the configuration file (`superuser` / `readwrite` / `readonly`, restricting access with `tables = [...]` if needed). Use `readwrite` for applications and `superuser` only for administration.
- TLS: If `cert_file`/`key_file` are not provided, a self-signed certificate is generated in `data_dir/tls`. Specify your own certificates in production.
- Port 9787 between nodes only performs mutual authentication (HMAC) and is **not encrypted**. Block 9787 via network/firewall so it is only accessible between cluster nodes.
- Ineffective options (e.g., `like_enabled = true`, `cluster_mtls = true`) are not silently ignored; the server will refuse to start.

```bash
# Firewall (firewalld example): Open 9786 to the client network, and 9787 only to other nodes
sudo firewall-cmd --permanent --add-port=9786/tcp
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="10.0.0.2" port port="9787" protocol="tcp" accept'
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="10.0.0.3" port port="9787" protocol="tcp" accept'   # Node 1 perspective — open to the other two nodes
sudo firewall-cmd --reload
```

### 4. Start and Verify

```bash
sudo systemctl enable --now jamsildb          # On all three nodes
sudo jamsild status --config /etc/jamsildb/jamsildb.toml
#   raft: role=leader term=… commit=… quorum=true ready=true     (ready=true only on Leader)
#   peer 2 … connected=true match=…                              (Follower's match should follow the commit)
journalctl -u jamsildb -f                     # Logs (JSON), also written to /var/log/jamsildb/
```

You can also verify via SQL: `SELECT * FROM jamsil_catalog.cluster_nodes;`, `SELECT * FROM jamsil_catalog.replica_checksum;` (index and checksum should match across all three nodes).

## Client Integration

### Connection Info

| Item | Value |
|---|---|
| Host / Port | `ip:9786` for **all nodes** |
| Database | `jamsil` |
| User / Password | Accounts defined in `[[security.users]]` in the config file |
| SSL | `require` (TLS required. Server certificate validation is disabled by default) |
| Driver | Standard PostgreSQL Driver (pgjdbc 42.x, psycopg 3.x, DBeaver's PostgreSQL) |

**List the entire cluster in the connection address, not just one.** You can read/write connected to any node. If you only list one and it dies, the client will fail even if the server cluster is healthy.
However, **dropped connections and in-progress transactions are not recovered.** Applications must reconnect and retry, using the "Idempotent Key" for safe retries as shown below.

### Common Rules (All Languages)

(Examples below assume `CREATE TABLE orders (id BIGINT PRIMARY KEY, customer TEXT, amount BIGINT)` exists.)

1. List all nodes in the connection address, and configure connection timeout (`connectTimeout`) and socket timeout (`socketTimeout`). Without a socket timeout, you may wait indefinitely during Leader changes or loss of quorum.
2. Errors to retry: Connection loss `08xxx` (`08007` = unknown commit state), connection failure, `57P03` (no quorum/electing), `40001` (serialization failure), `40P01`. Do not retry other errors.
3. **Idempotent Key:** Generate a single key per logical write operation (request) and use the same key for retries. The key applies to the next 1 write statement (or COMMIT if in a transaction).
   ```sql
   SET jamsil.request_id = 'order-1001-6f1c…';        -- Unique value per request
   INSERT INTO orders VALUES (1001, 'kim', 5000);      -- Applied only once even if resent due to lost response
   ```
   Retrying with the same key returns the exact same initial result (`RETURNING` rows, affected row count). Using a different statement/parameter with the same key results in `22023`. The server doesn't delete these keys, so make them universally unique like UUIDs.
4. Do not put side effects outside the DB (like sending emails or publishing messages) inside the code block being retried.
5. Do not hardcode passwords in code/repositories (use environment variables/secret vaults).

### Java (JDBC)

**Dependency** (Maven):

```xml
<dependency>
  <groupId>org.postgresql</groupId>
  <artifactId>postgresql</artifactId>
  <version>42.7.4</version>
</dependency>
```

**Connection URL** — List all nodes separated by commas:

```
jdbc:postgresql://10.0.0.1:9786,10.0.0.2:9786,10.0.0.3:9786/jamsil?sslmode=require&loadBalanceHosts=true&hostRecheckSeconds=2&connectTimeout=3&socketTimeout=30
```

**Simplest Usage** (No retries — for learning/tooling):

```java
Properties p = new Properties();
p.setProperty("user", System.getenv("JAMSIL_USER"));
p.setProperty("password", System.getenv("JAMSIL_PASSWORD"));
try (Connection c = DriverManager.getConnection(url, p);
     PreparedStatement s = c.prepareStatement("SELECT id, customer FROM orders WHERE amount > ?")) {
    s.setLong(1, 1000);
    try (ResultSet r = s.executeQuery()) { while (r.next()) System.out.println(r.getLong(1) + " " + r.getString(2)); }
}
```

**Production — Sample with Reconnection, Retries, and Idempotent Keys** ([examples/jdbc/](examples/jdbc/), no external dependencies, copy and use):

```java
try (JamsilClient db = JamsilClient.builder().hosts("10.0.0.1,10.0.0.2,10.0.0.3")
        .credentials(System.getenv("JAMSIL_USER"), System.getenv("JAMSIL_PASSWORD")).build()) {

    // Single statement write: 1 key per logical task. Applied once even if retried.
    db.update(JamsilClient.newRequestKey("order-1001"),
              "INSERT INTO orders VALUES (?, ?, ?)", 1001L, "kim", 5000L);

    // Multi-statement transaction: Key applies to COMMIT. If the COMMIT response was lost and the body is re-run,
    // the client treats a 23505 as "already committed" when the key was used (returns null); write bodies to be re-runnable where you can.
    db.write(JamsilClient.newRequestKey("order-1002"), true, c -> {
        try (PreparedStatement p = c.prepareStatement("INSERT INTO orders VALUES (?, ?, ?)")) {
            p.setLong(1, 1002); p.setString(2, "lee"); p.setLong(3, 7000); p.executeUpdate();
        }
        return null;
    });

    // Read: Reconnects to another node and retries if disconnected
    long n = db.read(c -> { try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT count(*) FROM orders")) { r.next(); return r.getLong(1); } });
}
```

To run: `JAMSIL_USER=… JAMSIL_PASSWORD=… examples/jdbc/run.sh 10.0.0.1,10.0.0.2,10.0.0.3` — Detailed rules and HikariCP configuration examples are in [examples/jdbc/README.md](examples/jdbc/README.md).
Even with connection pools, retries and idempotent keys remain the application's responsibility (pools only swap dead connections, they do not re-execute failed tasks).

### Python (psycopg 3)

```bash
pip install "psycopg[binary]"          # psycopg 3.x, Python 3.9+
```

**Simplest Usage**:

```python
import os, psycopg
with psycopg.connect(host="10.0.0.1", port=9786, dbname="jamsil", sslmode="require",
                     user=os.environ["JAMSIL_USER"], password=os.environ["JAMSIL_PASSWORD"],
                     connect_timeout=3, autocommit=True) as conn:
    for row in conn.execute("SELECT id, customer FROM orders WHERE amount > %s", (1000,)):
        print(row)
```

**Production Sample** ([examples/python/jamsil_client.py](examples/python/jamsil_client.py), single file, copy and use): Connects sequentially shuffling the nodes. On disconnect, reconnects to another node and retries using the same idempotent key.

```python
from jamsil_client import JamsilClient, new_request_key

with JamsilClient(["10.0.0.1", "10.0.0.2", "10.0.0.3"], os.environ["JAMSIL_USER"], os.environ["JAMSIL_PASSWORD"]) as db:
    # Single statement write — Applied once even if retried
    db.execute(new_request_key("order-1001"), "INSERT INTO orders VALUES (%s, %s, %s)", (1001, "kim", 5000))

    # RETURNING results are also perfectly reproduced on retry
    amount = db.write(new_request_key("bump"), lambda c: c.execute(
        "UPDATE orders SET amount = amount + %s WHERE id = %s RETURNING amount", (100, 1001)).fetchone()[0])

    # Multi-statement transaction — Key applies to COMMIT
    def place(c):
        c.execute("INSERT INTO orders VALUES (%s, %s, %s)", (1002, "lee", 7000))
    db.write(new_request_key("order-1002"), place, transaction=True)

    rows = db.read(lambda c: c.execute("SELECT id, customer, amount FROM orders ORDER BY id").fetchall())
```

To run: `JAMSIL_HOSTS=10.0.0.1,10.0.0.2,10.0.0.3 python examples/python/quickstart.py` — Explanations and a failover demo are in [examples/python/README.md](examples/python/README.md).

### DBeaver

Connect using the PostgreSQL driver (tested with 36 DBeaver PostgreSQL introspection queries).

1. **Database → New Database Connection → PostgreSQL** (Accept the driver download prompt if it's your first time).
2. **Main** tab — For a single node: *Connect by* **Host**, Host `10.0.0.1`, Port `9786`, Database `jamsil`, enter Username/Password.
3. **To ensure uninterrupted access during node failures, use all nodes:** Change *Connect by* to **URL** and list all nodes separated by commas in the JDBC URL (Use the URL field, not the Host field, as appending ports in the Host field can break the address).
   ```
   jdbc:postgresql://10.0.0.1:9786,10.0.0.2:9786,10.0.0.3:9786/jamsil?sslmode=require&loadBalanceHosts=true&hostRecheckSeconds=2&connectTimeout=3&socketTimeout=30
   ```
   Enter your Username/Password in the *Authentication* section.
4. **SSL** — `sslmode=require` in the URL is sufficient. If you created it in Host mode, turn on **Use SSL** in the *SSL* tab and leave *SSL mode* as `require` (the server strictly requires TLS). Menu names may vary slightly depending on your DBeaver version.
5. Save after **Test Connection**. Once connected, you can explore tables, columns, indexes, and constraints in the `jamsil` database and `public` schema, and run queries via the SQL Editor (Ctrl+Enter).

Usage Tips:
- If the node DBeaver is connected to dies, the connection is lost. Use *Invalidate/Reconnect* (Right-click → Invalidate/Reconnect) to reconnect to another node listed in the URL. In-progress transactions in the editor will be rolled back.
- Running in `Manual commit` mode can hold a static Snapshot and Writer while the transaction is open. Commit or rollback promptly (Idle transactions are released after `storage.idle_tx_timeout_ms`).
- System info views: `jamsil_catalog.cluster_nodes`, `replication_status`, `storage_status`, `search_indexes`, `active_sessions`, `metrics`, `replica_checksum` (Requires superuser).
- Tables cannot be created without a PRIMARY KEY (`JDB-5101`), and `LIKE` is rejected on TEXT columns (`JDB-4102`) — Use `TEXT_MATCH` for full-text search.

## SQL Usage Notes

```sql
-- All tables must have a PRIMARY KEY
CREATE TABLE documents (id BIGINT PRIMARY KEY, title VARCHAR(200), content TEXT);

-- Full-text search (Korean morphological analysis)
CREATE SEARCH INDEX idx_documents_content ON documents(content) WITH (analyzer = 'korean', scoring = 'bm25');
SELECT id, title, TEXT_SCORE(content) AS score
  FROM documents
 WHERE TEXT_MATCH(content, 'finance AND (stocks OR bonds)')
 ORDER BY TEXT_SCORE(content) DESC LIMIT 20;

-- Wait until the newly written document is reflected in the search
SELECT id FROM documents WHERE TEXT_MATCH(content, '"financial investment"') WITH SEARCH CONSISTENCY COMMITTED;
```

- **Supported:** SELECT/INSERT/UPDATE/DELETE/RETURNING/ON CONFLICT, JOIN, GROUP BY, Subqueries/CTEs, Views, Indexes, Limited ALTER TABLE, SAVEPOINT, Prepared Statements, `::` Casts,
  Types: SMALLINT/INTEGER/BIGINT/SERIAL, REAL/DOUBLE, BOOLEAN, CHAR/VARCHAR, TEXT(32MiB), BYTEA, DATE/TIME/TIMESTAMP/TIMESTAMPTZ(UTC), UUID, JSON/JSONB.
- **Unsupported/Limited:** `DECIMAL/NUMERIC` (exact fixed-point not implemented), Sequence functions, Triggers, Procedures, TEMP tables, `COPY`, `LISTEN/NOTIFY`. DDL is not allowed in the same transaction after DML. No timezone conversion (UTC only).
- **Isolation Levels:** Default is READ COMMITTED. REPEATABLE READ/SERIALIZABLE locks the connected node's Snapshot at the first statement. If there was another COMMIT before the first write attempt, it yields `40001` (requires retry).
  **`TEXT_MATCH`/`TEXT_SCORE` inside a fixed Snapshot transaction are rejected with `JDB-4105` (`0A000`)** — Run searches outside transactions or in READ COMMITTED.
- **Read Consistency:** Default is `linearizable` (always latest commit). `SET jamsil.read_consistency = 'local'` reads the connected node's current state immediately (may lag behind).
- **TEXT `LIKE` Prohibited:** Applies to all expressions derived from TEXT. Use `VARCHAR(n)` for values that require pattern matching.
- For detailed SQL scope, permissions, and metrics, see [docs/REFERENCE.md](docs/REFERENCE.md) §4~§6.

## Error Codes and Retry Guide

Server error messages include `JDB-xxxx` codes, and use standard SQLSTATE values.

| Code | SQLSTATE | Meaning | Client Action |
|---|---|---|---|
| `JDB-2001` | `57P03` | No Quorum / Leader Election in progress — Write & linearizable reads unavailable | Retry (with backoff) |
| `JDB-2002` | `40001` | Serialization failure | Retry the transaction from the beginning |
| `JDB-2003` | `08007` | Commit status unknown (Lost response / Timeout) | Retry **with the same Idempotent Key** |
| `JDB-4102` | | `LIKE` used on TEXT | Modify query (`TEXT_MATCH` or use VARCHAR) |
| `JDB-4104` | `54000` | Search matches exceed `search.max_candidates` | Narrow down the search terms |
| `JDB-4105` | `0A000` | `TEXT_MATCH` inside RR/SERIALIZABLE transaction | Search outside transaction / in READ COMMITTED |
| `53200` | | Insufficient memory budget for large message | Retry after a short delay |

## Testing

```bash
cargo test --workspace                       # Rust Unit/Integration tests
scripts/dev.sh standalone                    # Local 1-node (127.0.0.1:9786, jamsil/jamsil account — Dev only)
J="java -cp tests/jdbc/postgresql-42.7.4.jar"
$J tests/jdbc/JdbcConformance.java "jdbc:postgresql://127.0.0.1:9786/jamsil?sslmode=require" big   # 19 JDBC Conformance tests (+32MiB)
$J tests/jdbc/DbeaverCompat.java   "jdbc:postgresql://127.0.0.1:9786/jamsil?sslmode=require"   # 36 DBeaver introspection queries
tests/run-multihost-failover.sh 10.0.0.1,10.0.0.2,10.0.0.3   # Multi-host failover (3 VMs, requires ssh+sudo systemctl)
```

- We tested Leader/Follower forced termination, network partition, 2-node stop, full restart, node rebuild after data deletion, and exact fault injection (right before/after fsync) on 3 physical VMs (Rocky 9.8). Methods and results in [docs/REFERENCE.md](docs/REFERENCE.md) §8.
- Multi-host failover tests (5 runs): Stopped and restarted nodes one by one while continuously writing. All 472 committed writes existed across all 3 nodes without duplicates. Time to first committed write after a stop was 0.7~2.1 seconds.
- Fault injection tests apply test configurations (`scripts/vm-acceptance.sh setup|restore`) and **must not be run on production nodes.**

## Known Limitations

- Port 9787 between nodes performs authentication only, not encryption (mTLS not implemented) — Isolate the network.
- Write throughput is bottlenecked by the single cluster-wide Writer.
- `DECIMAL/NUMERIC`, Sequence functions, Triggers, Procedures, TEMP tables, `COPY`, and `LISTEN/NOTIFY` are not supported. No timezone conversion (UTC only).
- Users and roles are managed via config file (`CREATE USER`/`GRANT` unsupported). `pg_catalog` only mimics what JDBC/DBeaver requires.
- Result sets are buffered in server memory before transmission (512MiB limit per session). Simultaneous processing of 32MiB TEXTs can significantly increase `jamsil-sync` memory usage.
- A node with deleted data will be rebuilt from the majority upon restart, but **do not delete data from two nodes simultaneously.**
- For justifications regarding design decisions (custom Raft, custom PG wire implementation, etc.), see [docs/REFERENCE.md](docs/REFERENCE.md) §7.

## Repository Structure

```text
bin/                   6 Excecutables (jamsild, jamsil-sql, jamsil-store, jamsil-sync, jamsil-search, jamsil-log)
crates/                Common, IPC, SQL Translation, Storage Engine, PG Protocol, Raft, Search Engine, Observability
config/                Configuration examples for standalone.toml, cluster.toml
packaging/, scripts/   RPM (eroundb.spec) · systemd, Local execution (dev.sh) · RPM Build · VM Test scripts
examples/jdbc/         Java integration sample (Retries, Reconnection, Idempotent Keys)
examples/python/       Python (psycopg 3) integration sample
tests/jdbc, tests/vm   JDBC Conformance, Fault Injection, Regression, Failover tests
docs/REFERENCE.md      Implementation specs, Consistency contracts, SQL scope, Tests, Known limitations
docs/DESIGN.md         Design Document
```

`tests/jdbc/postgresql-42.7.4.jar` is a PostgreSQL JDBC Driver (BSD-2-Clause, <https://jdbc.postgresql.org>) included for testing and is not part of erounDB's code.

## License

[Apache License 2.0](LICENSE). The `tests/jdbc/postgresql-42.7.4.jar` (PostgreSQL JDBC, BSD-2-Clause) and dependencies downloaded via Cargo follow their respective licenses.