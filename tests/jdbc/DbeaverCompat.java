// DBeaver PostgreSQL 플러그인 탐색 질의 호환 시험 (DBeaver devel 소스의 질의를 같은 바인딩으로 실행).
//   java -cp postgresql-42.7.4.jar DbeaverCompat.java <jdbc-url>
import java.sql.*;
import java.util.*;

public class DbeaverCompat {
    static int pass = 0, fail = 0;

    static void q(Connection c, String name, String sql, Object... params) {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                if (params[i] instanceof Long l) ps.setLong(i + 1, l);
                else ps.setString(i + 1, params[i].toString());
            }
            int rows = 0, cols;
            try (ResultSet r = ps.executeQuery()) {
                cols = r.getMetaData().getColumnCount();
                while (r.next()) {
                    for (int i = 1; i <= cols; i++) r.getObject(i);
                    rows++;
                }
            }
            pass++;
            System.out.printf("PASS %-28s rows=%-4d cols=%d%n", name, rows, cols);
        } catch (SQLException e) {
            fail++;
            System.out.printf("FAIL %-28s %s%n", name, e.getMessage().replace('\n', ' '));
        }
    }

    public static void main(String[] a) throws Exception {
        Properties p = new Properties();
        p.setProperty("user", System.getenv().getOrDefault("JAMSIL_USER", "jamsil"));
        p.setProperty("password", System.getenv().getOrDefault("JAMSIL_PASSWORD", "jamsil"));
        Connection c = DriverManager.getConnection(a[0], p);
        try (Statement s = c.createStatement()) {
            s.execute("DROP VIEW IF EXISTS dbv_view");
            s.execute("DROP TABLE IF EXISTS dbv_child");
            s.execute("DROP TABLE IF EXISTS dbv_parent");
            s.execute("CREATE TABLE dbv_parent (id BIGSERIAL PRIMARY KEY, name VARCHAR(50) NOT NULL UNIQUE, note TEXT, created TIMESTAMPTZ DEFAULT now())");
            s.execute("CREATE TABLE dbv_child (id BIGINT PRIMARY KEY, parent_id BIGINT REFERENCES dbv_parent(id), qty INT CHECK (qty >= 0))");
            s.execute("CREATE INDEX dbv_child_qty ON dbv_child (qty)");
            s.execute("CREATE VIEW dbv_view AS SELECT id, name FROM dbv_parent");
        }
        long ns = 2200L;
        long parentOid;
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT oid FROM pg_catalog.pg_class WHERE relname = 'dbv_parent'")) {
            r.next();
            parentOid = r.getLong(1);
        }
        // 연결 부트스트랩
        q(c, "current_database", "SELECT current_database()");
        q(c, "version", "SELECT version()");
        q(c, "settings(name)", "select * from pg_catalog.pg_settings where name=?", "standard_conforming_strings");
        q(c, "settings(all)", "select * from pg_catalog.pg_settings");
        q(c, "databases", "SELECT db.oid,db.* FROM pg_catalog.pg_database db WHERE 1 = 1 AND datallowconn AND NOT datistemplate OR db.datname =?", "jamsil");
        q(c, "database by name", "SELECT db.oid,db.* FROM pg_catalog.pg_database db WHERE datname=?", "jamsil");
        q(c, "template dbs", "SELECT db.datname FROM pg_catalog.pg_database db WHERE datistemplate");
        q(c, "db size", "SELECT db.datname,pg_database_size(db.oid) FROM pg_catalog.pg_database db WHERE db.datname=?", "jamsil");
        q(c, "shdescription", "select description from pg_shdescription where objoid=? and classoid='pg_database'::regclass", 16384L);
        q(c, "keywords", "select string_agg(word, ',') from pg_catalog.pg_get_keywords() where word <> ALL ('{a,abs}'::text[])");
        // 스키마·타입·역할
        q(c, "schemas", "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n\nLEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass\n ORDER BY nspname");
        q(c, "data types", "SELECT t.oid,t.*,c.relkind,format_type(nullif(t.typbasetype, 0), t.typtypmod) as base_type_name, d.description\nFROM pg_catalog.pg_type t\nLEFT OUTER JOIN pg_catalog.pg_type et ON et.oid=t.typelem \nLEFT OUTER JOIN pg_catalog.pg_class c ON c.oid=t.typrelid\nLEFT OUTER JOIN pg_catalog.pg_description d ON t.oid=d.objoid\nWHERE t.typname IS NOT NULL\nAND (c.relkind IS NULL OR c.relkind = 'c') AND (et.typcategory IS NULL OR et.typcategory <> 'C')");
        q(c, "roles", "SELECT a.oid,a.*,pd.description FROM pg_catalog.pg_roles a \nleft join pg_catalog.pg_shdescription pd on a.oid = pd.objoid\nORDER BY a.rolname");
        q(c, "access methods", "SELECT am.oid,am.* FROM pg_catalog.pg_am am \nORDER BY am.oid");
        q(c, "collations", "SELECT c.oid,c.* FROM pg_catalog.pg_collation c \nORDER BY c.oid");
        q(c, "languages", "SELECT l.oid,l.* FROM pg_catalog.pg_language l \nORDER BY l.oid");
        q(c, "extensions", "SELECT e.oid,e.*,n.nspname FROM pg_catalog.pg_extension e LEFT OUTER JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace ORDER BY e.oid");
        q(c, "event triggers", "SELECT pet.*, d.description FROM pg_catalog.pg_event_trigger pet\nLEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid = pet.oid\nORDER BY pet.evtname");
        q(c, "enums", "SELECT * FROM pg_catalog.pg_enum");
        // 테이블 탐색
        q(c, "tables", "SELECT c.oid,c.*,d.description,pg_catalog.pg_get_expr(c.relpartbound, c.oid) as partition_expr,  pg_catalog.pg_get_partkeydef(c.oid) as partition_key \nFROM pg_catalog.pg_class c\nLEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_class'::regclass\nWHERE c.relnamespace=? AND c.relkind not in ('i','I','c')", ns);
        q(c, "table by name", "SELECT c.oid,c.*,d.description,pg_catalog.pg_get_expr(c.relpartbound, c.oid) as partition_expr,  pg_catalog.pg_get_partkeydef(c.oid) as partition_key \nFROM pg_catalog.pg_class c\nLEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_class'::regclass\nWHERE c.relnamespace=? AND c.relkind not in ('i','I','c') AND relname=?", ns, "dbv_parent");
        q(c, "columns(schema)", "SELECT c.relname,a.*,pg_catalog.pg_get_expr(ad.adbin, ad.adrelid, true) as def_value,dsc.description,dep.objid\nFROM pg_catalog.pg_attribute a\nINNER JOIN pg_catalog.pg_class c ON (a.attrelid=c.oid)\nLEFT OUTER JOIN pg_catalog.pg_attrdef ad ON (a.attrelid=ad.adrelid AND a.attnum = ad.adnum)\nLEFT OUTER JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)\nLEFT OUTER JOIN pg_depend dep on dep.refobjid = a.attrelid AND dep.deptype = 'i' and dep.refobjsubid = a.attnum and dep.classid = dep.refclassid\nWHERE NOT a.attisdropped AND c.relkind not in ('i','I','c') AND c.relnamespace=?\nORDER BY a.attnum", ns);
        q(c, "columns(table)", "SELECT c.relname,a.*,pg_catalog.pg_get_expr(ad.adbin, ad.adrelid, true) as def_value,dsc.description,dep.objid\nFROM pg_catalog.pg_attribute a\nINNER JOIN pg_catalog.pg_class c ON (a.attrelid=c.oid)\nLEFT OUTER JOIN pg_catalog.pg_attrdef ad ON (a.attrelid=ad.adrelid AND a.attnum = ad.adnum)\nLEFT OUTER JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)\nLEFT OUTER JOIN pg_depend dep on dep.refobjid = a.attrelid AND dep.deptype = 'i' and dep.refobjsubid = a.attnum and dep.classid = dep.refclassid\nWHERE NOT a.attisdropped AND c.relkind not in ('i','I','c') AND c.oid=?\nORDER BY a.attnum", parentOid);
        q(c, "constraints", "SELECT c.oid,c.*,t.relname as tabrelname,rt.relnamespace as refnamespace,d.description, case when c.contype='c' then \"substring\"(pg_get_constraintdef(c.oid), 7) else null end consrc_copy\nFROM pg_catalog.pg_constraint c\nINNER JOIN pg_catalog.pg_class t ON t.oid=c.conrelid\nLEFT OUTER JOIN pg_catalog.pg_class rt ON rt.oid=c.confrelid\nLEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_constraint'::regclass\nWHERE t.relnamespace=?\nORDER BY c.oid", ns);
        q(c, "constraint def", "SELECT pg_catalog.pg_get_constraintdef(?)", parentOid);
        q(c, "indexes", "SELECT i.*,i.indkey as keys,c.relname,c.relnamespace,c.relam,c.reltablespace,tc.relname as tabrelname,dsc.description,pg_catalog.pg_get_expr(i.indpred, i.indrelid) as pred_expr,pg_catalog.pg_get_expr(i.indexprs, i.indrelid, true) as expr,pg_catalog.pg_relation_size(i.indexrelid) as index_rel_size,pg_catalog.pg_stat_get_numscans(i.indexrelid) as index_num_scans\nFROM pg_catalog.pg_index i\nINNER JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid\nINNER JOIN pg_catalog.pg_class tc ON tc.oid=i.indrelid\nLEFT OUTER JOIN pg_catalog.pg_description dsc ON i.indexrelid=dsc.objoid\nWHERE  c.relnamespace=? ORDER BY tabrelname, c.relname", ns);
        q(c, "index def", "SELECT pg_catalog.pg_get_indexdef(?)", parentOid);
        q(c, "index column expr", "select pg_catalog.pg_get_indexdef(?, ?, true)", parentOid, 1L);
        q(c, "triggers", "SELECT x.oid,x.*,p.pronamespace as func_schema_id,d.description\nFROM pg_catalog.pg_trigger x\nINNER JOIN pg_catalog.pg_proc p ON p.oid=x.tgfoid\nLEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=x.oid\nWHERE x.tgrelid=? AND NOT x.tgisinternal", parentOid);
        q(c, "rules", "SELECT r.oid,r.*, pg_get_ruledef(r.oid) AS definition\nFROM pg_rewrite r\nWHERE r.ev_class=? AND r.rulename <> '_RETURN'::name", parentOid);
        q(c, "procedures", "SELECT p.oid as poid,p.*,d.description FROM pg_catalog.pg_proc p LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=p.oid WHERE p.pronamespace=? ORDER BY p.proname", ns);
        q(c, "view definition", "SELECT pg_catalog.pg_get_viewdef(?, true)", parentOid);
        q(c, "table statistics", "select c.oid,pg_catalog.pg_total_relation_size(c.oid) as total_rel_size,pg_catalog.pg_relation_size(c.oid) as rel_size FROM pg_class c WHERE c.relnamespace=?", ns);
        q(c, "default acl", "SELECT * FROM pg_default_acl WHERE defaclnamespace = ?", ns);
        q(c, "auth members", "SELECT * FROM pg_catalog.pg_auth_members WHERE member=?", 10L);
        q(c, "data editor read", "SELECT x.* FROM public.dbv_parent x LIMIT 200");
        System.out.printf("%nRESULT: %d passed, %d failed%n", pass, fail);
        System.exit(fail == 0 ? 0 : 1);
    }
}
