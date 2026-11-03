//! `pg_catalog` / `information_schema` 가상 뷰 (Reader Connection별 TEMP 뷰).
//!
//! JDBC DatabaseMetaData와 DBeaver가 조회하는 컬럼 위주로 PostgreSQL 카탈로그 형태를 흉내낸다.
//! OID는 이름 기반 안정 해시(`_jamsil_oid`)이므로 모든 노드에서 같다.

use rusqlite::Connection;

use jamsil_common::DbError;
use jamsil_sql::types::PG_TYPES;

use crate::convert::map_err;

pub fn install(conn: &Connection, database: &str, user: &str) -> Result<(), DbError> {
    let q = |s: &str| s.replace('\'', "''");
    let db = q(database);
    let usr = q(user);
    let mut sql = String::new();
    sql.push_str(&format!(
        r#"
CREATE TEMP TABLE IF NOT EXISTS pg_type (
    oid INTEGER PRIMARY KEY, typname TEXT, typnamespace INTEGER, typowner INTEGER, typlen INTEGER,
    typbyval INTEGER, typtype TEXT, typcategory TEXT, typispreferred INTEGER, typisdefined INTEGER,
    typdelim TEXT, typrelid INTEGER, typelem INTEGER, typarray INTEGER, typinput TEXT, typoutput TEXT,
    typreceive TEXT, typsend TEXT, typmodin TEXT, typmodout TEXT, typanalyze TEXT, typalign TEXT,
    typstorage TEXT, typnotnull INTEGER, typbasetype INTEGER, typtypmod INTEGER, typndims INTEGER,
    typcollation INTEGER, typdefaultbin TEXT, typdefault TEXT, typacl TEXT);
DELETE FROM temp.pg_type;

CREATE TEMP VIEW IF NOT EXISTS pg_namespace(oid, nspname, nspowner, nspacl) AS
    VALUES (11, 'pg_catalog', 10, NULL), (2200, 'public', 10, NULL),
           (13000, 'information_schema', 10, NULL), (13100, 'jamsil_catalog', 10, NULL);

CREATE TEMP VIEW IF NOT EXISTS _jm_rel AS
    SELECT name AS relname, CASE type WHEN 'table' THEN 'r' ELSE 'v' END AS relkind, sql
    FROM main.sqlite_schema
    WHERE type IN ('table', 'view') AND substr(name, 1, 7) <> '_jamsil' AND substr(name, 1, 7) <> 'sqlite_';

CREATE TEMP VIEW IF NOT EXISTS _jm_idx AS
    SELECT t.relname AS tbl, il.name AS sqlite_name,
           CASE WHEN il.origin = 'pk' THEN t.relname || '_pkey'
                WHEN substr(il.name, 1, 17) = 'sqlite_autoindex_' THEN t.relname || '_key' || il.seq
                ELSE il.name END AS relname,
           il."unique" AS indisunique, (il.origin = 'pk') AS indisprimary, il.partial AS partial
    FROM _jm_rel t, pragma_index_list(t.relname) il WHERE t.relkind = 'r'
    UNION ALL
    SELECT t.relname, NULL, t.relname || '_pkey', 1, 1, 0
    FROM _jm_rel t
    WHERE t.relkind = 'r'
      AND NOT EXISTS (SELECT 1 FROM pragma_index_list(t.relname) il WHERE il.origin = 'pk')
      AND EXISTS (SELECT 1 FROM pragma_table_info(t.relname) ti WHERE ti.pk > 0);

CREATE TEMP VIEW IF NOT EXISTS _jm_idxcol AS
    SELECT i.tbl, i.relname AS idxname, ii.seqno AS seqno, ii.cid + 1 AS attnum, ii.name AS colname
    FROM _jm_idx i, pragma_index_info(i.sqlite_name) ii WHERE i.sqlite_name IS NOT NULL AND ii.cid >= 0
    UNION ALL
    SELECT i.tbl, i.relname, ti.pk - 1, ti.cid + 1, ti.name
    FROM _jm_idx i, pragma_table_info(i.tbl) ti WHERE i.sqlite_name IS NULL AND ti.pk > 0;

CREATE TEMP VIEW IF NOT EXISTS _jm_indkeys AS
    SELECT _jamsil_oid('i', c.idxname) AS indexrelid, c.seqno + 1 AS pos, c.attnum AS attnum, c.colname AS colname
    FROM _jm_idxcol c;

CREATE TEMP VIEW IF NOT EXISTS pg_class AS
    SELECT _jamsil_oid('r', relname) AS oid, relname, 2200 AS relnamespace, 0 AS reltype, 0 AS reloftype,
           10 AS relowner, CASE relkind WHEN 'r' THEN 2 ELSE 0 END AS relam, 0 AS relfilenode,
           0 AS reltablespace, 0 AS relpages, -1.0 AS reltuples, 0 AS relallvisible, 0 AS reltoastrelid,
           EXISTS (SELECT 1 FROM _jm_idx i WHERE i.tbl = r.relname) AS relhasindex, 0 AS relisshared,
           'p' AS relpersistence, relkind,
           (SELECT count(*) FROM pragma_table_info(r.relname)) AS relnatts, 0 AS relchecks,
           (relkind = 'v') AS relhasrules, 0 AS relhastriggers, 0 AS relhassubclass, 0 AS relrowsecurity,
           0 AS relforcerowsecurity, 1 AS relispopulated, 'd' AS relreplident, 0 AS relispartition,
           0 AS relrewrite, 0 AS relfrozenxid, 0 AS relminmxid, NULL AS relacl, NULL AS reloptions,
           NULL AS relpartbound
    FROM _jm_rel r
    UNION ALL
    SELECT _jamsil_oid('i', relname), relname, 2200, 0, 0, 10, 403, 0, 0, 0, -1.0, 0, 0, 0, 0, 'p', 'i',
           (SELECT count(*) FROM _jm_idxcol c WHERE c.idxname = i.relname AND c.tbl = i.tbl), 0, 0, 0, 0,
           0, 0, 1, 'n', 0, 0, 0, 0, NULL, NULL, NULL
    FROM _jm_idx i
    UNION ALL
    SELECT 1259, 'pg_class', 11, 0, 0, 10, 2, 0, 0, 0, -1.0, 0, 0, 1, 0, 'p', 'r', 33, 0, 0, 0, 0, 0, 0, 1,
           'n', 0, 0, 0, 0, NULL, NULL, NULL;

CREATE TEMP VIEW IF NOT EXISTS pg_attribute AS
    SELECT _jamsil_oid('r', r.relname) AS attrelid, ti.name AS attname, _jamsil_decl_oid(ti.type) AS atttypid,
           -1 AS attstattarget, (SELECT typlen FROM temp.pg_type t WHERE t.oid = _jamsil_decl_oid(ti.type)) AS attlen,
           ti.cid + 1 AS attnum, 0 AS attndims, -1 AS attcacheoff, _jamsil_decl_typmod(ti.type) AS atttypmod,
           0 AS attbyval, 'x' AS attstorage, 'i' AS attalign, (ti."notnull" OR ti.pk > 0) AS attnotnull,
           (ti.dflt_value IS NOT NULL) AS atthasdef, 0 AS atthasmissing,
           CASE WHEN ti.pk = 1 AND upper(ti.type) = 'INTEGER'
                 AND (SELECT count(*) FROM pragma_table_info(r.relname) x WHERE x.pk > 0) = 1 THEN 'd' ELSE '' END AS attidentity,
           '' AS attgenerated, 0 AS attisdropped, 1 AS attislocal, 0 AS attinhcount, 0 AS attcollation,
           NULL AS attacl, NULL AS attoptions, NULL AS attfdwoptions
    FROM _jm_rel r, pragma_table_info(r.relname) ti
    UNION ALL
    SELECT _jamsil_oid('i', c.idxname), c.colname, (SELECT _jamsil_decl_oid(ti.type) FROM pragma_table_info(c.tbl) ti WHERE ti.cid + 1 = c.attnum),
           -1, -1, c.seqno + 1, 0, -1, -1, 0, 'p', 'i', 0, 0, 0, '', '', 0, 1, 0, 0, NULL, NULL, NULL
    FROM _jm_idxcol c;

CREATE TEMP VIEW IF NOT EXISTS pg_attrdef AS
    SELECT _jamsil_oid('d', r.relname || '.' || ti.name) AS oid, _jamsil_oid('r', r.relname) AS adrelid,
           ti.cid + 1 AS adnum, ti.dflt_value AS adbin, ti.dflt_value AS adsrc
    FROM _jm_rel r, pragma_table_info(r.relname) ti WHERE ti.dflt_value IS NOT NULL;

CREATE TEMP VIEW IF NOT EXISTS pg_index AS
    SELECT _jamsil_oid('i', i.relname) AS indexrelid, _jamsil_oid('r', i.tbl) AS indrelid,
           (SELECT count(*) FROM _jm_idxcol c WHERE c.idxname = i.relname AND c.tbl = i.tbl) AS indnatts,
           (SELECT count(*) FROM _jm_idxcol c WHERE c.idxname = i.relname AND c.tbl = i.tbl) AS indnkeyatts,
           i.indisunique, i.indisprimary, 0 AS indisexclusion, 1 AS indimmediate, 0 AS indisclustered,
           1 AS indisvalid, 0 AS indcheckxmin, 1 AS indisready, 1 AS indislive, 0 AS indisreplident,
           (SELECT group_concat(attnum, ' ') FROM (SELECT attnum FROM _jm_idxcol c WHERE c.idxname = i.relname AND c.tbl = i.tbl ORDER BY seqno)) AS indkey,
           NULL AS indcollation, NULL AS indclass, NULL AS indoption, NULL AS indexprs,
           CASE WHEN i.partial THEN '' ELSE NULL END AS indpred
    FROM _jm_idx i;

CREATE TEMP VIEW IF NOT EXISTS pg_constraint AS
    SELECT _jamsil_oid('c', i.relname) AS oid, i.relname AS conname, 2200 AS connamespace,
           CASE WHEN i.indisprimary THEN 'p' ELSE 'u' END AS contype, 0 AS condeferrable, 0 AS condeferred,
           1 AS convalidated, _jamsil_oid('r', i.tbl) AS conrelid, 0 AS contypid, _jamsil_oid('i', i.relname) AS conindid,
           0 AS conparentid, 0 AS confrelid, ' ' AS confupdtype, ' ' AS confdeltype, ' ' AS confmatchtype,
           1 AS conislocal, 0 AS coninhcount, 1 AS connoinherit,
           '{{' || (SELECT group_concat(attnum, ',') FROM (SELECT attnum FROM _jm_idxcol c WHERE c.idxname = i.relname AND c.tbl = i.tbl ORDER BY seqno)) || '}}' AS conkey,
           NULL AS confkey, NULL AS conbin
    FROM _jm_idx i WHERE i.indisprimary OR (i.indisunique AND (i.sqlite_name IS NULL OR substr(i.sqlite_name, 1, 17) = 'sqlite_autoindex_'))
    UNION ALL
    SELECT _jamsil_oid('f', r.relname || '.' || fk.id), r.relname || '_' || fk."from" || '_fkey', 2200, 'f', 0, 0, 1,
           _jamsil_oid('r', r.relname), 0, _jamsil_oid('i', fk."table" || '_pkey'), 0, _jamsil_oid('r', fk."table"),
           CASE fk.on_update WHEN 'CASCADE' THEN 'c' WHEN 'SET NULL' THEN 'n' WHEN 'SET DEFAULT' THEN 'd' WHEN 'RESTRICT' THEN 'r' ELSE 'a' END,
           CASE fk.on_delete WHEN 'CASCADE' THEN 'c' WHEN 'SET NULL' THEN 'n' WHEN 'SET DEFAULT' THEN 'd' WHEN 'RESTRICT' THEN 'r' ELSE 'a' END,
           's', 1, 0, 1,
           '{{' || (SELECT ti.cid + 1 FROM pragma_table_info(r.relname) ti WHERE ti.name = fk."from") || '}}',
           '{{' || coalesce((SELECT ti.cid + 1 FROM pragma_table_info(fk."table") ti WHERE ti.name = fk."to"), 1) || '}}',
           NULL
    FROM _jm_rel r, pragma_foreign_key_list(r.relname) fk WHERE r.relkind = 'r';

CREATE TEMP VIEW IF NOT EXISTS pg_database(oid, datname, datdba, encoding, datlocprovider, datistemplate,
    datallowconn, datconnlimit, datlastsysoid, datfrozenxid, datminmxid, dattablespace, datcollate, datctype, datacl) AS
    VALUES (16384, '{db}', 10, 6, 'c', 0, 1, -1, 12000, 0, 1, 1663, 'C.UTF-8', 'C.UTF-8', NULL);

CREATE TEMP VIEW IF NOT EXISTS pg_roles(oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin,
    rolreplication, rolconnlimit, rolpassword, rolvaliduntil, rolbypassrls, rolconfig) AS
    VALUES (10, '{usr}', 1, 1, 1, 1, 1, 0, -1, '********', NULL, 1, NULL);
CREATE TEMP VIEW IF NOT EXISTS pg_authid AS SELECT * FROM pg_roles;
CREATE TEMP VIEW IF NOT EXISTS pg_user(usename, usesysid, usecreatedb, usesuper, userepl, usebypassrls, passwd, valuntil, useconfig) AS
    VALUES ('{usr}', 10, 1, 1, 0, 1, '********', NULL, NULL);

CREATE TEMP VIEW IF NOT EXISTS pg_default_acl(oid, defaclrole, defaclnamespace, defaclobjtype, defaclacl) AS SELECT 0,0,0,'','' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_auth_members(oid, roleid, member, grantor, admin_option, inherit_option, set_option) AS SELECT 0,0,0,0,0,0,0 WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_available_extensions(name, default_version, installed_version, comment) AS SELECT '','','','' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_stat_user_tables AS SELECT _jamsil_oid('r', relname) AS relid, 'public' AS schemaname, relname, 0 AS seq_scan, 0 AS n_live_tup, 0 AS n_dead_tup FROM _jm_rel WHERE relkind = 'r';
CREATE TEMP VIEW IF NOT EXISTS pg_am(oid, amname, amhandler, amtype) AS VALUES (2, 'heap', 'heap_tableam_handler', 't'), (403, 'btree', 'bthandler', 'i');
CREATE TEMP VIEW IF NOT EXISTS pg_tablespace(oid, spcname, spcowner, spcacl, spcoptions) AS VALUES (1663, 'pg_default', 10, NULL, NULL);
CREATE TEMP VIEW IF NOT EXISTS pg_description(objoid, classoid, objsubid, description) AS SELECT 0, 0, 0, '' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_shdescription(objoid, classoid, description) AS SELECT 0, 0, '' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_inherits(inhrelid, inhparent, inhseqno, inhdetachpending) AS SELECT 0, 0, 0, 0 WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_trigger(oid, tgrelid, tgparentid, tgname, tgfoid, tgtype, tgenabled, tgisinternal, tgconstrrelid, tgconstrindid, tgconstraint, tgdeferrable, tginitdeferred, tgnargs, tgattr, tgargs, tgqual, tgoldtable, tgnewtable) AS SELECT 0,0,0,'',0,0,'',0,0,0,0,0,0,0,'','','','','' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_rewrite(oid, rulename, ev_class, ev_type, ev_enabled, is_instead, ev_qual, ev_action) AS SELECT 0,'',0,'','',0,'','' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_depend(classid, objid, objsubid, refclassid, refobjid, refobjsubid, deptype) AS SELECT 0,0,0,0,0,0,'' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_sequence(seqrelid, seqtypid, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle) AS SELECT 0,0,0,0,0,0,0,0 WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_extension(oid, extname, extowner, extnamespace, extrelocatable, extversion, extconfig, extcondition) AS SELECT 0,'',0,0,0,'','','' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_collation(oid, collname, collnamespace, collowner, collprovider, collisdeterministic, collencoding, collcollate, collctype) AS VALUES (100, 'default', 11, 10, 'd', 1, -1, NULL, NULL), (950, 'C', 11, 10, 'c', 1, -1, 'C', 'C');
CREATE TEMP VIEW IF NOT EXISTS pg_proc(oid, proname, pronamespace, proowner, prolang, procost, prorows, provariadic, prosupport, prokind, prosecdef, proleakproof, proisstrict, proretset, provolatile, proparallel, pronargs, pronargdefaults, prorettype, proargtypes, proallargtypes, proargmodes, proargnames, proargdefaults, protrftypes, prosrc, probin, prosqlbody, proconfig, proacl) AS
    SELECT 0,'',0,0,0,0,0,0,0,'',0,0,0,0,'','',0,0,0,'','','','','','','','','','','' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_language(oid, lanname, lanowner, lanispl, lanpltrusted, lanplcallfoid, laninline, lanvalidator, lanacl) AS VALUES (12, 'internal', 10, 0, 0, 0, 0, 0, NULL), (14, 'sql', 10, 0, 1, 0, 0, 0, NULL);
CREATE TEMP VIEW IF NOT EXISTS pg_event_trigger(oid, evtname, evtevent, evtowner, evtfoid, evtenabled, evttags) AS SELECT 0,'','',0,0,'','' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_foreign_server(oid, srvname) AS SELECT 0,'' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_foreign_data_wrapper(oid, fdwname) AS SELECT 0,'' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_foreign_table(ftrelid, ftserver, ftoptions) AS SELECT 0,0,'' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_partitioned_table(partrelid, partstrat, partnatts, partdefid, partattrs) AS SELECT 0,'',0,0,'' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_policy(oid, polname, polrelid) AS SELECT 0,'',0 WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_statistic_ext(oid, stxrelid, stxname) AS SELECT 0,0,'' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_cast(oid, castsource, casttarget, castfunc, castcontext, castmethod) AS SELECT 0,0,0,0,'','' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_operator(oid, oprname, oprnamespace) AS SELECT 0,'',0 WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_opclass(oid, opcmethod, opcname, opcnamespace, opcowner, opcfamily, opcintype, opcdefault, opckeytype) AS SELECT 0,0,'',0,0,0,0,0,0 WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_aggregate(aggfnoid, aggkind) AS SELECT 0,'' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_enum(oid, enumtypid, enumsortorder, enumlabel) AS SELECT 0,0,0,'' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_range(rngtypid, rngsubtype) AS SELECT 0,0 WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_locks(locktype, database, relation, pid, mode, granted) AS SELECT '',0,0,0,'',0 WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS pg_stat_activity(datid, datname, pid, usesysid, usename, application_name, client_addr, client_port, backend_start, xact_start, query_start, state_change, wait_event_type, wait_event, state, backend_xid, backend_xmin, query, backend_type) AS
    VALUES (16384, '{db}', 0, 10, '{usr}', '', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'active', NULL, NULL, '', 'client backend');
CREATE TEMP VIEW IF NOT EXISTS pg_settings(name, setting, unit, category, short_desc, extra_desc, context, vartype, source, min_val, max_val, enumvals, boot_val, reset_val, sourcefile, sourceline, pending_restart) AS
    VALUES ('server_version', '14.0', NULL, 'Preset Options', 'Server version', NULL, 'internal', 'string', 'default', NULL, NULL, NULL, '14.0', '14.0', NULL, NULL, 0),
           ('server_encoding', 'UTF8', NULL, 'Preset Options', 'Server encoding', NULL, 'internal', 'string', 'default', NULL, NULL, NULL, 'UTF8', 'UTF8', NULL, NULL, 0),
           ('max_identifier_length', '63', NULL, 'Preset Options', '', NULL, 'internal', 'integer', 'default', NULL, NULL, NULL, '63', '63', NULL, NULL, 0),
           ('standard_conforming_strings', 'on', NULL, 'Version and Platform Compatibility', '', NULL, 'user', 'bool', 'default', NULL, NULL, NULL, 'on', 'on', NULL, NULL, 0),
           ('TimeZone', 'UTC', NULL, 'Client Connection Defaults', '', NULL, 'user', 'string', 'default', NULL, NULL, NULL, 'UTC', 'UTC', NULL, NULL, 0),
           ('max_index_keys', '32', NULL, 'Preset Options', '', NULL, 'internal', 'integer', 'default', NULL, NULL, NULL, '32', '32', NULL, NULL, 0),
           ('max_function_args', '100', NULL, 'Preset Options', '', NULL, 'internal', 'integer', 'default', NULL, NULL, NULL, '100', '100', NULL, NULL, 0),
           ('block_size', '8192', NULL, 'Preset Options', '', NULL, 'internal', 'integer', 'default', NULL, NULL, NULL, '8192', '8192', NULL, NULL, 0),
           ('integer_datetimes', 'on', NULL, 'Preset Options', '', NULL, 'internal', 'bool', 'default', NULL, NULL, NULL, 'on', 'on', NULL, NULL, 0),
           ('DateStyle', 'ISO, MDY', NULL, 'Client Connection Defaults', '', NULL, 'user', 'string', 'default', NULL, NULL, NULL, 'ISO, MDY', 'ISO, MDY', NULL, NULL, 0),
           ('max_connections', '1000', NULL, 'Connections and Authentication', '', NULL, 'postmaster', 'integer', 'default', NULL, NULL, NULL, '1000', '1000', NULL, NULL, 0),
           ('search_path', 'public', NULL, 'Client Connection Defaults', '', NULL, 'user', 'string', 'default', NULL, NULL, NULL, 'public', 'public', NULL, NULL, 0);

CREATE TEMP VIEW IF NOT EXISTS pg_tables AS
    SELECT 'public' AS schemaname, relname AS tablename, '{usr}' AS tableowner, NULL AS tablespace,
           EXISTS (SELECT 1 FROM _jm_idx i WHERE i.tbl = r.relname) AS hasindexes, 0 AS hasrules, 0 AS hastriggers, 0 AS rowsecurity
    FROM _jm_rel r WHERE relkind = 'r';
CREATE TEMP VIEW IF NOT EXISTS pg_views AS
    SELECT 'public' AS schemaname, relname AS viewname, '{usr}' AS viewowner, sql AS definition FROM _jm_rel WHERE relkind = 'v';
CREATE TEMP VIEW IF NOT EXISTS pg_indexes AS
    SELECT 'public' AS schemaname, i.tbl AS tablename, i.relname AS indexname, NULL AS tablespace,
           coalesce((SELECT s.sql FROM main.sqlite_schema s WHERE s.name = i.sqlite_name),
                    'CREATE UNIQUE INDEX ' || i.relname || ' ON public.' || i.tbl || ' USING btree (' ||
                    (SELECT group_concat(colname, ', ') FROM (SELECT colname FROM _jm_idxcol c WHERE c.idxname = i.relname AND c.tbl = i.tbl ORDER BY seqno)) || ')') AS indexdef
    FROM _jm_idx i;
CREATE TEMP VIEW IF NOT EXISTS pg_matviews(schemaname, matviewname, matviewowner, tablespace, hasindexes, ispopulated, definition) AS SELECT '','','','',0,0,'' WHERE 0;

CREATE TEMP VIEW IF NOT EXISTS information_schema_schemata(catalog_name, schema_name, schema_owner, default_character_set_catalog, default_character_set_schema, default_character_set_name, sql_path) AS
    VALUES ('{db}', 'public', '{usr}', NULL, NULL, NULL, NULL), ('{db}', 'pg_catalog', '{usr}', NULL, NULL, NULL, NULL),
           ('{db}', 'information_schema', '{usr}', NULL, NULL, NULL, NULL), ('{db}', 'jamsil_catalog', '{usr}', NULL, NULL, NULL, NULL);
CREATE TEMP VIEW IF NOT EXISTS information_schema_tables AS
    SELECT '{db}' AS table_catalog, 'public' AS table_schema, relname AS table_name,
           CASE relkind WHEN 'r' THEN 'BASE TABLE' ELSE 'VIEW' END AS table_type,
           NULL AS self_referencing_column_name, NULL AS reference_generation, NULL AS user_defined_type_catalog,
           NULL AS user_defined_type_schema, NULL AS user_defined_type_name,
           CASE relkind WHEN 'r' THEN 'YES' ELSE 'NO' END AS is_insertable_into, 'NO' AS is_typed, NULL AS commit_action
    FROM _jm_rel;
CREATE TEMP VIEW IF NOT EXISTS information_schema_columns AS
    SELECT '{db}' AS table_catalog, 'public' AS table_schema, r.relname AS table_name, ti.name AS column_name,
           ti.cid + 1 AS ordinal_position, ti.dflt_value AS column_default,
           CASE WHEN ti."notnull" OR ti.pk > 0 THEN 'NO' ELSE 'YES' END AS is_nullable,
           _jamsil_format_decl(ti.type) AS data_type,
           CASE WHEN _jamsil_decl_typmod(ti.type) > 4 THEN _jamsil_decl_typmod(ti.type) - 4 ELSE NULL END AS character_maximum_length,
           NULL AS character_octet_length, NULL AS numeric_precision, NULL AS numeric_precision_radix, NULL AS numeric_scale,
           NULL AS datetime_precision, NULL AS interval_type, NULL AS interval_precision,
           NULL AS character_set_catalog, NULL AS character_set_schema, NULL AS character_set_name,
           NULL AS collation_catalog, NULL AS collation_schema, NULL AS collation_name,
           NULL AS domain_catalog, NULL AS domain_schema, NULL AS domain_name,
           '{db}' AS udt_catalog, 'pg_catalog' AS udt_schema, _jamsil_type_name(_jamsil_decl_oid(ti.type)) AS udt_name,
           NULL AS scope_catalog, NULL AS scope_schema, NULL AS scope_name, NULL AS maximum_cardinality,
           ti.cid + 1 AS dtd_identifier, 'NO' AS is_self_referencing, 'NO' AS is_identity, NULL AS identity_generation,
           NULL AS identity_start, NULL AS identity_increment, NULL AS identity_maximum, NULL AS identity_minimum,
           NULL AS identity_cycle, 'NEVER' AS is_generated, NULL AS generation_expression,
           CASE r.relkind WHEN 'r' THEN 'YES' ELSE 'NO' END AS is_updatable
    FROM _jm_rel r, pragma_table_info(r.relname) ti;
CREATE TEMP VIEW IF NOT EXISTS information_schema_table_constraints AS
    SELECT '{db}' AS constraint_catalog, 'public' AS constraint_schema, conname AS constraint_name,
           '{db}' AS table_catalog, 'public' AS table_schema,
           (SELECT relname FROM pg_class c WHERE c.oid = k.conrelid) AS table_name,
           CASE contype WHEN 'p' THEN 'PRIMARY KEY' WHEN 'u' THEN 'UNIQUE' WHEN 'f' THEN 'FOREIGN KEY' ELSE 'CHECK' END AS constraint_type,
           'NO' AS is_deferrable, 'NO' AS initially_deferred, 'YES' AS enforced, NULL AS nulls_distinct
    FROM pg_constraint k;
CREATE TEMP VIEW IF NOT EXISTS information_schema_key_column_usage AS
    SELECT '{db}' AS constraint_catalog, 'public' AS constraint_schema,
           CASE WHEN i.indisprimary THEN i.tbl || '_pkey' ELSE i.relname END AS constraint_name,
           '{db}' AS table_catalog, 'public' AS table_schema, c.tbl AS table_name, c.colname AS column_name,
           c.seqno + 1 AS ordinal_position, NULL AS position_in_unique_constraint
    FROM _jm_idx i JOIN _jm_idxcol c ON c.idxname = i.relname AND c.tbl = i.tbl
    WHERE i.indisprimary OR i.indisunique;
CREATE TEMP VIEW IF NOT EXISTS information_schema_views AS
    SELECT '{db}' AS table_catalog, 'public' AS table_schema, relname AS table_name, sql AS view_definition,
           'NONE' AS check_option, 'NO' AS is_updatable, 'NO' AS is_insertable_into, 'NO' AS is_trigger_updatable,
           'NO' AS is_trigger_deletable, 'NO' AS is_trigger_insertable_into
    FROM _jm_rel WHERE relkind = 'v';
CREATE TEMP VIEW IF NOT EXISTS information_schema_routines(specific_catalog, specific_schema, specific_name, routine_catalog, routine_schema, routine_name, routine_type, data_type) AS SELECT '','','','','','','','' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS information_schema_sequences(sequence_catalog, sequence_schema, sequence_name, data_type) AS SELECT '','','','' WHERE 0;
CREATE TEMP VIEW IF NOT EXISTS information_schema_character_sets(character_set_catalog, character_set_schema, character_set_name, character_repertoire, form_of_use, default_collate_catalog, default_collate_schema, default_collate_name) AS
    VALUES (NULL, NULL, 'UTF8', 'UCS', 'UTF8', '{db}', 'pg_catalog', 'C.UTF-8');
"#
    ));
    conn.execute_batch(&sql).map_err(map_err)?;
    {
        let mut st = conn
            .prepare(
                "INSERT OR REPLACE INTO temp.pg_type VALUES (?1, ?2, 11, 10, ?3, ?4, ?5, ?6, 0, 1, ',', 0, ?7, ?8, \
                 '', '', '', '', '-', '-', '-', 'i', 'p', 0, 0, -1, 0, 0, NULL, NULL, NULL)",
            )
            .map_err(map_err)?;
        for (oid, name, len, cat, arr) in PG_TYPES {
            let is_array = *cat == 'A';
            let elem = if is_array {
                PG_TYPES
                    .iter()
                    .find(|t| t.4 == *oid)
                    .map(|t| t.0)
                    .unwrap_or(0)
            } else {
                0
            };
            st.execute(rusqlite::params![
                *oid as i64,
                *name,
                *len as i64,
                (*len > 0 && *len <= 8) as i64,
                if *cat == 'P' { "p" } else { "b" },
                cat.to_string(),
                elem as i64,
                *arr as i64
            ])
            .map_err(map_err)?;
        }
    }
    Ok(())
}
