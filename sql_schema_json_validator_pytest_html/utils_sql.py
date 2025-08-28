from sqlalchemy import text

SQL_LIST_TABLES = text("""
SELECT t.table_schema, t.table_name
FROM information_schema.tables t
WHERE t.table_type='BASE TABLE'
  AND t.table_schema = ANY(:include_schemas)
  AND t.table_schema NOT IN :exclude_schemas
  AND t.table_name LIKE :table_like
ORDER BY 1,2
""")

SQL_LIST_COLUMNS = text("""
SELECT c.column_name,
       c.is_nullable,
       c.data_type,
       c.udt_name,
       c.character_maximum_length,
       c.numeric_precision,
       c.numeric_scale,
       c.datetime_precision,
       c.column_default,
       c.is_identity
FROM information_schema.columns c
WHERE c.table_schema=:schema AND c.table_name=:table
ORDER BY c.ordinal_position
""")

SQL_PK = text("""
SELECT tc.constraint_name,
       array_agg(kcu.column_name ORDER BY kcu.ordinal_position) AS colnames
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
 AND tc.table_schema = kcu.table_schema
 AND tc.table_name = kcu.table_name
WHERE tc.table_schema=:schema AND tc.table_name=:table AND tc.constraint_type='PRIMARY KEY'
GROUP BY tc.constraint_name
""")

SQL_UNIQUES = text("""
SELECT tc.constraint_name,
       array_agg(kcu.column_name ORDER BY kcu.ordinal_position) AS colnames
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
 AND tc.table_schema = kcu.table_schema
 AND tc.table_name = kcu.table_name
WHERE tc.table_schema=:schema AND tc.table_name=:table AND tc.constraint_type='UNIQUE'
GROUP BY tc.constraint_name
ORDER BY tc.constraint_name
""")

SQL_FKS = text("""
SELECT
  tc.constraint_name,
  kcu.column_name,
  ccu.table_schema AS foreign_table_schema,
  ccu.table_name AS foreign_table_name,
  ccu.column_name AS foreign_column_name,
  kcu.ordinal_position
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
 AND tc.table_schema = kcu.table_schema
 AND tc.table_name = kcu.table_name
JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
WHERE tc.table_schema = :schema
  AND tc.table_name = :table
  AND tc.constraint_type = 'FOREIGN KEY'
ORDER BY tc.constraint_name, kcu.ordinal_position
""")

SQL_VIEWS = text("""
SELECT table_schema, table_name, pg_get_viewdef(format('%I.%I', table_schema, table_name)::regclass, true) AS definition
FROM information_schema.views
WHERE table_schema = ANY(:include_schemas)
  AND table_schema NOT IN :exclude_schemas
ORDER BY 1,2
""")

SQL_FUNCTIONS = text("""
SELECT n.nspname AS schema,
       p.proname AS name,
       pg_get_function_identity_arguments(p.oid) AS args,
       pg_catalog.pg_get_function_result(p.oid) AS returns,
       l.lanname AS language,
       pg_get_functiondef(p.oid) AS definition
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
JOIN pg_language l ON p.prolang = l.oid
WHERE n.nspname = ANY(:include_schemas)
  AND n.nspname NOT IN :exclude_schemas
ORDER BY 1,2,3
""")

SQL_ROLES = text("""
SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication
FROM pg_roles
ORDER BY 1
""")

SQL_ROLE_MEMBERS = text("""
SELECT r.rolname AS role, m.rolname AS member
FROM pg_auth_members am
JOIN pg_roles r ON r.oid = am.roleid
JOIN pg_roles m ON m.oid = am.member
ORDER BY 1,2
""")

SQL_SEQUENCES = text("""
SELECT sequence_schema, sequence_name, data_type, start_value, minimum_value, maximum_value, increment, cycle_option
FROM information_schema.sequences
WHERE sequence_schema = ANY(:include_schemas)
  AND sequence_schema NOT IN :exclude_schemas
ORDER BY 1,2
""")

SQL_SEQUENCE_OWNED_BY = text("""
SELECT seq_ns.nspname AS schema_name,
       seq.relname AS sequence_name,
       tbl_ns.nspname AS table_schema,
       tbl.relname AS table_name,
       att.attname  AS column_name
FROM pg_class seq
JOIN pg_namespace seq_ns ON seq.relnamespace = seq_ns.oid
LEFT JOIN pg_depend d ON d.objid = seq.oid AND d.deptype = 'a'
LEFT JOIN pg_class tbl ON d.refobjid = tbl.oid
LEFT JOIN pg_namespace tbl_ns ON tbl.relnamespace = tbl_ns.oid
LEFT JOIN pg_attribute att ON d.refobjid = att.attrelid AND d.refobjsubid = att.attnum
WHERE seq.relkind = 'S'
ORDER BY 1,2
""")

SQL_INDEXES = text("""
SELECT schemaname, tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname = ANY(:include_schemas)
  AND schemaname NOT IN :exclude_schemas
ORDER BY 1,2,3
""")

SQL_TRIGGERS = text("""
SELECT event_object_schema AS table_schema,
       event_object_table  AS table_name,
       trigger_schema,
       trigger_name,
       action_timing,
       event_manipulation,
       action_statement
FROM information_schema.triggers
WHERE event_object_schema = ANY(:include_schemas)
  AND event_object_schema NOT IN :exclude_schemas
ORDER BY 1,2,4,6
""")

SQL_TABLE_OWNERS = text("""
SELECT n.nspname AS schema,
       c.relname  AS table,
       r.rolname  AS owner
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_roles r ON r.oid = c.relowner
WHERE c.relkind = 'r'
  AND n.nspname = ANY(:include_schemas)
  AND n.nspname NOT IN :exclude_schemas
ORDER BY 1,2
""")

SQL_TABLE_PRIVILEGES = text("""
SELECT table_schema, table_name, grantee, privilege_type, is_grantable
FROM information_schema.table_privileges
WHERE table_schema = ANY(:include_schemas)
  AND table_schema NOT IN :exclude_schemas
ORDER BY 1,2,3,4
""")
