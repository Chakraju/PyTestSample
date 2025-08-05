SELECT tc.constraint_name
FROM information_schema.table_constraints tc
WHERE tc.constraint_type = 'UNIQUE'
  AND tc.table_schema = %s AND tc.table_name = %s;