SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = %s AND table_name = %s
ORDER BY ordinal_position;