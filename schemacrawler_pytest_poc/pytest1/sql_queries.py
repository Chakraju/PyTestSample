#!/usr/bin/env python3
"""
SQL Queries Helper - Contains all SQL queries for database schema extraction
Separated from main logic for better maintainability and testing
"""

class SchemaQueries:
    """Collection of SQL queries for extracting database schema information"""
    
    @staticmethod
    def get_tables_query():
        """SQL query to extract table information with columns and constraints"""
        return """
        SELECT 
            t.table_schema,
            t.table_name,
            t.table_type,
            c.column_name,
            c.ordinal_position,
            c.column_default,
            c.is_nullable,
            c.data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            tc.constraint_name,
            tc.constraint_type
        FROM information_schema.tables t
        LEFT JOIN information_schema.columns c 
            ON t.table_name = c.table_name 
            AND t.table_schema = c.table_schema
        LEFT JOIN information_schema.key_column_usage kcu 
            ON c.table_name = kcu.table_name 
            AND c.column_name = kcu.column_name 
            AND c.table_schema = kcu.table_schema
        LEFT JOIN information_schema.table_constraints tc 
            ON kcu.constraint_name = tc.constraint_name 
            AND kcu.table_schema = tc.table_schema
        WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY t.table_schema, t.table_name, c.ordinal_position;
        """
    
    @staticmethod
    def get_views_query():
        """SQL query to extract view information with columns"""
        return """
        SELECT 
            v.table_schema,
            v.table_name as view_name,
            v.view_definition,
            c.column_name,
            c.ordinal_position,
            c.data_type,
            c.is_nullable
        FROM information_schema.views v
        LEFT JOIN information_schema.columns c 
            ON v.table_name = c.table_name 
            AND v.table_schema = c.table_schema
        WHERE v.table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY v.table_schema, v.table_name, c.ordinal_position;
        """
    
    @staticmethod
    def get_functions_query():
        """SQL query to extract function and procedure information"""
        return """
        SELECT 
            n.nspname as schema_name,
            p.proname as function_name,
            pg_catalog.pg_get_function_result(p.oid) as return_type,
            pg_catalog.pg_get_function_arguments(p.oid) as arguments,
            pg_catalog.pg_get_functiondef(p.oid) as definition,
            CASE 
                WHEN p.prokind = 'f' THEN 'function'
                WHEN p.prokind = 'p' THEN 'procedure'
                WHEN p.prokind = 'a' THEN 'aggregate'
                WHEN p.prokind = 'w' THEN 'window'
                ELSE 'unknown'
            END as function_type,
            l.lanname as language
        FROM pg_catalog.pg_proc p
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        LEFT JOIN pg_catalog.pg_language l ON l.oid = p.prolang
        WHERE n.nspname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY n.nspname, p.proname;
        """
    
    @staticmethod
    def get_indexes_query():
        """SQL query to extract index information"""
        return """
        SELECT 
            schemaname,
            tablename,
            indexname,
            indexdef
        FROM pg_indexes 
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY schemaname, tablename, indexname;
        """
    
    @staticmethod
    def get_sequences_query():
        """SQL query to extract sequence information"""
        return """
        SELECT 
            sequence_schema,
            sequence_name,
            data_type,
            numeric_precision,
            numeric_scale,
            start_value,
            minimum_value,
            maximum_value,
            increment,
            cycle_option
        FROM information_schema.sequences
        WHERE sequence_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY sequence_schema, sequence_name;
        """
    
    @staticmethod
    def get_triggers_query():
        """SQL query to extract trigger information"""
        return """
        SELECT 
            trigger_schema,
            trigger_name,
            event_manipulation,
            event_object_schema,
            event_object_table,
            action_orientation,
            action_timing,
            action_condition,
            action_statement
        FROM information_schema.triggers
        WHERE trigger_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY trigger_schema, trigger_name;
        """
    
    @staticmethod
    def get_constraints_query():
        """SQL query to extract detailed constraint information"""
        return """
        SELECT 
            tc.constraint_schema,
            tc.constraint_name,
            tc.constraint_type,
            tc.table_schema,
            tc.table_name,
            kcu.column_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            rc.update_rule,
            rc.delete_rule
        FROM information_schema.table_constraints tc
        LEFT JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        LEFT JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name
            AND tc.table_schema = ccu.table_schema
        LEFT JOIN information_schema.referential_constraints rc
            ON tc.constraint_name = rc.constraint_name
            AND tc.table_schema = rc.constraint_schema
        WHERE tc.table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY tc.table_schema, tc.table_name, tc.constraint_name;
        """
    
    @staticmethod
    def get_column_privileges_query():
        """SQL query to extract column privilege information"""
        return """
        SELECT 
            table_schema,
            table_name,
            column_name,
            privilege_type,
            grantee,
            grantor,
            is_grantable
        FROM information_schema.column_privileges
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name, column_name, privilege_type;
        """
    
    @staticmethod
    def get_table_privileges_query():
        """SQL query to extract table privilege information"""
        return """
        SELECT 
            table_schema,
            table_name,
            privilege_type,
            grantee,
            grantor,
            is_grantable
        FROM information_schema.table_privileges
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name, privilege_type;
        """
    
    @staticmethod
    def get_database_info_query():
        """SQL query to extract database metadata"""
        return """
        SELECT 
            current_database() as database_name,
            current_user as current_user,
            session_user as session_user,
            version() as postgres_version,
            current_setting('server_version') as server_version,
            current_setting('server_encoding') as encoding,
            current_setting('lc_collate') as collation,
            current_setting('lc_ctype') as ctype;
        """
    
    @staticmethod
    def get_schema_info_query():
        """SQL query to extract schema information"""
        return """
        SELECT 
            schema_name,
            schema_owner
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
        ORDER BY schema_name;
        """
    
    @staticmethod
    def get_user_defined_types_query():
        """SQL query to extract user-defined types"""
        return """
        SELECT 
            n.nspname as schema_name,
            t.typname as type_name,
            CASE t.typtype
                WHEN 'b' THEN 'base'
                WHEN 'c' THEN 'composite'
                WHEN 'd' THEN 'domain'
                WHEN 'e' THEN 'enum'
                WHEN 'p' THEN 'pseudo'
                WHEN 'r' THEN 'range'
                ELSE 'unknown'
            END as type_category,
            pg_catalog.format_type(t.oid, NULL) as formatted_type,
            t.typdefault as default_value
        FROM pg_catalog.pg_type t
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname NOT IN ('information_schema', 'pg_catalog')
        AND t.typtype IN ('b', 'c', 'd', 'e', 'r')
        ORDER BY n.nspname, t.typname;
        """

class ValidationQueries:
    """Collection of SQL queries for validating database schema"""
    
    @staticmethod
    def get_existing_tables_query():
        """SQL query to get existing tables in target database"""
        return """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name;
        """
    
    @staticmethod
    def get_table_columns_query():
        """SQL query to get columns for a specific table"""
        return """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
        """
    
    @staticmethod
    def get_existing_views_query():
        """SQL query to get existing views in target database"""
        return """
        SELECT table_schema, table_name
        FROM information_schema.views 
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name;
        """
    
    @staticmethod
    def get_existing_functions_query():
        """SQL query to get existing functions in target database"""
        return """
        SELECT n.nspname as schema_name, p.proname as function_name
        FROM pg_catalog.pg_proc p
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY n.nspname, p.proname;
        """
    
    @staticmethod
    def get_existing_indexes_query():
        """SQL query to get existing indexes in target database"""
        return """
        SELECT schemaname, tablename, indexname
        FROM pg_indexes 
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY schemaname, tablename, indexname;
        """
    
    @staticmethod
    def get_existing_sequences_query():
        """SQL query to get existing sequences in target database"""
        return """
        SELECT sequence_schema, sequence_name
        FROM information_schema.sequences
        WHERE sequence_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY sequence_schema, sequence_name;
        """
    
    @staticmethod
    def get_existing_triggers_query():
        """SQL query to get existing triggers in target database"""
        return """
        SELECT trigger_schema, trigger_name
        FROM information_schema.triggers
        WHERE trigger_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY trigger_schema, trigger_name;
        """
    
    @staticmethod
    def get_existing_constraints_query():
        """SQL query to get existing constraints in target database"""
        return """
        SELECT constraint_schema, constraint_name, constraint_type
        FROM information_schema.table_constraints
        WHERE constraint_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY constraint_schema, constraint_name;
        """

# Query categories for organized access
EXTRACTION_QUERIES = {
    'tables': SchemaQueries.get_tables_query,
    'views': SchemaQueries.get_views_query,
    'functions': SchemaQueries.get_functions_query,
    'indexes': SchemaQueries.get_indexes_query,
    'sequences': SchemaQueries.get_sequences_query,
    'triggers': SchemaQueries.get_triggers_query,
    'constraints': SchemaQueries.get_constraints_query,
    'column_privileges': SchemaQueries.get_column_privileges_query,
    'table_privileges': SchemaQueries.get_table_privileges_query,
    'database_info': SchemaQueries.get_database_info_query,
    'schema_info': SchemaQueries.get_schema_info_query,
    'user_types': SchemaQueries.get_user_defined_types_query
}

VALIDATION_QUERIES = {
    'existing_tables': ValidationQueries.get_existing_tables_query,
    'table_columns': ValidationQueries.get_table_columns_query,
    'existing_views': ValidationQueries.get_existing_views_query,
    'existing_functions': ValidationQueries.get_existing_functions_query,
    'existing_indexes': ValidationQueries.get_existing_indexes_query,
    'existing_sequences': ValidationQueries.get_existing_sequences_query,
    'existing_triggers': ValidationQueries.get_existing_triggers_query,
    'existing_constraints': ValidationQueries.get_existing_constraints_query
}

def get_query(category: str, query_name: str) -> str:
    """
    Get a specific query by category and name
    
    Args:
        category: 'extraction' or 'validation'
        query_name: Name of the query
        
    Returns:
        SQL query string
        
    Raises:
        ValueError: If category or query_name not found
    """
    if category == 'extraction':
        if query_name in EXTRACTION_QUERIES:
            return EXTRACTION_QUERIES[query_name]()
        else:
            raise ValueError(f"Extraction query '{query_name}' not found. Available: {list(EXTRACTION_QUERIES.keys())}")
    
    elif category == 'validation':
        if query_name in VALIDATION_QUERIES:
            return VALIDATION_QUERIES[query_name]()
        else:
            raise ValueError(f"Validation query '{query_name}' not found. Available: {list(VALIDATION_QUERIES.keys())}")
    
    else:
        raise ValueError(f"Invalid category '{category}'. Use 'extraction' or 'validation'")

def list_available_queries():
    """List all available queries by category"""
    return {
        'extraction': list(EXTRACTION_QUERIES.keys()),
        'validation': list(VALIDATION_QUERIES.keys())
    }

if __name__ == "__main__":
    # Example usage and testing
    print("Available queries:")
    queries = list_available_queries()
    
    print("\nExtraction Queries:")
    for query_name in queries['extraction']:
        print(f"  - {query_name}")
    
    print("\nValidation Queries:")
    for query_name in queries['validation']:
        print(f"  - {query_name}")
    
    # Test query retrieval
    print("\nExample query:")
    print(get_query('extraction', 'tables'))
