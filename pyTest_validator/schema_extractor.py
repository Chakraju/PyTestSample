#!/usr/bin/env python3
"""
Schema Extractor - Reads sandbox database schema and generates JSON files
for tables, views, functions, procedures, etc.
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import argparse
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SchemaExtractor:
    def __init__(self, connection_params: Dict[str, str], output_dir: str = "schema_json"):
        self.connection_params = connection_params
        self.output_dir = output_dir
        self.connection = None
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            logger.info("Connected to database successfully")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def execute_query(self, query: str) -> List[Dict]:
        """Execute query and return results as list of dictionaries"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def extract_tables(self) -> List[Dict]:
        """Extract table information"""
        query = """
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
        
        results = self.execute_query(query)
        tables = {}
        
        for row in results:
            table_key = f"{row['table_schema']}.{row['table_name']}"
            
            if table_key not in tables:
                tables[table_key] = {
                    'schema': row['table_schema'],
                    'name': row['table_name'],
                    'type': row['table_type'],
                    'columns': [],
                    'constraints': []
                }
            
            if row['column_name']:
                column_info = {
                    'name': row['column_name'],
                    'position': row['ordinal_position'],
                    'data_type': row['data_type'],
                    'is_nullable': row['is_nullable'],
                    'default': row['column_default'],
                    'max_length': row['character_maximum_length'],
                    'numeric_precision': row['numeric_precision'],
                    'numeric_scale': row['numeric_scale']
                }
                
                # Avoid duplicate columns
                if not any(col['name'] == column_info['name'] for col in tables[table_key]['columns']):
                    tables[table_key]['columns'].append(column_info)
            
            if row['constraint_name'] and row['constraint_type']:
                constraint_info = {
                    'name': row['constraint_name'],
                    'type': row['constraint_type']
                }
                # Avoid duplicate constraints
                if constraint_info not in tables[table_key]['constraints']:
                    tables[table_key]['constraints'].append(constraint_info)
        
        return list(tables.values())
    
    def extract_views(self) -> List[Dict]:
        """Extract view information"""
        query = """
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
        
        results = self.execute_query(query)
        views = {}
        
        for row in results:
            view_key = f"{row['table_schema']}.{row['view_name']}"
            
            if view_key not in views:
                views[view_key] = {
                    'schema': row['table_schema'],
                    'name': row['view_name'],
                    'definition': row['view_definition'],
                    'columns': []
                }
            
            if row['column_name']:
                column_info = {
                    'name': row['column_name'],
                    'position': row['ordinal_position'],
                    'data_type': row['data_type'],
                    'is_nullable': row['is_nullable']
                }
                views[view_key]['columns'].append(column_info)
        
        return list(views.values())
    
    def extract_functions(self) -> List[Dict]:
        """Extract function/procedure information"""
        query = """
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
        
        results = self.execute_query(query)
        return results
    
    def extract_indexes(self) -> List[Dict]:
        """Extract index information"""
        query = """
        SELECT 
            schemaname,
            tablename,
            indexname,
            indexdef
        FROM pg_indexes 
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
        ORDER BY schemaname, tablename, indexname;
        """
        
        results = self.execute_query(query)
        return results
    
    def extract_sequences(self) -> List[Dict]:
        """Extract sequence information"""
        query = """
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
        
        results = self.execute_query(query)
        return results
    
    def save_to_json(self, data: Any, filename: str):
        """Save data to JSON file"""
        filepath = os.path.join(self.output_dir, filename)
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str)
            logger.info(f"Saved {filename} with {len(data) if isinstance(data, list) else 1} records")
        except Exception as e:
            logger.error(f"Failed to save {filename}: {e}")
            raise
    
    def extract_all_schemas(self):
        """Extract all schema objects and save to JSON files"""
        try:
            self.connect()
            
            # Generate metadata
            metadata = {
                'extraction_timestamp': datetime.now().isoformat(),
                'database_info': {
                    'host': self.connection_params.get('host'),
                    'database': self.connection_params.get('database'),
                    'user': self.connection_params.get('user')
                }
            }
            self.save_to_json(metadata, 'metadata.json')
            
            # Extract tables
            logger.info("Extracting tables...")
            tables = self.extract_tables()
            self.save_to_json(tables, 'tables.json')
            
            # Extract views
            logger.info("Extracting views...")
            views = self.extract_views()
            self.save_to_json(views, 'views.json')
            
            # Extract functions/procedures
            logger.info("Extracting functions...")
            functions = self.extract_functions()
            self.save_to_json(functions, 'functions.json')
            
            # Extract indexes
            logger.info("Extracting indexes...")
            indexes = self.extract_indexes()
            self.save_to_json(indexes, 'indexes.json')
            
            # Extract sequences
            logger.info("Extracting sequences...")
            sequences = self.extract_sequences()
            self.save_to_json(sequences, 'sequences.json')
            
            logger.info(f"Schema extraction completed. Files saved to: {self.output_dir}")
            
        except Exception as e:
            logger.error(f"Schema extraction failed: {e}")
            raise
        finally:
            self.disconnect()

def main():
    parser = argparse.ArgumentParser(description='Extract database schema to JSON files')
    parser.add_argument('--host', required=True, help='Database host')
    parser.add_argument('--port', default='5432', help='Database port')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--user', required=True, help='Database user')
    parser.add_argument('--password', required=True, help='Database password')
    parser.add_argument('--output-dir', default='schema_json', help='Output directory for JSON files')
    
    args = parser.parse_args()
    
    connection_params = {
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'user': args.user,
        'password': args.password
    }
    
    extractor = SchemaExtractor(connection_params, args.output_dir)
    extractor.extract_all_schemas()

if __name__ == "__main__":
    main()
