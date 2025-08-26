#!/usr/bin/env python3
"""
Pytest Schema Validator - Validates dev database schema against JSON files
and generates HTML report with success and error details.
"""

import json
import os
import pytest
import psycopg2
from psycopg2.extras import RealDictCursor
import argparse
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from jinja2 import Template
from sql_queries import get_query

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    test_name: str
    status: str  # 'PASS', 'FAIL', 'SKIP'
    message: str
    details: Optional[Dict] = None
    duration: float = 0.0

class SchemaValidator:
    def __init__(self, connection_params: Dict[str, str], json_dir: str = "schema_json"):
        self.connection_params = connection_params
        self.json_dir = json_dir
        self.connection = None
        self.validation_results = []
        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            logger.info("Connected to dev database successfully")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def execute_query(self, query: str) -> List[Dict]:
        """Execute query and return results"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def load_json_file(self, filename: str) -> Any:
        """Load JSON file"""
        filepath = os.path.join(self.json_dir, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"JSON file not found: {filename}")
            return None
        except Exception as e:
            logger.error(f"Failed to load {filename}: {e}")
            raise

class TestSchemaValidation:
    """Pytest test class for schema validation"""
    
    @classmethod
    def setup_class(cls):
        """Setup test class with database connection"""
        cls.validator = None
    
    @classmethod
    def teardown_class(cls):
        """Cleanup after tests"""
        if cls.validator:
            cls.validator.disconnect()
    
    def setup_method(self):
        """Setup for each test method"""
        if not hasattr(self.__class__, 'validator') or self.__class__.validator is None:
            # Get connection params from pytest config or environment
            connection_params = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': os.getenv('DB_PORT', '5432'),
                'database': os.getenv('DB_NAME', 'test_db'),
                'user': os.getenv('DB_USER', 'postgres'),
                'password': os.getenv('DB_PASSWORD', 'password')
            }
            json_dir = os.getenv('JSON_DIR', 'schema_json')
            
            self.__class__.validator = SchemaValidator(connection_params, json_dir)
            self.__class__.validator.connect()
    
    def test_tables_exist(self):
        """Test that all expected tables exist in dev database"""
        tables_data = self.validator.load_json_file('tables.json')
        if not tables_data:
            pytest.skip("No tables.json file found")
        
        # Get existing tables from dev database
        query = get_query('validation', 'existing_tables')
        existing_tables = self.validator.execute_query(query)
        existing_table_keys = {f"{t['table_schema']}.{t['table_name']}" for t in existing_tables}
        
        missing_tables = []
        for expected_table in tables_data:
            table_key = f"{expected_table['schema']}.{expected_table['name']}"
            if table_key not in existing_table_keys:
                missing_tables.append(table_key)
        
        assert not missing_tables, f"Missing tables in dev database: {missing_tables}"
    
    def test_table_columns(self):
        """Test that tables have expected columns with correct data types"""
        tables_data = self.validator.load_json_file('tables.json')
        if not tables_data:
            pytest.skip("No tables.json file found")
        
        errors = []
        
        for expected_table in tables_data:
            table_schema = expected_table['schema']
            table_name = expected_table['name']
            
            # Get actual columns from dev database
            query = get_query('validation', 'table_columns')
            
            try:
                actual_columns = self.validator.execute_query(query)
                actual_column_dict = {col['column_name']: col for col in actual_columns}
                
                for expected_col in expected_table['columns']:
                    col_name = expected_col['name']
                    
                    if col_name not in actual_column_dict:
                        errors.append(f"Table {table_schema}.{table_name}: Missing column '{col_name}'")
                        continue
                    
                    actual_col = actual_column_dict[col_name]
                    
                    # Check data type
                    if expected_col['data_type'] != actual_col['data_type']:
                        errors.append(
                            f"Table {table_schema}.{table_name}, column '{col_name}': "
                            f"Expected type '{expected_col['data_type']}', got '{actual_col['data_type']}'"
                        )
                    
                    # Check nullable
                    if expected_col['is_nullable'] != actual_col['is_nullable']:
                        errors.append(
                            f"Table {table_schema}.{table_name}, column '{col_name}': "
                            f"Expected nullable '{expected_col['is_nullable']}', got '{actual_col['is_nullable']}'"
                        )
                        
            except Exception as e:
                errors.append(f"Error validating table {table_schema}.{table_name}: {str(e)}")
        
        assert not errors, f"Column validation errors: {errors}"
    
    def test_views_exist(self):
        """Test that all expected views exist"""
        views_data = self.validator.load_json_file('views.json')
        if not views_data:
            pytest.skip("No views.json file found")
        
        query = get_query('validation', 'existing_views')
        existing_views = self.validator.execute_query(query)
        existing_view_keys = {f"{v['table_schema']}.{v['table_name']}" for v in existing_views}
        
        missing_views = []
        for expected_view in views_data:
            view_key = f"{expected_view['schema']}.{expected_view['name']}"
            if view_key not in existing_view_keys:
                missing_views.append(view_key)
        
        assert not missing_views, f"Missing views in dev database: {missing_views}"
    
    def test_functions_exist(self):
        """Test that all expected functions exist"""
        functions_data = self.validator.load_json_file('functions.json')
        if not functions_data:
            pytest.skip("No functions.json file found")
        
        query = get_query('validation', 'existing_functions')
        existing_functions = self.validator.execute_query(query)
        existing_func_keys = {f"{f['schema_name']}.{f['function_name']}" for f in existing_functions}
        
        missing_functions = []
        for expected_func in functions_data:
            func_key = f"{expected_func['schema_name']}.{expected_func['function_name']}"
            if func_key not in existing_func_keys:
                missing_functions.append(func_key)
        
        assert not missing_functions, f"Missing functions in dev database: {missing_functions}"
    
    def test_indexes_exist(self):
        """Test that all expected indexes exist"""
        indexes_data = self.validator.load_json_file('indexes.json')
        if not indexes_data:
            pytest.skip("No indexes.json file found")
        
        query = get_query('validation', 'existing_indexes')
        existing_indexes = self.validator.execute_query(query)
        existing_index_keys = {f"{i['schemaname']}.{i['indexname']}" for i in existing_indexes}
        
        missing_indexes = []
        for expected_idx in indexes_data:
            idx_key = f"{expected_idx['schemaname']}.{expected_idx['indexname']}"
            if idx_key not in existing_index_keys:
                missing_indexes.append(idx_key)
        
        assert not missing_indexes, f"Missing indexes in dev database: {missing_indexes}"
    
    def test_sequences_exist(self):
        """Test that all expected sequences exist"""
        sequences_data = self.validator.load_json_file('sequences.json')
        if not sequences_data:
            pytest.skip("No sequences.json file found")
        
        query = get_query('validation', 'existing_sequences')
        existing_sequences = self.validator.execute_query(query)
        existing_seq_keys = {f"{s['sequence_schema']}.{s['sequence_name']}" for s in existing_sequences}
        
        missing_sequences = []
        for expected_seq in sequences_data:
            seq_key = f"{expected_seq['sequence_schema']}.{expected_seq['sequence_name']}"
            if seq_key not in existing_seq_keys:
                missing_sequences.append(seq_key)
        
        assert not missing_sequences, f"Missing sequences in dev database: {missing_sequences}"

def generate_html_report(json_report_path: str, html_report_path: str):
    """Generate HTML report from pytest JSON report"""
    
    html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>Database Schema Validation Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { background-color: #f0f0f0; padding: 20px; border-radius: 5px; }
        .summary { margin: 20px 0; }
        .test-case { border: 1px solid #ddd; margin: 10px 0; padding: 15px; border-radius: 5px; }
        .passed { border-left: 5px solid #4CAF50; }
        .failed { border-left: 5px solid #f44336; }
        .skipped { border-left: 5px solid #ff9800; }
        .error-details { background-color: #ffebee; padding: 10px; margin-top: 10px; border-radius: 3px; }
        .stats { display: flex; gap: 20px; }
        .stat-box { padding: 10px; border-radius: 5px; text-align: center; }
        .stat-passed { background-color: #e8f5e8; color: #4CAF50; }
        .stat-failed { background-color: #ffebee; color: #f44336; }
        .stat-skipped { background-color: #fff3e0; color: #ff9800; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Database Schema Validation Report</h1>
        <p>Generated: {{ report_time }}</p>
        <p>Database: {{ database_info }}</p>
    </div>
    
    <div class="summary">
        <h2>Test Summary</h2>
        <div class="stats">
            <div class="stat-box stat-passed">
                <h3>{{ passed_count }}</h3>
                <p>Passed</p>
            </div>
            <div class="stat-box stat-failed">
                <h3>{{ failed_count }}</h3>
                <p>Failed</p>
            </div>
            <div class="stat-box stat-skipped">
                <h3>{{ skipped_count }}</h3>
                <p>Skipped</p>
            </div>
        </div>
        <p><strong>Total Duration:</strong> {{ total_duration }}s</p>
    </div>
    
    <div class="test-results">
        <h2>Test Results</h2>
        {% for test in tests %}
        <div class="test-case {{ test.outcome }}">
            <h3>{{ test.nodeid }}</h3>
            <p><strong>Status:</strong> {{ test.outcome.upper() }}</p>
            <p><strong>Duration:</strong> {{ "%.2f"|format(test.duration) }}s</p>
            {% if test.outcome == 'failed' %}
            <div class="error-details">
                <h4>Error Details:</h4>
                <pre>{{ test.longrepr }}</pre>
            </div>
            {% endif %}
        </div>
        {% endfor %}
    </div>
</body>
</html>
    """
    
    try:
        # Load pytest JSON report
        with open(json_report_path, 'r') as f:
            report_data = json.load(f)
        
        # Extract test information
        tests = report_data.get('tests', [])
        summary = report_data.get('summary', {})
        
        passed_count = summary.get('passed', 0)
        failed_count = summary.get('failed', 0)
        skipped_count = summary.get('skipped', 0)
        total_duration = report_data.get('duration', 0)
        
        # Generate HTML
        template = Template(html_template)
        html_content = template.render(
            report_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            database_info=os.getenv('DB_NAME', 'Unknown'),
            passed_count=passed_count,
            failed_count=failed_count,
            skipped_count=skipped_count,
            total_duration=f"{total_duration:.2f}",
            tests=tests
        )
        
        # Save HTML report
        with open(html_report_path, 'w') as f:
            f.write(html_content)
        
        logger.info(f"HTML report generated: {html_report_path}")
        
    except Exception as e:
        logger.error(f"Failed to generate HTML report: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Validate database schema using pytest')
    parser.add_argument('--host', required=True, help='Database host')
    parser.add_argument('--port', default='5432', help='Database port')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--user', required=True, help='Database user')
    parser.add_argument('--password', required=True, help='Database password')
    parser.add_argument('--json-dir', default='schema_json', help='Directory containing JSON schema files')
    parser.add_argument('--report-dir', default='reports', help='Directory for test reports')
    
    args = parser.parse_args()
    
    # Set environment variables for pytest
    os.environ['DB_HOST'] = args.host
    os.environ['DB_PORT'] = args.port
    os.environ['DB_NAME'] = args.database
    os.environ['DB_USER'] = args.user
    os.environ['DB_PASSWORD'] = args.password
    os.environ['JSON_DIR'] = args.json_dir
    
    # Create reports directory
    os.makedirs(args.report_dir, exist_ok=True)
    
    # Define report paths
    json_report_path = os.path.join(args.report_dir, 'pytest_report.json')
    html_report_path = os.path.join(args.report_dir, 'schema_validation_report.html')
    
    # Run pytest with JSON reporting
    pytest_args = [
        __file__,
        '-v',
        f'--json-report',
        f'--json-report-file={json_report_path}',
        '--tb=short'
    ]
    
    logger.info("Running pytest schema validation...")
    exit_code = pytest.main(pytest_args)
    
    # Generate HTML report
    if os.path.exists(json_report_path):
        generate_html_report(json_report_path, html_report_path)
        print(f"\nReports generated:")
        print(f"  JSON: {json_report_path}")
        print(f"  HTML: {html_report_path}")
    else:
        logger.error("JSON report not found, cannot generate HTML report")
    
    return exit_code

if __name__ == "__main__":
    exit(main())
