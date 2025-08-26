## 📁 Project Structure

```
database-schema-validator/
├── schema_extractor.py          # Main extraction program
├── pytest_validator.py          # Validation and testing program  
├── sql_queries.py              # SQL queries helper module
├── README.md                   # This documentation
├── requirements.txt            # Python dependencies
├── schema_json/               # Generated schema files (created by extractor)
│   ├── metadata.json
│   ├── tables.json
│   ├── views.json
│   ├── functions.json
│   ├── indexes.json
│   └── sequences.json
└── reports/                   # Generated test reports (created by validator)
    ├── pytest_report.json
    └── schema_validation_report.html
```

## 🔧 Architecture

The suite is designed with separation of concerns:

- **`schema_extractor.py`** - Main extraction logic and database connection handling
- **`pytest_validator.py`** - Validation logic and HTML report generation  
- **`sql_queries.py`** - Centralized SQL query repository for maintainability

### SQL Queries Helper (`sql_queries.py`)

This module contains all SQL queries organized into logical groups:
- **SchemaQueries** - Extraction queries for sandbox database
- **ValidationQueries** - Validation queries for dev database  
- **Helper Functions** - Query retrieval and management utilities

Benefits:
- ✅ Centralized query management
- ✅ Easy to maintain and update SQL
- ✅ Reusable across multiple modules
- ✅ Better testing capabilities
- ✅ Version control for query changes# Database Schema Validation Suite

A comprehensive Python-based solution for extracting database schemas from sandbox environments and validating them against development databases using pytest with detailed HTML reporting.

## 🚀 Overview

This suite consists of two main programs:

1. **Schema Extractor** - Extracts complete schema information from sandbox database
2. **Pytest Validator** - Validates dev database against extracted schema with detailed reporting

## 📋 Features

### Schema Extractor
- ✅ Extract tables with complete column metadata (data types, constraints, defaults)
- ✅ Extract views with definitions and column structures
- ✅ Extract functions, procedures, and their signatures
- ✅ Extract indexes and their definitions
- ✅ Extract sequences with configuration
- ✅ Generate organized JSON files for each schema component
- ✅ Comprehensive logging and error handling

### Pytest Validator
- ✅ Validate table existence and structure
- ✅ Verify column data types and nullable constraints
- ✅ Check view definitions and structures
- ✅ Validate function and procedure existence
- ✅ Verify index and sequence presence
- ✅ Generate professional HTML reports with statistics
- ✅ Detailed error reporting and debugging information

## 🛠 Prerequisites

- Python 3.7+
- PostgreSQL database access
- Required Python packages (see Installation)

## 📦 Installation

1. **Clone or download the files:**
   ```bash
   # Download the four Python files:
   # - schema_extractor.py
   # - pytest_validator.py  
   # - sql_queries.py
   # - README.md
   ```

2. **Install required dependencies:**
   ```bash
   pip install psycopg2-binary pytest pytest-json-report jinja2
   ```

   Or create a `requirements.txt` file:
   ```txt
   psycopg2-binary>=2.9.0
   pytest>=7.0.0
   pytest-json-report>=1.5.0
   jinja2>=3.0.0
   ```
   
   Then install:
   ```bash
   pip install -r requirements.txt
   ```

## 🚀 Quick Start

### Step 1: Extract Schema from Sandbox

```bash
python schema_extractor.py \
    --host sandbox.example.com \
    --port 5432 \
    --database sandbox_db \
    --user your_username \
    --password your_password \
    --output-dir schema_json
```

### Step 2: Validate Dev Database

```bash
python pytest_validator.py \
    --host dev.example.com \
    --port 5432 \
    --database dev_db \
    --user your_username \
    --password your_password \
    --json-dir schema_json \
    --report-dir reports
```

### Step 3: View Results

Open `reports/schema_validation_report.html` in your browser to see the detailed validation report.

## 📖 Detailed Usage

### Schema Extractor Options

```bash
python schema_extractor.py [OPTIONS]

Required Arguments:
  --host          Database host address
  --database      Database name
  --user          Database username
  --password      Database password

Optional Arguments:
  --port          Database port (default: 5432)
  --output-dir    Output directory for JSON files (default: schema_json)
```

### Pytest Validator Options

```bash
python pytest_validator.py [OPTIONS]

Required Arguments:
  --host          Database host address
  --database      Database name
  --user          Database username
  --password      Database password

Optional Arguments:
  --port          Database port (default: 5432)
  --json-dir      Directory containing JSON schema files (default: schema_json)
  --report-dir    Directory for test reports (default: reports)
```

## 🎯 Query Categories

The `sql_queries.py` module organizes queries into two main categories:

### Extraction Queries
- **tables** - Complete table structures with columns and constraints
- **views** - View definitions and column information
- **functions** - Functions, procedures, and their metadata
- **indexes** - Index definitions and properties
- **sequences** - Sequence configurations
- **triggers** - Trigger definitions and actions
- **constraints** - Detailed constraint information
- **privileges** - Column and table permissions
- **database_info** - Database metadata and settings
- **user_types** - Custom user-defined types

### Validation Queries
- **existing_tables** - Tables present in target database
- **table_columns** - Column verification for specific tables
- **existing_views** - Views present in target database
- **existing_functions** - Functions present in target database
- **existing_indexes** - Indexes present in target database
- **existing_sequences** - Sequences present in target database
- **existing_triggers** - Triggers present in target database
- **existing_constraints** - Constraints present in target database

## 📁 Output Structure

### Schema Extraction Output

```
schema_json/
├── metadata.json      # Extraction metadata and database info
├── tables.json        # Complete table structures
├── views.json         # View definitions and columns
├── functions.json     # Functions and procedures
├── indexes.json       # Index definitions
└── sequences.json     # Sequence configurations
```

### Validation Report Output

```
reports/
├── pytest_report.json                 # Detailed pytest JSON report
└── schema_validation_report.html      # Professional HTML report
```

## 📊 Sample JSON Schema Structure

### Tables JSON Structure
```json
[
  {
    "schema": "public",
    "name": "users",
    "type": "BASE TABLE",
    "columns": [
      {
        "name": "id",
        "position": 1,
        "data_type": "integer",
        "is_nullable": "NO",
        "default": "nextval('users_id_seq'::regclass)",
        "max_length": null,
        "numeric_precision": 32,
        "numeric_scale": 0
      }
    ],
    "constraints": [
      {
        "name": "users_pkey",
        "type": "PRIMARY KEY"
      }
    ]
  }
]
```

## 🧪 Testing & Development

### Testing SQL Queries

The `sql_queries.py` module includes a built-in test runner:

```bash
# Test the SQL queries helper
python sql_queries.py
```

This will:
- List all available queries
- Test query retrieval functions
- Display sample query output

### Adding New Schema Objects

To add support for new database objects:

1. **Add extraction query** in `SchemaQueries` class
2. **Add validation query** in `ValidationQueries` class  
3. **Update the query dictionaries** (`EXTRACTION_QUERIES`, `VALIDATION_QUERIES`)
4. **Add extraction method** in `schema_extractor.py`
5. **Add validation test** in `pytest_validator.py`

Example for adding materialized views:

```python
# In sql_queries.py
@staticmethod
def get_materialized_views_query():
    return """
    SELECT schemaname, matviewname, definition
    FROM pg_matviews
    WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
    """

# Add to extraction queries
EXTRACTION_QUERIES['materialized_views'] = SchemaQueries.get_materialized_views_query
```

### Query Performance Optimization

For large databases, you can optimize queries by:

1. **Adding indexes** on system tables (if permitted)
2. **Filtering schemas** more specifically
3. **Paginating results** for very large schemas
4. **Parallel extraction** for different object types

## 🧪 Test Coverage

The pytest validator includes comprehensive tests for:

- **Table Validation**
  - Table existence check
  - Column name and data type validation
  - Nullable constraint verification
  - Default value checking

- **View Validation**
  - View existence verification
  - Column structure validation

- **Function Validation**
  - Function and procedure existence
  - Signature verification

- **Index Validation**
  - Index existence and definition

- **Sequence Validation**
  - Sequence presence and configuration

## 📈 HTML Report Features

The generated HTML report includes:

- 📊 **Visual Summary Statistics** - Pass/Fail/Skip counts with color coding
- ⏱️ **Test Duration Metrics** - Individual and total execution times
- 🔍 **Detailed Test Results** - Each test with status and error details
- 🎨 **Professional Styling** - Clean, readable format with responsive design
- 🚨 **Error Details** - Complete stack traces and failure reasons

## 🔧 Advanced Usage

### Custom Query Development

You can extend the `sql_queries.py` module to add new extraction or validation queries:

```python
# Add to sql_queries.py
@staticmethod
def get_custom_objects_query():
    """Custom SQL query for specific database objects"""
    return """
    SELECT custom_field1, custom_field2
    FROM custom_table
    WHERE custom_condition = true;
    """

# Add to the EXTRACTION_QUERIES dictionary
EXTRACTION_QUERIES['custom_objects'] = SchemaQueries.get_custom_objects_query
```

### Using Individual Queries

You can use the query helper independently:

```python
from sql_queries import get_query, list_available_queries

# Get a specific query
tables_query = get_query('extraction', 'tables')

# List all available queries
available = list_available_queries()
print(available)
```

### Database-Specific Customization

The queries are designed for PostgreSQL but can be adapted for other databases by modifying the `sql_queries.py` file:

```python
# Example: MySQL adaptation
@staticmethod
def get_tables_query_mysql():
    """MySQL version of tables query"""
    return """
    SELECT 
        TABLE_SCHEMA as table_schema,
        TABLE_NAME as table_name,
        TABLE_TYPE as table_type
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema')
    """
```

### Environment Variables

You can also use environment variables instead of command-line arguments:

```bash
export DB_HOST=your_host
export DB_PORT=5432
export DB_NAME=your_database
export DB_USER=your_username
export DB_PASSWORD=your_password
export JSON_DIR=schema_json
```

## 🔧 Configuration Options

### Database Connection
- Host and port specification
- Username/password authentication
- Database name selection
- SSL connection support (through psycopg2 defaults)

## 🛡️ Error Handling

Both tools include comprehensive error handling:

- **Connection Errors** - Clear messages for database connectivity issues
- **Authentication Failures** - Specific error reporting for credential problems
- **Missing Files** - Graceful handling of missing JSON schema files
- **Schema Mismatches** - Detailed reporting of validation failures
- **SQL Errors** - Proper error propagation with context

## 📝 Logging

Detailed logging is provided for:
- Database connection status
- Schema extraction progress
- Test execution details
- Error conditions and debugging information

Logs are output to console with timestamps and severity levels.

## 🔄 Integration Examples

### CI/CD Pipeline Integration

```bash
#!/bin/bash
# Enhanced schema validation pipeline script

# Set error handling
set -e

echo "Starting Schema Validation Pipeline..."

# Extract schema from sandbox
echo "Step 1: Extracting schema from sandbox..."
python schema_extractor.py \
    --host $SANDBOX_HOST \
    --database $SANDBOX_DB \
    --user $DB_USER \
    --password $DB_PASSWORD \
    --output-dir schema_json

echo "Schema extraction completed!"

# Validate dev database
echo "Step 2: Validating dev database..."
python pytest_validator.py \
    --host $DEV_HOST \
    --database $DEV_DB \
    --user $DB_USER \
    --password $DB_PASSWORD \
    --json-dir schema_json \
    --report-dir reports

# Check validation results
if [ $? -eq 0 ]; then
    echo "✅ Schema validation passed!"
    echo "📊 View detailed report: reports/schema_validation_report.html"
    
    # Optional: Deploy to staging or next environment
    # ./deploy_to_staging.sh
else
    echo "❌ Schema validation failed!"
    echo "📊 Check detailed report: reports/schema_validation_report.html"
    echo "📋 JSON report: reports/pytest_report.json"
    exit 1
fi
```

### Enhanced Docker Integration

```dockerfile
FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY schema_extractor.py .
COPY pytest_validator.py .
COPY sql_queries.py .

# Create directories for output
RUN mkdir -p schema_json reports

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "pytest_validator.py", "--help"]
```

### Kubernetes Job Example

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: schema-validation
spec:
  template:
    spec:
      containers:
      - name: validator
        image: schema-validator:latest
        env:
        - name: DB_HOST
          value: "postgres-service"
        - name: DB_NAME
          value: "myapp_dev"
        - name: DB_USER
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: username
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: password
        command: ["python", "pytest_validator.py"]
        args: [
          "--host", "$(DB_HOST)",
          "--database", "$(DB_NAME)", 
          "--user", "$(DB_USER)",
          "--password", "$(DB_PASSWORD)"
        ]
      restartPolicy: Never
```

## 🤝 Contributing

To contribute to this project:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is open source. Feel free to use and modify according to your needs.

## 🆘 Troubleshooting

### Common Issues

1. **Connection Refused**
   ```
   Solution: Verify database host, port, and firewall settings
   ```

2. **Authentication Failed**
   ```
   Solution: Check username, password, and database permissions
   ```

3. **Missing JSON Files**
   ```
   Solution: Run schema_extractor.py first to generate JSON files
   ```

4. **Import Errors**
   ```
   Solution: Install all required dependencies using pip
   ```

### Debug Mode

For debugging, you can modify the logging level in both scripts:
```python
logging.basicConfig(level=logging.DEBUG)
```

## 📞 Support

For issues and questions:
1. Check the troubleshooting section above
2. Review the error messages in the HTML report
3. Check the console logs for detailed error information
4. Verify database connectivity and permissions

---

**Happy Schema Validating! 🎉**