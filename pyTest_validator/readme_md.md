# Database Schema Validation Suite

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
   # Download the three Python files:
   # - schema_extractor.py
   # - pytest_validator.py
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

## 🔧 Configuration Options

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

### Database Connection

The tools support standard PostgreSQL connection parameters:
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
# Schema validation pipeline script

# Extract schema from sandbox
python schema_extractor.py \
    --host $SANDBOX_HOST \
    --database $SANDBOX_DB \
    --user $DB_USER \
    --password $DB_PASSWORD \
    --output-dir schema_json

# Validate dev database
python pytest_validator.py \
    --host $DEV_HOST \
    --database $DEV_DB \
    --user $DB_USER \
    --password $DB_PASSWORD \
    --json-dir schema_json \
    --report-dir reports

# Check validation results
if [ $? -eq 0 ]; then
    echo "Schema validation passed!"
    # Deploy to staging
else
    echo "Schema validation failed! Check reports/schema_validation_report.html"
    exit 1
fi
```

### Docker Integration

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY schema_extractor.py pytest_validator.py ./

# Run schema validation
CMD ["python", "pytest_validator.py"]
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