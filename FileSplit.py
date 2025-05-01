import re
import os
import sys
import argparse
import subprocess
import tempfile

def extract_table_name(statement, command):
    pattern = rf'{command}\s+(ONLY\s+)?("?[\w\.]+"?)'
    match = re.search(pattern, statement, re.IGNORECASE)
    if match:
        return match.group(2).strip('"')
    return None

def is_primary_key(statement):
    return re.search(r'PRIMARY\s+KEY', statement, re.IGNORECASE) is not None

def is_foreign_key(statement):
    return re.search(r'FOREIGN\s+KEY', statement, re.IGNORECASE) is not None

def separate_ddl_statements(ddl_content):
    try:
        statements = re.split(r';\s*\n', ddl_content)
        statements = [stmt.strip() for stmt in statements if stmt.strip()]
        create_tables = {}
        alter_statements = []

        for stmt in statements:
            table_name = extract_table_name(stmt, 'CREATE TABLE')
            if table_name:
                create_tables[table_name] = stmt + ';'
            else:
                alter_statements.append(stmt + ';')

        alters_for_new = []
        alters_for_existing = []

        for stmt in alter_statements:
            table_name = extract_table_name(stmt, 'ALTER TABLE')
            if table_name:
                if table_name in create_tables:
                    alters_for_new.append((stmt, table_name))
                else:
                    alters_for_existing.append(stmt)
            else:
                alters_for_existing.append(stmt)

        alters_grouped = {}
        for stmt, table_name in alters_for_new:
            alters_grouped.setdefault(table_name, []).append(stmt)

        new_table_ddl = []
        for table_name, create_stmt in create_tables.items():
            new_table_ddl.append(create_stmt)
            pk_stmts, fk_stmts, other_stmts = [], [], []
            for alter_stmt in alters_grouped.get(table_name, []):
                if is_primary_key(alter_stmt):
                    pk_stmts.append(alter_stmt)
                elif is_foreign_key(alter_stmt):
                    fk_stmts.append(alter_stmt)
                else:
                    other_stmts.append(alter_stmt)
            new_table_ddl.extend(pk_stmts + fk_stmts + other_stmts)

        return new_table_ddl, alters_for_existing
    except Exception as e:
        raise RuntimeError(f"Error while parsing DDL statements: {e}")

def run_script(script_name, input_file, output_folder, db_type=None):
    cmd = ['python', script_name, input_file, '--output', output_folder]
    if db_type:
        cmd += ['--db', db_type]
    try:
        print(f"🔧 Running: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Script '{script_name}' failed with exit code {e.returncode}")
    except FileNotFoundError:
        raise RuntimeError(f"Script '{script_name}' not found. Make sure it exists.")

def main():
    parser = argparse.ArgumentParser(description="Liquibase DDL splitter and dispatcher.")
    parser.add_argument('--input', required=True, help='Path to input DDL SQL file')
    parser.add_argument('--output', default='.', help='Output folder for generated YAML (default: current directory)')
    parser.add_argument('--db', help='Target database type (e.g., postgres, mysql)')

    args = parser.parse_args()

    input_ddl_path = args.input
    output_folder = args.output
    db_type = args.db

    if not os.path.isfile(input_ddl_path):
        print(f"❌ Error: File not found — {input_ddl_path}")
        sys.exit(1)

    if not os.path.isdir(output_folder):
        print(f"❌ Error: Output folder does not exist — {output_folder}")
        sys.exit(1)

    try:
        with open(input_ddl_path, 'r') as f:
            ddl_content = f.read()
    except Exception as e:
        print(f"❌ Failed to read input file: {e}")
        sys.exit(1)

    try:
        new_table_ddl, existing_alters = separate_ddl_statements(ddl_content)

        create_file_path = alter_file_path = None

        if new_table_ddl:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.sql', mode='w') as create_temp:
                create_file_path = create_temp.name
                create_temp.write('\n\n'.join(new_table_ddl))

        if existing_alters:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.sql', mode='w') as alter_temp:
                alter_file_path = alter_temp.name
                alter_temp.write('\n\n'.join(existing_alters))

        if create_file_path:
            run_script('create.py', create_file_path, output_folder, db_type)

        if alter_file_path:
            run_script('alter.py', alter_file_path, output_folder, db_type)

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        # Clean up temp files
        if create_file_path and os.path.exists(create_file_path):
            os.unlink(create_file_path)
        if alter_file_path and os.path.exists(alter_file_path):
            os.unlink(alter_file_path)

if __name__ == "__main__":
    main()
