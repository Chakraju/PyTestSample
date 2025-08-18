import argparse
import glob
import os
import sys
import yaml
import subprocess

JAR_PATH = os.path.join("dbunit_runner", "target", "dbunit-runner-jar-with-dependencies.jar")

def parse_args():
    ap = argparse.ArgumentParser(description="Test Dev DB using DBUnit with Flat XML datasets.")
    ap.add_argument("--config", required=True, help="Path to config.yaml")
    ap.add_argument("--datasets_dir", default="datasets", help="Directory containing *.xml exports")
    ap.add_argument("--exclude_cols", default="", help="Comma-separated column names to ignore (e.g., created_ts,updated_ts)")
    return ap.parse_args()

def load_cfg(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def sqlalchemy_to_jdbc_pg(sa_url: str, schema: str) -> str:
    # Convert SQLAlchemy PG URL -> JDBC (very small helper for this POC)
    # SA: postgresql+psycopg2://user:pass@host:5432/db
    # JDBC: jdbc:postgresql://host:5432/db?currentSchema=schema
    if not sa_url.startswith("postgresql"):
        raise ValueError("Only PostgreSQL SQLAlchemy URLs are supported in this POC.")
    rest = sa_url.split("://", 1)[1]
    creds, hostpart = rest.split("@", 1)
    host, db = hostpart.rsplit("/", 1)
    if ":" not in host:
        host = host + ":5432"
    return f"jdbc:postgresql://{host}/{db}?currentSchema={schema}", creds

def main():
    args = parse_args()
    cfg = load_cfg(args.config)

    dev = cfg["dev"]
    jdbc_url, creds = sqlalchemy_to_jdbc_pg(dev["url"], dev.get("schema", "public"))
    user, passwd = creds.split(":", 1)

    jar = JAR_PATH
    if not os.path.exists(jar):
        print(f"[error] JAR not found at {jar}. Build it with: mvn -q -DskipTests package (inside dbunit_runner)", file=sys.stderr)
        sys.exit(3)

    xml_files = sorted(glob.glob(os.path.join(args.datasets_dir, "*.xml")))
    if not xml_files:
        print("[error] No XML files found. Did you run export_sandbox.py?", file=sys.stderr)
        sys.exit(2)

    failures = 0
    for xml in xml_files:
        # Table tag equals schema-qualified table name; our exporter used filename like schema_table.xml
        table_name = os.path.basename(xml).replace(".xml", "").replace("_", ".")
        print(f"[DBUnit] Compare {table_name}")
        cmd = ["java", "-jar", jar, jdbc_url, user, passwd, xml, table_name, args.exclude_cols]
        rc = subprocess.call(cmd)
        if rc != 0:
            failures += 1

    if failures:
        print(f"[DBUnit] Completed with {failures} failure(s).")
        sys.exit(1)
    print("[DBUnit] All comparisons passed.")

if __name__ == "__main__":
    main()
