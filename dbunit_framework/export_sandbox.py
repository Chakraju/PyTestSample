import argparse
import os
import yaml
from typing import List, Dict, Tuple
from sqlalchemy import create_engine, text
from xml.etree.ElementTree import Element, tostring
from xml.dom import minidom

LIST_TABLES_SQL = """
SELECT t.table_schema, t.table_name
FROM information_schema.tables t
WHERE t.table_type = 'BASE TABLE'
  AND t.table_schema NOT IN :exclude_schemas
  AND t.table_schema = ANY(:include_schemas)
  AND t.table_name LIKE :table_like
ORDER BY t.table_schema, t.table_name
"""

LIST_COLUMNS_SQL = """
SELECT c.column_name
FROM information_schema.columns c
WHERE c.table_schema = :schema
  AND c.table_name = :table
ORDER BY c.ordinal_position
"""

def parse_args():
    ap = argparse.ArgumentParser(description="Export Sandbox tables (discovered via SQL) as DBUnit Flat XML per table.")
    ap.add_argument("--config", required=True, help="Path to config.yaml")
    ap.add_argument("--outdir", default="datasets", help="Output directory for XML files")
    ap.add_argument("--only_tables", nargs="*", help="Optional explicit list of schema.table overrides discovery")
    return ap.parse_args()

def load_cfg(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def pretty_xml(elem):
    rough = tostring(elem, encoding="utf-8")
    return minidom.parseString(rough).toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")

def row_to_elem(tag_name, row: dict):
    el = Element(tag_name)
    for k, v in row.items():
        if v is None:
            continue  # DBUnit NULL: omit attribute
        el.set(k, "" if v == "" else str(v))
    return el

def build_dataset(tag_name, rows):
    ds = Element("dataset")
    for r in rows:
        ds.append(row_to_elem(tag_name, r))
    return ds

def discover_tables(conn, include_schemas: List[str], exclude_schemas: List[str], like: str) -> List[Tuple[str,str]]:
    # For Postgres, SQLAlchemy/psycopg2 passes arrays using tuple/list bindings.
    res = conn.execute(
        text(LIST_TABLES_SQL),
        {
            "exclude_schemas": tuple(exclude_schemas),
            "include_schemas": include_schemas,
            "table_like": like
        }
    )
    return [(r[0], r[1]) for r in res]

def fetch_columns(conn, schema: str, table: str) -> List[str]:
    res = conn.execute(text(LIST_COLUMNS_SQL), {"schema": schema, "table": table})
    return [r[0] for r in res]

def export_table(conn, schema: str, table: str, cols: List[str], outdir: str, row_limit: int | None):
    col_list = ", ".join([f'"{c}"' for c in cols]) if cols else "*"
    sql = f'SELECT {col_list} FROM "{schema}"."{table}"'
    if row_limit:
        sql += f" LIMIT {int(row_limit)}"
    rows = [dict(r) for r in conn.execute(text(sql)).mappings()]
    tag = f"{schema}.{table}"  # schema-qualified tag, consistent with DBUnit runner
    xml = pretty_xml(build_dataset(tag, rows))
    safe = f"{schema}_{table}.xml"
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, safe), "w", encoding="utf-8") as f:
        f.write(xml)

def main():
    args = parse_args()
    cfg = load_cfg(args.config)
    sb = cfg["sandbox"]

    include_schemas = sb.get("include_schemas", ["public"])
    exclude_schemas = sb.get("exclude_schemas", ["pg_catalog", "information_schema"])
    table_like = sb.get("table_like", "%")
    row_limit = sb.get("row_limit", None)

    engine = create_engine(sb["url"], future=True)
    with engine.connect() as conn:
        if args.only_tables:
            targets = []
            for fq in args.only_tables:
                if "." in fq:
                    s, t = fq.split(".", 1)
                else:
                    s, t = sb.get("schema", "public"), fq
                targets.append((s, t))
        else:
            targets = discover_tables(conn, include_schemas, exclude_schemas, table_like)

        for schema, table in targets:
            print(f"[export] {schema}.{table}")
            cols = fetch_columns(conn, schema, table)
            export_table(conn, schema, table, cols, args.outdir, row_limit)

    print(f"Done. XML datasets written to: {args.outdir}")

if __name__ == "__main__":
    main()
