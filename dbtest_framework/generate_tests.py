import os
import psycopg2
import yaml

def load_sql(filename):
    with open(os.path.join("sql", filename)) as f:
        return f.read()

def get_all_tables(conn):
    with conn.cursor() as cur:
        cur.execute(load_sql("all_tables.sql"))
        return cur.fetchall()

def extract_metadata(conn, schema, table):
    snapshot = {"columns": []}
    with conn.cursor() as cur:
        cur.execute(load_sql("columns.sql"), (schema, table))
        for row in cur.fetchall():
            snapshot["columns"].append({
                "name": row[0],
                "type": row[1],
                "nullable": row[2] == "YES",
                "default": row[3]
            })

        cur.execute(load_sql("primary_key.sql"), (schema, table))
        pk = [r[0] for r in cur.fetchall()]
        if pk:
            snapshot["primary_key"] = pk

        cur.execute(load_sql("unique_constraints.sql"), (schema, table))
        uc = [r[0] for r in cur.fetchall()]
        if uc:
            snapshot["unique_constraints"] = uc

        cur.execute(load_sql("triggers.sql"), (schema, table))
        triggers = [{"name": r[0], "event": r[1], "function": r[2]} for r in cur.fetchall()]
        if triggers:
            snapshot["triggers"] = triggers

    return snapshot

def save_yaml(table, snapshot):
    os.makedirs("expected/tables", exist_ok=True)
    with open(f"expected/tables/{table}.yaml", "w") as f:
        yaml.dump(snapshot, f, sort_keys=False)

def main():
    conn = psycopg2.connect(
        host="localhost",
        dbname="sandbox_db",
        user="sandbox_user",
        password="secret"
    )
    try:
        for schema, table in get_all_tables(conn):
            print(f"Generating snapshot for {schema}.{table}")
            snapshot = extract_metadata(conn, schema, table)
            save_yaml(table, snapshot)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
