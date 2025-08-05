import psycopg2

def run_sql_script(conn, file_path):
    with open(file_path) as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def apply_release(conn, release_path, mode='forward'):
    script_file = f"{release_path}/{mode}.sql"
    run_sql_script(conn, script_file)
