import argparse, os, json, sys, hashlib, yaml
from sqlalchemy import create_engine
from utils_sql import *
from collections import defaultdict

def norm_sql(s: str) -> str:
    return " ".join((s or "").split())

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def cmp_tables(conn, snap_dir: str):
    rc = 0
    tables_dir = os.path.join(snap_dir, "tables")
    want_files = [f for f in os.listdir(tables_dir) if f.endswith(".json")]
    for fname in sorted(want_files):
        obj = load_json(os.path.join(tables_dir, fname))
        schema = obj["schema"]; name = obj["name"]
        rows = conn.execute(SQL_LIST_COLUMNS, {"schema": schema, "table": name}).all()
        if not rows:
            print(f"[FAIL] missing table {schema}.{name}"); rc = 1; continue

        actual = {}
        for r in rows:
            actual[r.column_name] = {
                "data_type": r.udt_name or r.data_type or "",
                "is_nullable": (r.is_nullable or ""),
                "char_max": r.character_maximum_length,
                "num_precision": r.numeric_precision,
                "num_scale": r.numeric_scale,
                "datetime_precision": r.datetime_precision,
                "default": r.column_default,
                "is_identity": (r.is_identity or ""),
            }

        want_cols = {c["name"]: c for c in obj.get("columns", [])}
        for cname, meta in want_cols.items():
            if cname not in actual:
                print(f"[FAIL] {schema}.{name} missing column {cname}"); rc = 1; continue
            for k, wv in meta.items():
                if k == "name": continue
                av = actual[cname].get(k)
                if (wv or None) != (av or None):
                    print(f"[FAIL] {schema}.{name}.{cname} mismatch {k}: expected '{wv}' got '{av}'"); rc = 1

        extra = set(actual.keys()) - set(want_cols.keys())
        if extra:
            print(f"[FAIL] {schema}.{name} has unexpected columns: {sorted(extra)}"); rc = 1

        # PK
        want_pk = obj.get("primary_key")
        pk_rows = conn.execute(SQL_PK, {"schema": schema, "table": name}).all()
        if want_pk:
            want_cols_list = list(want_pk.get("columns", []))
            if not pk_rows:
                print(f"[FAIL] {schema}.{name} missing primary key"); rc = 1
            else:
                have_cols = list(pk_rows[0].columns)
                if want_cols_list != have_cols:
                    print(f"[FAIL] {schema}.{name} PK columns differ: expected {want_cols_list} got {have_cols}"); rc = 1
        elif pk_rows:
            print(f"[FAIL] {schema}.{name} has PK but snapshot shows none"); rc = 1

        # UNIQUEs
        want_uqs = [tuple(u.get("columns", [])) for u in obj.get("uniques", []) or []]
        have_uqs = [tuple(u.columns) for u in conn.execute(SQL_UNIQUES, {"schema": schema, "table": name}).all()]
        if sorted(want_uqs) != sorted(have_uqs):
            print(f"[FAIL] {schema}.{name} UNIQUE sets differ: expected {sorted(want_uqs)} got {sorted(have_uqs)}"); rc = 1

        # FKs
        want_fks = []
        for fk in obj.get("foreign_keys", []) or []:
            maps = tuple((m["local"], m["remote"]) for m in fk.get("columns", []))
            want_fks.append((fk.get("name"), fk.get("ref_schema"), fk.get("ref_table"), maps))

        have_fk_rows = conn.execute(SQL_FKS, {"schema": schema, "table": name}).all()
        groups = defaultdict(list)
        for r in have_fk_rows:
            groups[r.constraint_name].append((r.column_name, r.foreign_column_name, r.foreign_table_schema, r.foreign_table_name))
        have_fks = []
        for cname, lst in groups.items():
            lst_sorted = sorted(lst, key=lambda x: x[0])
            ref_schema = lst_sorted[0][2]; ref_table = lst_sorted[0][3]
            maps = tuple((a,b) for a,b,_,_ in lst_sorted)
            have_fks.append((cname, ref_schema, ref_table, maps))

        if sorted(want_fks) != sorted(have_fks):
            print(f"[FAIL] {schema}.{name} FK sets differ:\n  expected {sorted(want_fks)}\n  got      {sorted(have_fks)}"); rc = 1

    return rc

def cmp_views(conn, snap_dir: str):
    rc = 0
    path = os.path.join(snap_dir, "views.json")
    if not os.path.exists(path): return 0
    want = {(v["schema"], v["name"]): norm_sql(v.get("definition_norm","")) for v in load_json(path)}
    rows = conn.execute(SQL_VIEWS, {
        "include_schemas": list({s for s,_ in want.keys()}),
        "exclude_schemas": tuple([])
    }).all()
    have = {(r.table_schema, r.table_name): norm_sql(r.definition) for r in rows}
    for key, wdef in want.items():
        if key not in have:
            print(f"[FAIL] missing view {key[0]}.{key[1]}"); rc = 1
        elif have[key] != wdef:
            print(f"[FAIL] view def differs for {key[0]}.{key[1]}"); rc = 1
    return rc

def cmp_functions(conn, snap_dir: str):
    rc = 0
    path = os.path.join(snap_dir, "functions.json")
    if not os.path.exists(path): return 0
    want = {(f["schema"], f["name"], f.get("args","")): f["definition_hash"] for f in load_json(path)}
    rows = conn.execute(SQL_FUNCTIONS, {
        "include_schemas": list({k[0] for k in want.keys()}),
        "exclude_schemas": tuple([])
    }).all()
    have = {}
    for r in rows:
        def_hash = hashlib.sha256(norm_sql(r.definition).encode("utf-8")).hexdigest()
        have[(r.schema, r.name, r.args or "")] = def_hash
    for key, wh in want.items():
        if key not in have:
            print(f"[FAIL] missing function {key}"); rc = 1
        elif have[key] != wh:
            print(f"[FAIL] function changed {key}"); rc = 1
    return rc

def cmp_roles(conn, snap_dir: str):
    rc = 0
    path = os.path.join(snap_dir, "roles.json")
    if os.path.exists(path):
        want_roles = {r["name"] for r in load_json(path)}
        have_roles = {r.rolname for r in conn.execute(SQL_ROLES)}
        missing = sorted(want_roles - have_roles)
        if missing:
            print(f"[FAIL] missing roles: {missing}"); rc = 1

    mpath = os.path.join(snap_dir, "role_memberships.json")
    if os.path.exists(mpath):
        want_m = {(m["role"], m["member"]) for m in load_json(mpath)}
        have_m = {(r.role, r.member) for r in conn.execute(SQL_ROLE_MEMBERS)}
        if want_m != have_m:
            miss = sorted(want_m - have_m); extra = sorted(have_m - want_m)
            if miss: print(f"[FAIL] missing role memberships: {miss}"); rc = 1
            if extra: print(f"[FAIL] unexpected role memberships: {extra}"); rc = 1
    return rc

def cmp_sequences(conn, snap_dir: str):
    rc = 0
    spath = os.path.join(snap_dir, "sequences.json")
    if os.path.exists(spath):
        want = {(s["schema"], s["name"]) for s in load_json(spath)}
        if want:
            rows = conn.execute(SQL_SEQUENCES, {
                "include_schemas": list({s for s,_ in want}),
                "exclude_schemas": tuple([])
            }).all()
            have = {(r.sequence_schema, r.sequence_name) for r in rows}
            missing = sorted(want - have)
            if missing:
                print(f"[FAIL] missing sequences: {missing}"); rc = 1
    opath = os.path.join(snap_dir, "sequence_ownerships.json")
    if os.path.exists(opath):
        want_owns = {(o["schema"], o["sequence"], o.get("table_schema",""), o.get("table",""), o.get("column","")) for o in load_json(opath)}
        have_owns = set()
        for r in conn.execute(SQL_SEQUENCE_OWNED_BY):
            if r.schema_name is None: continue
            have_owns.add((r.schema_name, r.sequence_name, r.table_schema or "", r.table_name or "", r.column_name or ""))
        if want_owns != have_owns:
            miss = sorted(want_owns - have_owns); extra = sorted(have_owns - want_owns)
            if miss: print(f"[FAIL] missing seq ownerships: {miss}"); rc = 1
            if extra: print(f"[FAIL] unexpected seq ownerships: {extra}"); rc = 1
    return rc

def main():
    ap = argparse.ArgumentParser(description="Validate Dev schema against JSON snapshots.")
    ap.add_argument("--config", required=True)
    ap.add_argument("--snapshots", default="snapshots")
    args = ap.parse_args()

    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f)
    engine = create_engine(cfg["dev"]["url"], future=True)

    rc = 0
    with engine.connect() as conn:
        rc |= cmp_tables(conn, args.snapshots)
        rc |= cmp_views(conn, args.snapshots)
        rc |= cmp_functions(conn, args.snapshots)
        rc |= cmp_roles(conn, args.snapshots)
        rc |= cmp_sequences(conn, args.snapshots)

    if rc:
        print("Schema validation completed with failures."); sys.exit(1)
    print("Schema validation passed."); sys.exit(0)

if __name__ == "__main__":
    main()
