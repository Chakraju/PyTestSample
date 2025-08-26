import argparse, os, json, yaml, difflib
from sqlalchemy import create_engine
from utils_sql import *

def norm_sql(s): return " ".join((s or "").split())

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_snapshot_tree(snap_dir):
    tree = {"tables": {}, "views": [], "functions": [], "roles": [], "role_memberships": [],
            "sequences": [], "sequence_ownerships": [], "indexes": [], "triggers": [],
            "table_owners": [], "table_privileges": []}
    tdir = os.path.join(snap_dir, "tables")
    if os.path.isdir(tdir):
        for f in sorted(os.listdir(tdir)):
            if f.endswith(".json"):
                obj = load_json(os.path.join(tdir, f))
                tree["tables"][f[:-5]] = obj
    for name in ["views","functions","roles","role_memberships","sequences","sequence_ownerships","indexes","triggers","table_owners","table_privileges"]:
        p = os.path.join(snap_dir, f"{name}.json")
        if os.path.exists(p):
            tree[name] = load_json(p)
    return tree

def materialize_dev(conn, include_schemas, exclude_schemas):
    mat = {"tables": {}, "views": [], "functions": [], "roles": [], "role_memberships": [],
           "sequences": [], "sequence_ownerships": [], "indexes": [], "triggers": [],
           "table_owners": [], "table_privileges": []}

    from collections import defaultdict
    rows = conn.execute(SQL_LIST_TABLES, {"include_schemas": include_schemas, "exclude_schemas": tuple(exclude_schemas), "table_like": "%"}).all()
    for r in rows:
        schema, table = r.table_schema, r.table_name
        cols = conn.execute(SQL_LIST_COLUMNS, {"schema": schema, "table": table}).all()
        entry = {"schema": schema, "name": table, "columns": []}
        for c in cols:
            entry["columns"].append({
                "name": c.column_name,
                "data_type": c.udt_name or c.data_type or "",
                "is_nullable": (c.is_nullable or ""),
                "char_max": c.character_maximum_length,
                "num_precision": c.numeric_precision,
                "num_scale": c.numeric_scale,
                "datetime_precision": c.datetime_precision,
                "default": c.column_default,
                "is_identity": (c.is_identity or ""),
            })
        pk = conn.execute(SQL_PK, {"schema": schema, "table": table}).all()
        if pk:
            entry["primary_key"] = {"name": pk[0]._mapping.get('constraint_name'), "columns": list(pk[0]._mapping.get('colnames') or [])}
        uqs = conn.execute(SQL_UNIQUES, {"schema": schema, "table": table}).all()
        if uqs:
            entry["uniques"] = [{"name": u._mapping.get('constraint_name'), "columns": list(u._mapping.get('colnames') or [])} for u in uqs]
        fk_rows = conn.execute(SQL_FKS, {"schema": schema, "table": table}).all()
        if fk_rows:
            grp = defaultdict(list)
            for r2 in fk_rows:
                grp[r2.constraint_name].append(r2)
            fks = []
            for cname, lst in grp.items():
                lst = sorted(lst, key=lambda x: x.ordinal_position or 0)
                fks.append({"name": cname, "ref_schema": lst[0].foreign_table_schema, "ref_table": lst[0].foreign_table_name,
                            "columns": [{"local": x.column_name, "remote": x.foreign_column_name} for x in lst]})
            entry["foreign_keys"] = fks
        mat["tables"][f"{schema}.{table}"] = entry

    vrows = conn.execute(SQL_VIEWS, {"include_schemas": include_schemas, "exclude_schemas": tuple(exclude_schemas)}).all()
    mat["views"] = [{"schema": v.table_schema, "name": v.table_name, "definition_norm": norm_sql(v.definition)} for v in vrows]

    frows = conn.execute(SQL_FUNCTIONS, {"include_schemas": include_schemas, "exclude_schemas": tuple(exclude_schemas)}).all()
    import hashlib
    mat["functions"] = [{"schema": f.schema, "name": f.name, "args": f.args or "", "returns": f.returns or "", "language": f.language or "",
                         "definition_hash": hashlib.sha256(norm_sql(f.definition).encode('utf-8')).hexdigest()} for f in frows]

    rrows = conn.execute(SQL_ROLES).all()
    mat["roles"] = [{"name": r.rolname, "can_login": bool(r.rolcanlogin), "superuser": bool(r.rolsuper), "inherit": bool(r.rolinherit),
                     "createrole": bool(r.rolcreaterole), "createdb": bool(r.rolcreatedb), "replication": bool(r.rolreplication)} for r in rrows]
    mat["role_memberships"] = [{"role": rm.role, "member": rm.member} for rm in conn.execute(SQL_ROLE_MEMBERS).all()]

    srows = conn.execute(SQL_SEQUENCES, {"include_schemas": include_schemas, "exclude_schemas": tuple(exclude_schemas)}).all()
    mat["sequences"] = [{"schema": s.sequence_schema, "name": s.sequence_name, "data_type": s.data_type,
                         "start": str(s.start_value), "min": str(s.minimum_value), "max": str(s.maximum_value),
                         "increment": str(s.increment), "cycle": str(s.cycle_option).lower()} for s in srows]
    orows = conn.execute(SQL_SEQUENCE_OWNED_BY).all()
    mat["sequence_ownerships"] = [{"schema": o.schema_name, "sequence": o.sequence_name,
                                   "table_schema": o.table_schema or "", "table": o.table_name or "", "column": o.column_name or ""}
                                  for o in orows if o.schema_name]

    irows = conn.execute(SQL_INDEXES, {"include_schemas": include_schemas, "exclude_schemas": tuple(exclude_schemas)}).all()
    mat["indexes"] = [{"schema": i.schemaname, "table": i.tablename, "name": i.indexname, "definition": i.indexdef} for i in irows]

    trows = conn.execute(SQL_TRIGGERS, {"include_schemas": include_schemas, "exclude_schemas": tuple(exclude_schemas)}).all()
    from collections import defaultdict
    trig = defaultdict(lambda: {"table_schema": None, "table": None, "name": None, "timing": None, "events": set()})
    for tr in trows:
        key = (tr.table_schema, tr.table_name, tr.trigger_name)
        trig[key]["table_schema"] = tr.table_schema
        trig[key]["table"] = tr.table_name
        trig[key]["name"] = tr.trigger_name
        trig[key]["timing"] = tr.action_timing
        trig[key]["events"].add(tr.event_manipulation)
    mat["triggers"] = [{"table_schema": v["table_schema"], "table": v["table"], "name": v["name"], "timing": v["timing"], "events": sorted(v["events"])} for _, v in trig.items()]

    orows2 = conn.execute(SQL_TABLE_OWNERS, {"include_schemas": include_schemas, "exclude_schemas": tuple(exclude_schemas)}).all()
    mat["table_owners"] = [{"schema": r.schema, "table": r.table, "owner": r.owner} for r in orows2]

    grows = conn.execute(SQL_TABLE_PRIVILEGES, {"include_schemas": include_schemas, "exclude_schemas": tuple(exclude_schemas)}).all()
    mat["table_privileges"] = [{"schema": g.table_schema, "table": g.table_name, "grantee": g.grantee,
                                "privilege": g.privilege_type, "is_grantable": str(g.is_grantable).lower()} for g in grows]
    return mat

def main():
    ap = argparse.ArgumentParser(description="HTML diff of snapshot JSON vs live Dev (materialized).")
    ap.add_argument("--config", required=True)
    ap.add_argument("--snapshots", default="snapshots")
    ap.add_argument("--out", default="reports/json_vs_live_diff.html")
    args = ap.parse_args()

    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f)
    engine = create_engine(cfg["dev"]["url"], future=True)
    sb = cfg.get('sandbox', {})
    include_schemas = sb.get('include_schemas', ['public'])
    exclude_schemas = sb.get('exclude_schemas', ['pg_catalog','information_schema'])

    snap_tree = load_snapshot_tree(args.snapshots)

    with engine.connect() as conn:
        live = materialize_dev(conn, include_schemas, exclude_schemas)

    s_text = json.dumps(snap_tree, indent=2, sort_keys=True, ensure_ascii=False).splitlines()
    d_text = json.dumps(live, indent=2, sort_keys=True, ensure_ascii=False).splitlines()

    html = difflib.HtmlDiff(wrapcolumn=120).make_file(s_text, d_text, fromdesc="Snapshot (JSON files)", todesc="Dev (live materialized)")
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[diff] wrote {args.out}")

if __name__ == "__main__":
    main()
