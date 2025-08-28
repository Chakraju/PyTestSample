from collections import defaultdict
from deepdiff import DeepDiff
import os
import pytest
from utils_sql import (
    SQL_LIST_COLUMNS, SQL_PK, SQL_UNIQUES, SQL_FKS, SQL_VIEWS, SQL_FUNCTIONS,
    SQL_ROLES, SQL_ROLE_MEMBERS, SQL_SEQUENCES, SQL_SEQUENCE_OWNED_BY,
    SQL_INDEXES, SQL_TRIGGERS, SQL_TABLE_OWNERS, SQL_TABLE_PRIVILEGES,
)

def _norm_sql(s): return " ".join((s or "").split())
def _as_list(v):
    if v is None: return []
    return list(v) if isinstance(v, (list, tuple)) else [v]

# ---------- TABLES (parametrized per snapshot) ----------

@pytest.mark.parametrize("snap_path", ids=lambda p: os.path.basename(p), argvalues=lambda snapshots: snapshots["table_files"])
def test_table_structure(dev_engine, snapshots, include_exclude, ignore, snap_path):
    """Validate columns, PK, UNIQUE, FKs of each table snapshot."""
    snap = snapshots["_loader"](snap_path)
    schema, table = snap["schema"], snap["name"]
    if snapshots["_ignore_table"](ignore, schema, table):
        pytest.skip(f"ignored by pattern: {schema}.{table}")

    with dev_engine.connect() as conn:
        cols = conn.execute(SQL_LIST_COLUMNS, {"schema": schema, "table": table}).all()
        assert cols, f"Missing table {schema}.{table}"

        actual_cols = {
            r.column_name: {
                "data_type": r.udt_name or r.data_type or "",
                "is_nullable": (r.is_nullable or ""),
                "char_max": r.character_maximum_length,
                "num_precision": r.numeric_precision,
                "num_scale": r.numeric_scale,
                "datetime_precision": r.datetime_precision,
                "default": r.column_default,
                "is_identity": (r.is_identity or ""),
            }
            for r in cols
            if r.column_name not in set(ignore["columns"].get(f"{schema}.{table}", []))
        }

        want_cols = {
            c["name"]: {k:v for k,v in c.items() if k not in {"name"} | ignore["column_fields"]}
            for c in snap.get("columns", [])
            if c["name"] not in set(ignore["columns"].get(f"{schema}.{table}", []))
        }

        diff = DeepDiff(want_cols, actual_cols, ignore_order=True)
        assert not diff, f"{schema}.{table} columns differ:\n{diff}"

        # PK
        pk = conn.execute(SQL_PK, {"schema": schema, "table": table}).all()
        want_pk = _as_list((snap.get("primary_key") or {}).get("columns"))
        have_pk = _as_list(pk[0]._mapping.get("colnames")) if pk else []
        assert want_pk == have_pk, f"{schema}.{table} PK mismatch: want {want_pk}, have {have_pk}"

        # UNIQUE
        have_uqs = [tuple(_as_list(u._mapping.get("colnames"))) for u in conn.execute(SQL_UNIQUES, {"schema": schema, "table": table}).all()]
        want_uqs = [tuple(u.get("columns", [])) for u in (snap.get("uniques") or [])]
        assert sorted(want_uqs) == sorted(have_uqs), f"{schema}.{table} UNIQUE mismatch: want {sorted(want_uqs)} vs {sorted(have_uqs)}"

        # FKs
        fk_rows = conn.execute(SQL_FKS, {"schema": schema, "table": table}).all()
        grouped = defaultdict(list)
        for r in fk_rows:
            grouped[r.constraint_name].append(r)
        have_fks = sorted(
            (cname,
             rows[0].foreign_table_schema,
             rows[0].foreign_table_name,
             tuple((r.column_name, r.foreign_column_name) for r in sorted(rows, key=lambda x: x.ordinal_position or 0)))
            for cname, rows in grouped.items()
        )
        want_fks = sorted(
            (fk.get("name"), fk.get("ref_schema"), fk.get("ref_table"),
             tuple((m["local"], m["remote"]) for m in fk.get("columns", [])))
            for fk in (snap.get("foreign_keys") or [])
        )
        assert want_fks == have_fks, f"{schema}.{table} FK mismatch:\nwant={want_fks}\nhave={have_fks}"

# ---------- GLOBAL OBJECTS ----------

def test_views(dev_engine, snapshots, include_exclude):
    if "views" not in snapshots["globals"]: pytest.skip("no views snapshot")
    want = {(v["schema"], v["name"]): _norm_sql(v["definition_norm"]) for v in snapshots["_loader"](snapshots["globals"]["views"])}
    with dev_engine.connect() as conn:
        rows = conn.execute(SQL_VIEWS, {"include_schemas": list({s for s,_ in want.keys()}), "exclude_schemas": tuple([])}).all()
        have = {(r.table_schema, r.table_name): _norm_sql(r.definition) for r in rows}
    assert want.keys() <= have.keys(), f"missing views: {sorted(want.keys()-have.keys())}"
    diffs = [(k, want[k], have[k]) for k in want if have.get(k) != want[k]]
    assert not diffs, f"view definitions differ: {diffs}"

def test_functions(dev_engine, snapshots, include_exclude):
    import hashlib
    if "functions" not in snapshots["globals"]: pytest.skip("no functions snapshot")
    want = {(f["schema"], f["name"], f.get("args","")): f["definition_hash"] for f in snapshots["_loader"](snapshots["globals"]["functions"])}
    with dev_engine.connect() as conn:
        rows = conn.execute(SQL_FUNCTIONS, {"include_schemas": list({k[0] for k in want}), "exclude_schemas": tuple([])}).all()
        have = {(r.schema, r.name, r.args or ""): hashlib.sha256(_norm_sql(r.definition).encode("utf-8")).hexdigest() for r in rows}
    assert want.keys() <= have.keys(), f"missing functions: {sorted(want.keys()-have.keys())}"
    changed = [k for k in want if have.get(k) != want[k]]
    assert not changed, f"function changed: {changed}"

def test_roles_and_memberships(dev_engine, snapshots):
    if "roles" not in snapshots["globals"]: pytest.skip("no roles snapshot")
    want_roles = {r["name"] for r in snapshots["_loader"](snapshots["globals"]["roles"])}
    want_rm = {(m["role"], m["member"]) for m in snapshots["_loader"](snapshots["globals"]["role_memberships"])}
    with dev_engine.connect() as conn:
        have_roles = {r.rolname for r in conn.execute(SQL_ROLES)}
        have_rm = {(r.role, r.member) for r in conn.execute(SQL_ROLE_MEMBERS)}
    assert want_roles <= have_roles, f"missing roles: {sorted(want_roles-have_roles)}"
    assert want_rm == have_rm, f"role memberships differ"

def test_sequences(dev_engine, snapshots, include_exclude):
    if "sequences" not in snapshots["globals"]: pytest.skip("no sequences snapshot")
    want = {(s["schema"], s["name"]) for s in snapshots["_loader"](snapshots["globals"]["sequences"])}
    with dev_engine.connect() as conn:
        rows = conn.execute(SQL_SEQUENCES, {"include_schemas": list({s for s,_ in want}), "exclude_schemas": tuple([])}).all()
        have = {(r.sequence_schema, r.sequence_name) for r in rows}
    assert want <= have, f"missing sequences: {sorted(want-have)}"

def test_sequence_ownerships(dev_engine, snapshots):
    if "sequence_ownerships" not in snapshots["globals"]: pytest.skip("no seq ownerships snapshot")
    want = {(o["schema"], o["sequence"], o.get("table_schema",""), o.get("table",""), o.get("column",""))
            for o in snapshots["_loader"](snapshots["globals"]["sequence_ownerships"])}
    with dev_engine.connect() as conn:
        have = set()
        for r in conn.execute(SQL_SEQUENCE_OWNED_BY):
            if r.schema_name:
                have.add((r.schema_name, r.sequence_name, r.table_schema or "", r.table_name or "", r.column_name or ""))
    assert want == have, f"sequence ownerships differ"

def test_indexes(dev_engine, snapshots, include_exclude, ignore):
    if "indexes" not in snapshots["globals"]: pytest.skip("no indexes snapshot")
    want = {(i["schema"], i["table"], i["name"], i["definition"]) for i in snapshots["_loader"](snapshots["globals"]["indexes"])}
    want = {t for t in want if not snapshots["_ignore_table"](ignore, t[0], t[1])}
    with dev_engine.connect() as conn:
        rows = conn.execute(SQL_INDEXES, {"include_schemas": list({s for s,_,_,_ in want}), "exclude_schemas": tuple([])}).all()
        have = {(r.schemaname, r.tablename, r.indexname, r.indexdef) for r in rows if not snapshots["_ignore_table"](ignore, r.schemaname, r.tablename)}
    assert want == have, f"indexes differ"

def test_triggers(dev_engine, snapshots, include_exclude, ignore):
    if "triggers" not in snapshots["globals"]: pytest.skip("no triggers snapshot")
    want = {(t["table_schema"], t["table"], t["name"], t.get("timing",""), tuple(t.get("events", []))) for t in snapshots["_loader"](snapshots["globals"]["triggers"])} 
    want = {w for w in want if not snapshots["_ignore_table"](ignore, w[0], w[1])}
    with dev_engine.connect() as conn:
        rows = conn.execute(SQL_TRIGGERS, {"include_schemas": list({w[0] for w in want}), "exclude_schemas": tuple([])}).all()
        from collections import defaultdict
        trig = defaultdict(lambda: {"timing": None, "events": set()})
        for r in rows:
            if snapshots["_ignore_table"](ignore, r.table_schema, r.table_name): continue
            key = (r.table_schema, r.table_name, r.trigger_name)
            trig[key]["timing"] = r.action_timing
            trig[key]["events"].add(r.event_manipulation)
        have = {(k[0], k[1], k[2], v["timing"] or "", tuple(sorted(v["events"]))) for k, v in trig.items()}
    assert want == have, f"triggers differ"

def test_table_owners(dev_engine, snapshots, include_exclude, ignore):
    if "table_owners" not in snapshots["globals"]: pytest.skip("no table owners snapshot")
    want = {(o["schema"], o["table"], o["owner"]) for o in snapshots["_loader"](snapshots["globals"]["table_owners"]) if not snapshots["_ignore_table"](ignore, o["schema"], o["table"])}
    with dev_engine.connect() as conn:
        rows = conn.execute(SQL_TABLE_OWNERS, {"include_schemas": list({s for s,_,_ in want}), "exclude_schemas": tuple([])}).all()
        have = {(r.schema, r.table, r.owner) for r in rows if not snapshots["_ignore_table"](ignore, r.schema, r.table)}
    assert want == have, f"table owners differ"

def test_table_privileges(dev_engine, snapshots, include_exclude, ignore):
    if "table_privileges" not in snapshots["globals"]: pytest.skip("no table privileges snapshot")
    want = {(g["schema"], g["table"], g["grantee"], g["privilege"], g.get("is_grantable","false"))
            for g in snapshots["_loader"](snapshots["globals"]["table_privileges"]) 
            if not snapshots["_ignore_table"](ignore, g["schema"], g["table"]) 
            and g.get("grantee") not in ignore["grants_ignore_grantees"] 
            and g.get("privilege") not in ignore["grants_ignore_privs"]}
    with dev_engine.connect() as conn:
        rows = conn.execute(SQL_TABLE_PRIVILEGES, {"include_schemas": list({s for s, *_ in want}), "exclude_schemas": tuple([])}).all()
        have = {(r.table_schema, r.table_name, r.grantee, r.privilege_type, str(r.is_grantable).lower())
                for r in rows if not snapshots["_ignore_table"](ignore, r.table_schema, r.table_name)
                and r.grantee not in ignore["grants_ignore_grantees"] and r.privilege_type not in ignore["grants_ignore_privs"]}
    assert want == have, f"table privileges differ"
