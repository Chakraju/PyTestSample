import json
import yaml
import os
import pytest
from tools.normalize import canonicalize

SNAP_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "snapshots")

def _load(path):
    with open(path, "r", encoding="utf-8") as f:
        if path.endswith(".json"):
            return json.load(f)
        return yaml.safe_load(f)

def pick_formats():
    sandbox_json = os.path.join(SNAP_DIR, "sandbox.json")
    dev_json = os.path.join(SNAP_DIR, "dev.json")
    sandbox_yaml = os.path.join(SNAP_DIR, "sandbox.yaml")
    dev_yaml = os.path.join(SNAP_DIR, "dev.yaml")
    if os.path.exists(sandbox_json) and os.path.exists(dev_json):
        return sandbox_json, dev_json
    if os.path.exists(sandbox_yaml) and os.path.exists(dev_yaml):
        return sandbox_yaml, dev_yaml
    raise pytest.SkipTest("No snapshots found. Run tools/export_snapshots.py first.")

def load_compare_config():
    import yaml
    with open("config.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    cmp_cfg = cfg.get("compare", {})
    ignore_keys = set(cmp_cfg.get("ignore_keys", []))
    ignore_sections = set(cmp_cfg.get("ignore_sections", []))
    normalize_sql_keys = set(cmp_cfg.get("normalize_sql_keys", []))
    include_root_keys = set(cmp_cfg.get("include_root_keys", []))
    return ignore_keys, ignore_sections, normalize_sql_keys, include_root_keys

def filter_root_keys(doc, include_root_keys, ignore_sections):
    if not isinstance(doc, dict):
        return doc
    pruned = {}
    for k, v in doc.items():
        if k in ignore_sections:
            continue
        if include_root_keys and k not in include_root_keys:
            continue
        pruned[k] = v
    return pruned

def test_schemacrawler_snapshots_equivalent():
    s_path, d_path = pick_formats()
    sandbox = _load(s_path)
    dev = _load(d_path)

    ignore_keys, ignore_sections, normalize_sql_keys, include_root_keys = load_compare_config()

    sandbox = filter_root_keys(sandbox, include_root_keys, ignore_sections)
    dev = filter_root_keys(dev, include_root_keys, ignore_sections)

    can_s = canonicalize(sandbox, ignore_keys=ignore_keys, normalize_sql_keys=normalize_sql_keys)
    can_d = canonicalize(dev, ignore_keys=ignore_keys, normalize_sql_keys=normalize_sql_keys)

    assert can_s == can_d, "SchemaCrawler snapshots differ after normalization"
