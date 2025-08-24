import argparse, os, json, yaml, difflib, datetime
from tools.normalize import canonicalize

SNAP_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "snapshots")
REPORTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "reports")

def _load(path):
    with open(path, "r", encoding="utf-8") as f:
        if path.endswith(".json"):
            return json.load(f)
        return yaml.safe_load(f)

def _dump_pretty(obj):
    return json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False)

def _pick_snapshots():
    s_json, d_json = os.path.join(SNAP_DIR, "sandbox.json"), os.path.join(SNAP_DIR, "dev.json")
    s_yaml, d_yaml = os.path.join(SNAP_DIR, "sandbox.yaml"), os.path.join(SNAP_DIR, "dev.yaml")
    if os.path.exists(s_json) and os.path.exists(d_json):
        return s_json, d_json
    if os.path.exists(s_yaml) and os.path.exists(d_yaml):
        return s_yaml, d_yaml
    raise FileNotFoundError("No snapshots found. Expected sandbox/dev .json or .yaml in snapshots/.")

def _load_compare_config(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cmp_cfg = cfg.get("compare", {})
    return {
        "ignore_keys": set(cmp_cfg.get("ignore_keys", [])),
        "ignore_sections": set(cmp_cfg.get("ignore_sections", [])),
        "normalize_sql_keys": set(cmp_cfg.get("normalize_sql_keys", [])),
        "include_root_keys": set(cmp_cfg.get("include_root_keys", [])),
    }

def _filter_root_keys(doc, include_root_keys, ignore_sections):
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

def main():
    ap = argparse.ArgumentParser(description="Generate an HTML diff report between Sandbox and Dev snapshots.")
    ap.add_argument("--config", required=True, help="Path to config.yaml")
    ap.add_argument("--out", default=os.path.join(REPORTS_DIR, "schema_diff.html"), help="Output HTML path")
    args = ap.parse_args()

    cfg = _load_compare_config(args.config)
    s_path, d_path = _pick_snapshots()
    sandbox = _load(s_path)
    dev = _load(d_path)

    sandbox = _filter_root_keys(sandbox, cfg["include_root_keys"], cfg["ignore_sections"])
    dev = _filter_root_keys(dev, cfg["include_root_keys"], cfg["ignore_sections"])

    can_s = canonicalize(sandbox, ignore_keys=cfg["ignore_keys"], normalize_sql_keys=cfg["normalize_sql_keys"])
    can_d = canonicalize(dev, ignore_keys=cfg["ignore_keys"], normalize_sql_keys=cfg["normalize_sql_keys"])

    s_text = _dump_pretty(can_s).splitlines()
    d_text = _dump_pretty(can_d).splitlines()

    differ = difflib.HtmlDiff(wrapcolumn=120)
    html_table = differ.make_table(s_text, d_text, fromdesc="Sandbox (normalized)", todesc="Dev (normalized)", context=True, numlines=2)

    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Schema Diff Report</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 16px; }}
    h1 {{ font-size: 20px; }}
    .meta {{ color: #666; margin-bottom: 12px; }}
    table.diff {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace; font-size: 12px; }}
    td, th {{ padding: 2px 6px; }}
  </style>
</head>
<body>
  <h1>Schema Diff Report</h1>
  <div class="meta">
    Generated: {ts}<br/>
    Source: {os.path.basename(s_path)} vs {os.path.basename(d_path)} (after normalization)
  </div>
  {html_table}
  <p style="margin-top:12px;color:#666;">Legend: Left = Sandbox, Right = Dev. Only differences after normalization are highlighted.</p>
</body>
</html>"""

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[diff] Wrote HTML diff: {args.out}")

if __name__ == "__main__":
    main()
