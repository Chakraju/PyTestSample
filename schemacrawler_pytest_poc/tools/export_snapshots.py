import argparse
import os
import subprocess
import yaml

SNAP_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "snapshots")

def run_schemacrawler(cfg: dict, env_name: str, fmt: str) -> str:
    exe = cfg["schemacrawler"].get("executable", "schemacrawler")
    server = cfg["schemacrawler"].get("server", "postgresql")
    info_level = cfg["schemacrawler"].get("info_level", "maximum")
    command = cfg["schemacrawler"].get("command", "details")
    schemas = cfg["schemacrawler"].get("schemas", None)

    db = cfg[env_name]
    out_file = os.path.join(SNAP_DIR, f"{env_name}.{fmt}")
    os.makedirs(SNAP_DIR, exist_ok=True)

    cmd = [
        exe,
        f"--server={server}",
        f"--host={db['host']}",
        f"--port={db['port']}",
        f"--database={db['database']}",
        f"--user={db['user']}",
        f"--password={db['password']}",
        f"--info-level={info_level}",
        f"--command={command}",
        f"--output-format={fmt}",
        f"--output-file={out_file}",
    ]
    if schemas:
        cmd.append(f"--schemas={schemas}")

    print("[SC] Running:", " ".join(cmd))
    subprocess.check_call(cmd)
    return out_file

def main():
    ap = argparse.ArgumentParser(description="Export SchemaCrawler snapshots for Sandbox and Dev.")
    ap.add_argument("--config", required=True)
    ap.add_argument("--only", choices=["sandbox", "dev", "both"], default="both")
    args = ap.parse_args()

    with open(args.config, "r") as f:
        cfg = yaml.safe_load(f)

    formats = cfg["schemacrawler"].get("output_formats", ["json"])
    targets = ["sandbox", "dev"] if args.only == "both" else [args.only]

    for env_name in targets:
        for fmt in formats:
            run_schemacrawler(cfg, env_name, fmt)

    print("[SC] Snapshots exported to:", SNAP_DIR)

if __name__ == "__main__":
    main()
