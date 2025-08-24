import argparse, subprocess, sys, os

def main():
    ap = argparse.ArgumentParser(description="Export snapshots, generate diff report, and run pytest.")
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    # Export both snapshots
    subprocess.check_call([sys.executable, os.path.join("tools", "export_snapshots.py"), "--config", args.config])

    # Generate HTML diff (optional)
    try:
        subprocess.check_call([sys.executable, os.path.join("tools", "diff_report.py"), "--config", args.config])
    except Exception as e:
        print("[warn] Could not generate diff report:", e)

    # Run tests
    rc = subprocess.call(["pytest", "-q"])
    sys.exit(rc)

if __name__ == "__main__":
    main()
