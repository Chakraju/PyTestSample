import sys
import os
import pytest

def run_pytest(release: str):
    test_dir = os.path.join("generated_tests", release)
    if not os.path.exists(test_dir):
        print(f"[ERROR] Test directory not found: {test_dir}")
        sys.exit(1)

    report_file = f"report_{release}.html"
    print(f"[INFO] Running tests in {test_dir}")
    print(f"[INFO] Generating report: {report_file}")

    result = pytest.main([
        test_dir,
        "--html=" + report_file,
        "--self-contained-html"
    ])

    if result == 0:
        print(f"[SUCCESS] Tests passed. Report saved to {report_file}")
    else:
        print(f"[FAILURE] Some tests failed. See {report_file} for details")
        sys.exit(result)

def main():
    if len(sys.argv) < 2:
        print("Usage: python execute_tests.py <release>")
        sys.exit(1)
    run_pytest(sys.argv[1])

if __name__ == "__main__":
    main()
