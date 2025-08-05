import subprocess
import sys

def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py <release>")
        sys.exit(1)

    release = sys.argv[1]

    print(f"[INFO] Generating test cases for release: {release}")
    subprocess.run(["python", "generate_tests.py", release], check=True)

    print(f"[INFO] Executing tests for release: {release}")
    subprocess.run(["python", "execute_tests.py", release], check=True)

if __name__ == "__main__":
    main()
