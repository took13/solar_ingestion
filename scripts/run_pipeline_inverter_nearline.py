from src.main import build_app
import subprocess
import sys


def main():
    app = build_app()

    print("=== INGESTION ===")
    app.run_job("inverter_history_nearline")

    print("=== NORMALIZE (GENERIC DEVICE) ===")
    subprocess.run([sys.executable, "-m", "scripts.run_normalize_generic"], check=False)

    print("=== DONE ===")


if __name__ == "__main__":
    main()
