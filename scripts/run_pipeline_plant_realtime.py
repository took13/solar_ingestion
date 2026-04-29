from src.main import build_app
import subprocess
import sys


def main():
    app = build_app()

    print("=== INGESTION ===")
    app.run_job("plant_realtime_online")

    print("=== NORMALIZE ===")
    subprocess.run([sys.executable, "-m", "scripts.run_normalize_plant_realtime"])

    print("=== MART ===")
    app.conn.cursor().execute("EXEC mart.usp_run_fact_plant_realtime_incremental")
    app.conn.commit()

    print("=== DONE ===")


if __name__ == "__main__":
    main()