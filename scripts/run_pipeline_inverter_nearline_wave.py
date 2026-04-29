import sys

from src.main import build_app


def main():
    if len(sys.argv) < 2:
        raise SystemExit(
            "Usage: python -m scripts.run_pipeline_inverter_nearline_wave A|B|C"
        )

    wave_group = sys.argv[1].upper().strip()

    if wave_group not in {"A", "B", "C"}:
        raise SystemExit("wave_group must be A, B, or C")

    app = build_app()

    print("=== INGESTION ===")
    app.run_job("inverter_history_nearline", wave_group=wave_group)

    print("=== NORMALIZE (GENERIC DEVICE) ===")
    app.normalize_device_realtime()

    print("=== DONE ===")


if __name__ == "__main__":
    main()