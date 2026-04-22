from __future__ import annotations

from src.main import build_app


def main():
    app = build_app()
    app.run_job("plant_realtime_online")


if __name__ == "__main__":
    main()