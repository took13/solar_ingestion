from __future__ import annotations

from src.main import build_app


def main():
    app = build_app()
    app.run_job("inverter_history_nearline")


if __name__ == "__main__":
    main()