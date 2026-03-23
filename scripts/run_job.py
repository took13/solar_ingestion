from __future__ import annotations

import argparse
from src.main import build_app


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True, help="Job name without .yaml, e.g. dev_history_default")
    args = parser.parse_args()

    app = build_app()
    app.run_job(args.job)


if __name__ == "__main__":
    main()