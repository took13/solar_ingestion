from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(r"C:\SOLAR\solar_ingestion")
LOG_DIR = PROJECT_ROOT / "logs"
LOCK_FILE = LOG_DIR / "realtime_cycle.lock"


def main() -> int:
    args = parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"realtime_cycle_timed_{run_ts}.log"

    if LOCK_FILE.exists() and not args.force:
        print(f"Another realtime cycle is already running. Lock file exists: {LOCK_FILE}")
        print(f"If no task/process is running, delete it manually or rerun with --force.")
        return 2

    LOCK_FILE.write_text(datetime.now().isoformat(timespec="seconds"), encoding="utf-8")

    cycle_start = time.monotonic()
    exit_code = 0

    try:
        log(log_file, "==================================================")
        log(log_file, f"Realtime timed cycle started: {datetime.now()}")
        log(log_file, f"Project root: {PROJECT_ROOT}")
        log(log_file, f"Delay seconds: {args.delay_seconds}")
        log(log_file, "==================================================")

        run_step(
            log_file,
            "STEP 1 - Plant realtime",
            [sys.executable, "-m", "scripts.run_pipeline_plant_realtime"],
            dry_run=args.dry_run,
        )

        sleep_step(log_file, args.delay_seconds, "Delay after plant realtime", args.dry_run)

        run_step(
            log_file,
            "STEP 2 - Critical device realtime devType 10/17",
            [sys.executable, "-m", "scripts.run_pipeline_critical_realtime"],
            dry_run=args.dry_run,
        )

        sleep_step(log_file, args.delay_seconds, "Delay after critical realtime", args.dry_run)

        run_step(
            log_file,
            "STEP 3 - Inverter realtime selected plants",
            [sys.executable, "-m", "scripts.run_inverter_realtime_job"],
            dry_run=args.dry_run,
        )

        run_step(
            log_file,
            "STEP 4 - Realtime postprocess normalize + mart load",
            [
                sys.executable,
                "-m",
                "scripts.run_realtime_postprocess",
                "--lookback-minutes",
                str(args.lookback_minutes),
            ],
            dry_run=args.dry_run,
        )

    except Exception as exc:
        exit_code = 1
        log(log_file, f"[FAILED] {exc}")

    finally:
        total_seconds = time.monotonic() - cycle_start
        log(log_file, "==================================================")
        log(log_file, f"Realtime timed cycle finished: {datetime.now()}")
        log(log_file, f"TOTAL_SECONDS={total_seconds:.2f}")
        log(log_file, f"EXIT_CODE={exit_code}")
        log(log_file, "==================================================")

        if LOCK_FILE.exists():
            LOCK_FILE.unlink()

    if exit_code == 0:
        print(f"Completed. Log: {log_file}")
    else:
        print(f"Failed. Log: {log_file}")

    return exit_code


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run staggered realtime cycle with per-step timing."
    )

    parser.add_argument(
        "--delay-seconds",
        type=int,
        default=60,
        help="Delay between API groups. Default: 60.",
    )

    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=60,
        help="Postprocess lookback window. Default: 60.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print/log commands without executing them.",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore existing lock file.",
    )

    return parser.parse_args()


def run_step(log_file: Path, step_name: str, cmd: list[str], *, dry_run: bool) -> None:
    log(log_file, "")
    log(log_file, f"[START] {step_name}")
    log(log_file, f"[CMD] {' '.join(cmd)}")

    started = time.monotonic()

    if dry_run:
        log(log_file, f"[DRY-RUN] Skipped execution: {step_name}")
        elapsed = time.monotonic() - started
        log(log_file, f"[END] {step_name} SECONDS={elapsed:.2f}")
        return

    with subprocess.Popen(
        cmd,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    ) as proc:
        assert proc.stdout is not None

        for line in proc.stdout:
            log(log_file, line.rstrip("\n"))

        return_code = proc.wait()

    elapsed = time.monotonic() - started

    if return_code != 0:
        log(log_file, f"[FAILED] {step_name} RETURN_CODE={return_code} SECONDS={elapsed:.2f}")
        raise RuntimeError(f"{step_name} failed with return code {return_code}")

    log(log_file, f"[END] {step_name} SECONDS={elapsed:.2f}")


def sleep_step(log_file: Path, seconds: int, name: str, dry_run: bool) -> None:
    log(log_file, "")
    log(log_file, f"[START] {name} SECONDS={seconds}")

    started = time.monotonic()

    if not dry_run:
        time.sleep(seconds)

    elapsed = time.monotonic() - started
    log(log_file, f"[END] {name} SECONDS={elapsed:.2f}")


def log(log_file: Path, message: str) -> None:
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {message}"
    print(line)
    with log_file.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


if __name__ == "__main__":
    raise SystemExit(main())