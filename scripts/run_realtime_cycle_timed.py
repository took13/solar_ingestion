from __future__ import annotations

import argparse
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass
class StepResult:
    name: str
    command: list[str]
    started_at_utc: datetime
    finished_at_utc: datetime
    elapsed_seconds: float
    exit_code: int


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _run_step(name: str, module: str, extra_args: list[str] | None = None) -> StepResult:
    cmd = [sys.executable, "-m", module]
    if extra_args:
        cmd.extend(extra_args)

    print("=" * 80, flush=True)
    print(f"[STEP] START {name}", flush=True)
    print(f"[STEP] CMD   {' '.join(cmd)}", flush=True)

    started = _utcnow()
    t0 = time.perf_counter()
    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    elapsed = time.perf_counter() - t0
    finished = _utcnow()

    print(
        f"[STEP] END   {name} exit_code={result.returncode} elapsed_seconds={elapsed:.2f}",
        flush=True,
    )

    return StepResult(
        name=name,
        command=cmd,
        started_at_utc=started,
        finished_at_utc=finished,
        elapsed_seconds=elapsed,
        exit_code=int(result.returncode),
    )


def _sleep_step(name: str, seconds: int) -> StepResult:
    seconds = max(0, int(seconds))

    print("=" * 80, flush=True)
    print(f"[STEP] START {name} sleep_seconds={seconds}", flush=True)

    started = _utcnow()
    t0 = time.perf_counter()
    if seconds > 0:
        time.sleep(seconds)
    elapsed = time.perf_counter() - t0
    finished = _utcnow()

    print(f"[STEP] END   {name} elapsed_seconds={elapsed:.2f}", flush=True)

    return StepResult(
        name=name,
        command=["sleep", str(seconds)],
        started_at_utc=started,
        finished_at_utc=finished,
        elapsed_seconds=elapsed,
        exit_code=0,
    )


def _print_summary(results: list[StepResult]) -> None:
    print("=" * 80, flush=True)
    print("[SUMMARY] Realtime cycle timing", flush=True)

    total_elapsed = sum(r.elapsed_seconds for r in results)
    failed = [r for r in results if r.exit_code != 0]

    for r in results:
        print(
            f"[SUMMARY] {r.name:<36} "
            f"exit_code={r.exit_code:<3} elapsed_seconds={r.elapsed_seconds:>8.2f}",
            flush=True,
        )

    print(f"[SUMMARY] TOTAL elapsed_seconds={total_elapsed:.2f}", flush=True)
    print(f"[SUMMARY] TOTAL elapsed_minutes={total_elapsed / 60.0:.2f}", flush=True)

    if failed:
        failed_names = ", ".join(r.name for r in failed)
        print(f"[SUMMARY] RESULT=FAILED failed_steps={failed_names}", flush=True)
    else:
        print("[SUMMARY] RESULT=SUCCESS", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Huawei realtime cycle with configurable inter-step delay and timing output."
    )
    parser.add_argument(
        "--delay-seconds",
        type=int,
        default=60,
        help="Delay between plant realtime -> critical realtime and critical realtime -> inverter realtime.",
    )
    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=60,
        help="Reserved for validation/postprocess compatibility. Current runner does not alter SQL lookback windows.",
    )
    parser.add_argument(
        "--skip-postprocess",
        action="store_true",
        help="Skip generic normalization and device mart load after inverter realtime.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue subsequent steps even if one step exits non-zero.",
    )

    args = parser.parse_args()

    print("=" * 80, flush=True)
    print("[RUN] Huawei realtime timed cycle", flush=True)
    print(f"[RUN] delay_seconds={args.delay_seconds}", flush=True)
    print(f"[RUN] lookback_minutes={args.lookback_minutes}", flush=True)
    print(f"[RUN] project_root={PROJECT_ROOT}", flush=True)

    results: list[StepResult] = []

    plan: list[tuple[str, str | None, list[str] | None]] = [
        ("STEP 1 Plant realtime", "scripts.run_pipeline_plant_realtime", None),
        ("Delay 1", None, [str(args.delay_seconds)]),
        ("STEP 2 Critical EMI/Meter realtime", "scripts.run_pipeline_critical_realtime", None),
        ("Delay 2", None, [str(args.delay_seconds)]),
        ("STEP 3 Inverter realtime selected-batch", "scripts.run_inverter_realtime_job", None),
    ]

    if not args.skip_postprocess:
        plan.extend(
            [
                ("STEP 4A Normalize realtime device raw", "scripts.run_normalize_generic", None),
                ("STEP 4B Load device mart 5min", "scripts.run_mart_device_5min", None),
            ]
        )

    for name, module, extra in plan:
        if module is None:
            result = _sleep_step(name, int(extra[0]) if extra else args.delay_seconds)
        else:
            result = _run_step(name, module, extra)

        results.append(result)

        if result.exit_code != 0 and not args.continue_on_error:
            print(f"[RUN] Stop because step failed: {name}", flush=True)
            break

    _print_summary(results)

    return 1 if any(r.exit_code != 0 for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
