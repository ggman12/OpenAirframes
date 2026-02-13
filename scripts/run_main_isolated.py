#!/usr/bin/env python3
"""
Run src.adsb.main in an isolated git worktree so edits in the main
working tree won't affect subprocess imports during the run.

Usage:
    python scripts/run_main_isolated.py 2026-01-01
    python scripts/run_main_isolated.py --start_date 2026-01-01 --end_date 2026-01-03
"""
import argparse
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess:
    print(f"\n>>> {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=cwd, check=check)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run src.adsb.main in an isolated worktree")
    parser.add_argument("date", nargs="?", help="Single date to process (YYYY-MM-DD)")
    parser.add_argument("--start_date", help="Start date (inclusive, YYYY-MM-DD)")
    parser.add_argument("--end_date", help="End date (exclusive, YYYY-MM-DD)")
    parser.add_argument("--concat_with_latest_csv", action="store_true", help="Also concatenate with latest CSV from GitHub releases")
    args = parser.parse_args()

    if args.date and (args.start_date or args.end_date):
        raise SystemExit("Use a single date or --start_date/--end_date, not both.")

    if args.date:
        datetime.strptime(args.date, "%Y-%m-%d")
        main_args = ["--date", args.date]
    else:
        if not args.start_date or not args.end_date:
            raise SystemExit("Provide --start_date and --end_date, or a single date.")
        datetime.strptime(args.start_date, "%Y-%m-%d")
        datetime.strptime(args.end_date, "%Y-%m-%d")
        main_args = ["--start_date", args.start_date, "--end_date", args.end_date]

    if args.concat_with_latest_csv:
        main_args.append("--concat_with_latest_csv")

    repo_root = Path(__file__).resolve().parents[1]
    snapshots_root = repo_root / ".snapshots"
    snapshots_root.mkdir(exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot_root = snapshots_root / f"run_{timestamp}"
    snapshot_src = snapshot_root / "src"

    exit_code = 0
    try:
        shutil.copytree(repo_root / "src", snapshot_src)

        runner = (
            "import sys, runpy; "
            f"sys.path.insert(0, {repr(str(snapshot_root))}); "
            f"sys.argv = ['src.adsb.main'] + {main_args!r}; "
            "runpy.run_module('src.adsb.main', run_name='__main__')"
        )
        cmd = [sys.executable, "-c", runner]
        run(cmd, cwd=repo_root)
    except subprocess.CalledProcessError as exc:
        exit_code = exc.returncode
    finally:
        shutil.rmtree(snapshot_root, ignore_errors=True)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
