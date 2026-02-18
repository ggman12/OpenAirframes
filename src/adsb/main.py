"""
Main pipeline for processing ADS-B data from adsb.lol.

Usage:
    python -m src.adsb.main --date 2026-01-01
    python -m src.adsb.main --start_date 2026-01-01 --end_date 2026-01-03
"""
import argparse
import subprocess
import sys
from datetime import datetime, timedelta

import polars as pl

from src.adsb.download_and_list_icaos import NUMBER_PARTS


def main():
    parser = argparse.ArgumentParser(description="Process ADS-B data for a single day or date range")
    parser.add_argument("--date", type=str, help="Single date in YYYY-MM-DD format")
    parser.add_argument("--start_date", type=str, help="Start date (inclusive, YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, help="End date (exclusive, YYYY-MM-DD)")
    parser.add_argument("--concat_with_latest_csv", action="store_true", help="Also concatenate with latest CSV from GitHub releases")
    args = parser.parse_args()

    if args.date and (args.start_date or args.end_date):
        raise SystemExit("Use --date or --start_date/--end_date, not both.")

    if args.date:
        start_date = datetime.strptime(args.date, "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)
    else:
        if not args.start_date or not args.end_date:
            raise SystemExit("Provide --start_date and --end_date, or use --date.")
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    current = start_date
    while current < end_date:
        date_str = current.strftime("%Y-%m-%d")
        print(f"Processing day: {date_str}")

        # Download and split
        subprocess.run([sys.executable, "-m", "src.adsb.download_and_list_icaos", "--date", date_str], check=True)

        # Process parts
        for part_id in range(NUMBER_PARTS):
            subprocess.run([sys.executable, "-m", "src.adsb.process_icao_chunk", "--part-id", str(part_id), "--date", date_str], check=True)

        # Concatenate
        concat_cmd = [sys.executable, "-m", "src.adsb.concat_parquet_to_final", "--date", date_str]
        if args.concat_with_latest_csv:
            concat_cmd.append("--concat_with_latest_csv")
        subprocess.run(concat_cmd, check=True)

        current += timedelta(days=1)

    if end_date - start_date > timedelta(days=1):
        dates = []
        cur = start_date
        while cur < end_date:
            dates.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
        csv_files = [
            f"data/outputs/openairframes_adsb_{d}_{d}.csv"
            for d in dates
        ]
        frames = [pl.read_csv(p) for p in csv_files]
        df = pl.concat(frames, how="vertical", rechunk=True)
        output_path = f"data/outputs/openairframes_adsb_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}.csv"
        df.write_csv(output_path)
        print(f"Wrote combined CSV: {output_path}")

    print("Done")


if __name__ == "__main__":
    main()