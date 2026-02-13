#!/usr/bin/env python3
"""
Download ADS-B Exchange basic-ac-db.json.gz.

Usage:
    python -m src.contributions.create_daily_adsbexchange_release [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import shutil
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

URL = "https://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz"
OUT_ROOT = Path("data/openairframes")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create daily ADS-B Exchange JSON release")
    parser.add_argument("--date", type=str, help="Date to process (YYYY-MM-DD format, default: today UTC)")
    args = parser.parse_args()

    date_str = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    gz_path = OUT_ROOT / f"basic-ac-db_{date_str}.json.gz"

    print(f"Downloading {URL}...")
    req = Request(URL, headers={"User-Agent": "openairframes-downloader/1.0"}, method="GET")
    with urlopen(req, timeout=300) as r, gz_path.open("wb") as f:
        shutil.copyfileobj(r, f)

    print(f"Wrote: {gz_path}")


if __name__ == "__main__":
    main()
