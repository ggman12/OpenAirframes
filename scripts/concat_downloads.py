#!/usr/bin/env python3
import re
from pathlib import Path
import polars as pl

# Find all CSV.gz files in the downloaded artifacts
artifacts_dir = Path("downloads/adsb_artifacts")
files = sorted(artifacts_dir.glob("*/openairframes_adsb_*.csv.gz"))

if not files:
    raise SystemExit("No CSV.gz files found in downloads/adsb_artifacts/")

print(f"Found {len(files)} files to concatenate")

# Extract dates from filenames to determine range
def extract_dates(path: Path) -> tuple[str, str]:
    """Extract start and end dates from filename"""
    m = re.search(r"openairframes_adsb_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.csv\.gz", path.name)
    if m:
        return m.group(1), m.group(2)
    return None, None

# Collect all dates
all_dates = []
for f in files:
    start, end = extract_dates(f)
    if start and end:
        all_dates.extend([start, end])
        print(f"  {f.name}: {start} to {end}")

if not all_dates:
    raise SystemExit("Could not extract dates from filenames")

# Find earliest and latest dates
earliest = min(all_dates)
latest = max(all_dates)
print(f"\nDate range: {earliest} to {latest}")

# Read and concatenate all files
print("\nReading and concatenating files...")
frames = [pl.read_csv(f) for f in files]
df = pl.concat(frames, how="vertical", rechunk=True)

# Write output
output_path = Path("downloads") / f"openairframes_adsb_{earliest}_{latest}.csv.gz"
output_path.parent.mkdir(parents=True, exist_ok=True)
df.write_csv(output_path, compression="gzip")

print(f"\nWrote {output_path} with {df.height:,} rows")