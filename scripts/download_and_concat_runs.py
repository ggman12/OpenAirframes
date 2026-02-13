#!/usr/bin/env python3
"""
Download and concatenate artifacts from a specific set of workflow runs.

Usage:
    python scripts/download_and_concat_runs.py triggered_runs_20260216_123456.json
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def download_run_artifact(run_id, output_dir):
    """Download artifact from a specific workflow run."""
    print(f"  Downloading artifacts from run {run_id}...")
    
    cmd = [
        'gh', 'run', 'download', str(run_id),
        '--pattern', 'openairframes_adsb-*',
        '--dir', output_dir
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"  ✓ Downloaded")
        return True
    else:
        if "no artifacts" in result.stderr.lower():
            print(f"  ⚠ No artifacts found (workflow may still be running)")
        else:
            print(f"  ✗ Failed: {result.stderr}")
        return False


def find_csv_files(download_dir):
    """Find all CSV.gz files in the download directory."""
    csv_files = []
    for root, dirs, files in os.walk(download_dir):
        for file in files:
            if file.endswith('.csv.gz'):
                csv_files.append(os.path.join(root, file))
    return sorted(csv_files)


def concatenate_csv_files(csv_files, output_file):
    """Concatenate CSV files in order, preserving headers."""
    import gzip
    
    print(f"\nConcatenating {len(csv_files)} CSV files...")
    
    with gzip.open(output_file, 'wt') as outf:
        header_written = False
        
        for i, csv_file in enumerate(csv_files, 1):
            print(f"  [{i}/{len(csv_files)}] Processing {os.path.basename(csv_file)}")
            
            with gzip.open(csv_file, 'rt') as inf:
                lines = inf.readlines()
                
                if not header_written:
                    # Write header from first file
                    outf.writelines(lines)
                    header_written = True
                else:
                    # Skip header for subsequent files
                    outf.writelines(lines[1:])
    
    print(f"\n✓ Concatenated CSV saved to: {output_file}")
    
    # Show file size
    size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"  Size: {size_mb:.1f} MB")


def main():
    parser = argparse.ArgumentParser(
        description='Download and concatenate artifacts from workflow runs'
    )
    parser.add_argument(
        'runs_file',
        help='JSON file containing run IDs (from run_historical_adsb_action.py)'
    )
    parser.add_argument(
        '--output-dir',
        default='./downloads/historical_concat',
        help='Directory for downloads (default: ./downloads/historical_concat)'
    )
    parser.add_argument(
        '--wait',
        action='store_true',
        help='Wait for workflows to complete before downloading'
    )
    
    args = parser.parse_args()
    
    # Load run IDs
    if not os.path.exists(args.runs_file):
        print(f"Error: File not found: {args.runs_file}")
        sys.exit(1)
    
    with open(args.runs_file, 'r') as f:
        data = json.load(f)
    
    runs = data['runs']
    start_date = data['start_date']
    end_date = data['end_date']
    
    print("=" * 60)
    print("Download and Concatenate Historical Artifacts")
    print("=" * 60)
    print(f"Date range: {start_date} to {end_date}")
    print(f"Workflow runs: {len(runs)}")
    print(f"Output directory: {args.output_dir}")
    print("=" * 60)
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Wait for workflows to complete if requested
    if args.wait:
        print("\nWaiting for workflows to complete...")
        for run_info in runs:
            run_id = run_info['run_id']
            print(f"  Checking run {run_id}...")
            
            cmd = ['gh', 'run', 'watch', str(run_id)]
            subprocess.run(cmd)
    
    # Download artifacts
    print("\nDownloading artifacts...")
    successful_downloads = 0
    
    for i, run_info in enumerate(runs, 1):
        run_id = run_info['run_id']
        print(f"\n[{i}/{len(runs)}] Run {run_id} ({run_info['start']} to {run_info['end']})")
        
        if download_run_artifact(run_id, args.output_dir):
            successful_downloads += 1
    
    print(f"\n\nDownload Summary: {successful_downloads}/{len(runs)} artifacts downloaded")
    
    if successful_downloads == 0:
        print("\nNo artifacts downloaded. Workflows may still be running.")
        print("Use --wait to wait for completion, or try again later.")
        sys.exit(1)
    
    # Find all CSV files
    csv_files = find_csv_files(args.output_dir)
    
    if not csv_files:
        print("\nError: No CSV files found in download directory")
        sys.exit(1)
    
    print(f"\nFound {len(csv_files)} CSV file(s):")
    for csv_file in csv_files:
        print(f"  - {os.path.basename(csv_file)}")
    
    # Concatenate
    # Calculate actual end date for filename (end_date - 1 day since it's exclusive)
    from datetime import datetime, timedelta
    end_dt = datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=1)
    actual_end = end_dt.strftime('%Y-%m-%d')
    
    output_file = os.path.join(
        args.output_dir,
        f"openairframes_adsb_{start_date}_{actual_end}.csv.gz"
    )
    
    concatenate_csv_files(csv_files, output_file)
    
    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == '__main__':
    main()
