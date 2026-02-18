from pathlib import Path
import polars as pl
import argparse

OUTPUT_DIR = Path("./data/output")

def main():
    parser = argparse.ArgumentParser(description="Concatenate compressed parquet files for a single day")
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument("--concat_with_latest_csv", action="store_true", help="Whether to also concatenate with the latest CSV from GitHub releases")
    args = parser.parse_args()

    compressed_dir = OUTPUT_DIR / "compressed"
    date_dir = compressed_dir / args.date
    if not date_dir.is_dir():
        raise FileNotFoundError(f"No date folder found: {date_dir}")

    parquet_files = sorted(date_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {date_dir}")

    frames = [pl.read_parquet(p) for p in parquet_files]
    df = pl.concat(frames, how="vertical", rechunk=True)

    df = df.sort(["time", "icao"])
    output_path = OUTPUT_DIR / f"openairframes_adsb_{args.date}.parquet"
    print(f"Writing combined parquet to {output_path} with {df.height} rows")
    df.write_parquet(output_path)

    csv_output_path = OUTPUT_DIR / f"openairframes_adsb_{args.date}.csv.gz"
    print(f"Writing combined csv.gz to {csv_output_path} with {df.height} rows")
    df.write_csv(csv_output_path)  # polars detects .gz and compresses automatically

    if args.concat_with_latest_csv:
        print("Loading latest CSV from GitHub releases to concatenate with...")
        from src.get_latest_release import get_latest_aircraft_adsb_csv_df
        df_latest_csv, csv_date = get_latest_aircraft_adsb_csv_df()
        from src.adsb.compress_adsb_to_aircraft_data import concat_compressed_dfs
        df_final = concat_compressed_dfs(df, df_latest_csv)
        final_csv_output_path = OUTPUT_DIR / f"openairframes_adsb_{args.date}_{csv_date}.csv.gz"
        df_final.write_csv(final_csv_output_path)

if __name__ == "__main__":
    main()