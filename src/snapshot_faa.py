from faa_aircraft_registry import read
import pandas as pd
import zipfile
import zipfile
from pathlib import Path
from datetime import datetime, timezone
date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

out_dir = Path("data/faa_releasable")
out_dir.mkdir(parents=True, exist_ok=True)
zip_name = f"ReleasableAircraft_{date_str}.zip"
csv_name = f"ReleasableAircraft_{date_str}.csv"

zip_path = out_dir / zip_name
csv_path = out_dir / csv_name

# URL and paths
url = "https://registry.faa.gov/database/ReleasableAircraft.zip"
from urllib.request import Request, urlopen

req = Request(
    url,
    headers={"User-Agent": "Mozilla/5.0"},
    method="GET",
)

with urlopen(req, timeout=120) as r:
    body = r.read()
    zip_path.write_bytes(body)

with zipfile.ZipFile(zip_path) as z:
    registrations = read(z)

df = pd.DataFrame(registrations['master'].values())
col = "transponder_code_hex"
df = df[[col] + [c for c in df.columns if c != col]]
df = df.rename(columns={"transponder_code_hex": "icao"})
registrant = pd.json_normalize(df["registrant"]).add_prefix("registrant_")
df = df.drop(columns="registrant").join(registrant)
df = df.rename(columns={"aircraft_type": "aircraft_type_2"})
aircraft = pd.json_normalize(df["aircraft"]).add_prefix("aircraft_")
df = df.drop(columns="aircraft").join(aircraft)
df = df.rename(columns={"engine_type": "engine_type_2"})
engine = pd.json_normalize(df["engine"].where(df["engine"].notna(), {})).add_prefix("engine_")
df = df.drop(columns="engine").join(engine)
df = df.sort_values(by=["icao"])
df.to_csv(csv_path, index=False)

