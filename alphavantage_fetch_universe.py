import requests
from datetime import date
from pathlib import Path

API_KEY = ""
BASE_URL = "https://www.alphavantage.co/query"

OUT_DIR = Path("listing_status_yearly")
OUT_DIR.mkdir(exist_ok=True)

def fetch_and_save(snapshot_date=None):
    params = {
        "function": "LISTING_STATUS",
        "apikey": API_KEY
    }
    if snapshot_date:
        params["date"] = snapshot_date.isoformat()

    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()

    name = snapshot_date.isoformat() if snapshot_date else "current"
    path = OUT_DIR / f"listing_status_{name}.csv"

    with open(path, "w", encoding="utf-8") as f:
        f.write(r.text)

    print(f"Saved {path}")

# 1) current universe
fetch_and_save()

# 2) yearly snapshots (Jan 1st)
START_YEAR = 2010
END_YEAR = date.today().year

for year in range(START_YEAR, END_YEAR):
    fetch_and_save(date(year, 1, 1))
