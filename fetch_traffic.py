#!/usr/bin/env python3
"""
Fetch DAP traffic data for all snapshot dates and cache to dap_traffic.json.

Usage:
    python fetch_traffic.py --api-key YOUR_KEY
    DAP_API_KEY=YOUR_KEY python fetch_traffic.py
    python fetch_traffic.py --api-key YOUR_KEY --output dap_traffic.json
"""
import argparse
import datetime
import json
import os
import subprocess
import sys
import time
from pathlib import Path

# Canonical fetch dates for all 13 snapshots (11 unique calendar dates)
SNAPSHOT_DATES = [
    "2025-10-12",
    "2025-11-06",
    "2025-11-13",
    "2026-01-29",
    "2026-01-30",
    "2026-01-31",
    "2026-02-01",
    "2026-02-02",
    "2026-02-03",
    "2026-02-04",
    "2026-02-08",
]

API_ENDPOINT = "https://api.gsa.gov/analytics/dap/v2.0.0/reports/site/data"


def fetch_day(date: str, api_key: str) -> dict:
    """Fetch all domain visit records for a single calendar day."""
    dt = datetime.date.fromisoformat(date)
    before = str(dt + datetime.timedelta(days=1))
    url = f"{API_ENDPOINT}?api_key={api_key}&after={date}&before={before}&limit=10000"
    result = subprocess.run(
        ["curl", "-s", "--header", f"x-api-key: {api_key}", url],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl failed for {date}: {result.stderr}")
    data = json.loads(result.stdout)
    return {r["domain"]: r["visits"] for r in data}


def full_range_dates() -> list:
    """Generate all calendar dates from 2025-10-12 to 2026-02-08 inclusive."""
    start = datetime.date(2025, 10, 12)
    end   = datetime.date(2026, 2, 8)
    dates = []
    d = start
    while d <= end:
        dates.append(str(d))
        d += datetime.timedelta(days=1)
    return dates


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--api-key", default=os.environ.get("DAP_API_KEY", ""))
    p.add_argument("--output", default="dap_traffic.json")
    p.add_argument("--full-range", action="store_true",
                   help="Fetch every calendar day Oct 12 2025 – Feb 8 2026 (109 new calls)")
    args = p.parse_args()

    if not args.api_key:
        print("ERROR: provide --api-key or set DAP_API_KEY env var", file=sys.stderr)
        sys.exit(1)

    out_path = Path(args.output)

    # Load existing cache
    cache = {"dates": {}}
    if out_path.exists():
        with out_path.open() as f:
            cache = json.load(f)
        print(f"Loaded existing cache: {len(cache['dates'])} dates already present")

    today = str(datetime.date.today())
    fetched = 0

    dates_to_fetch = full_range_dates() if args.full_range else SNAPSHOT_DATES
    total = len(dates_to_fetch)

    for i, date in enumerate(dates_to_fetch, 1):
        if date in cache["dates"]:
            print(f"  {date}: already cached, skipping")
            continue

        print(f"  {date}: fetching ({i} of {total})...", end=" ", flush=True)
        domains = fetch_day(date, args.api_key)
        cache["dates"][date] = {
            "fetched_at": today,
            "domains": domains,
        }
        print(f"{len(domains)} domains")
        fetched += 1

        # Save after each date in case of interruption
        with out_path.open("w") as f:
            json.dump(cache, f)

        if i < total:
            time.sleep(1)

    print(f"\nDone. {fetched} new dates fetched, {len(cache['dates'])} total in cache.")
    print(f"Output: {out_path} ({out_path.stat().st_size // 1024}KB)")

    # Coverage summary against a sample of well-known domains
    sample = ["irs.gov", "ssa.gov", "va.gov", "nasa.gov", "weather.gov",
              "commerce.gov", "usajobs.gov", "cdc.gov", "dhs.gov", "state.gov"]
    print("\nSpot-check (latest cached date):")
    latest_date = sorted(cache["dates"].keys())[-1]
    domains = cache["dates"][latest_date]["domains"]
    for d in sample:
        visits = domains.get(d)
        print(f"  {d}: {visits:,}" if visits else f"  {d}: not in DAP")


if __name__ == "__main__":
    main()
