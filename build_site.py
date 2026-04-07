#!/usr/bin/env python3
"""
Build static site data from master_scrapes_classified.jsonl.

Generates:
  site/data.json   — pre-aggregated data for the site
  site/index.html  — the static site

Usage:
    python build_site.py
    python build_site.py --input master_scrapes_classified.jsonl --out site/
"""
import argparse
import json
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Date normalization — merge partial/overnight runs into single snapshots
# ---------------------------------------------------------------------------

# Maps source_file -> canonical snapshot date.
# This is more reliable than date-based merging because some source files
# span midnight and produce records on two calendar dates.
SOURCE_FILE_SNAPSHOT = {
    "homepages_full.jsonl":            "2025-10-12",
    "homepages_nov.jsonl":             "2025-11-06",
    "12_nov_homepages_nov.jsonl":      "2025-11-13-am",
    "13_nov_homepages.jsonl":          "2025-11-13-pm",
    "homepagesv2.jsonl":               "2026-01-29",
    "homepagesv2_29jan_1800.jsonl":    "2026-01-30",
    "homepagesv2_30Jan_1800.jsonl":    "2026-01-30-pm",
    "homepagesv2_31Jan_0530.jsonl":    "2026-01-31-am",
    "homepagesv3_31Jan_1300.jsonl":    "2026-02-01",
    "homepagesv3_1Feb_1200.jsonl":     "2026-02-02",
    "homepagesv3_2Feb_0800.jsonl":     "2026-02-03",
    "homepagesv3_3Feb_2000.jsonl":     "2026-02-04",
    "homepagesv3_8Feb_1200.jsonl":     "2026-02-08",
}

SNAPSHOT_LABELS = {
    "2025-10-12":    "Oct 12, 2025",
    "2025-11-06":    "Nov 6, 2025",
    "2025-11-13-am": "Nov 13, 2025 AM",
    "2025-11-13-pm": "Nov 13, 2025 PM",
    "2026-01-29":    "Jan 29, 2026",
    "2026-01-30":    "Jan 30, 2026",
    "2026-01-30-pm": "Jan 30, 2026 PM",
    "2026-01-31-am": "Jan 31, 2026 AM",
    "2026-02-01":    "Feb 1, 2026",
    "2026-02-02":    "Feb 2, 2026",
    "2026-02-03":    "Feb 3, 2026",
    "2026-02-04":    "Feb 4, 2026",
    "2026-02-08":    "Feb 8, 2026",
}


def canonical_date(raw_date: str, source_file: str) -> str:
    return SOURCE_FILE_SNAPSHOT.get(source_file, raw_date[:10])


# ---------------------------------------------------------------------------
# Site categorization
# ---------------------------------------------------------------------------

# Congressional caucus sites — inherently partisan by design, tracked separately
CONGRESSIONAL_SITES = {
    "https://democraticleader.gov",
    "https://democraticwhip.gov",
    "https://dems.gov",
    "https://gop.gov",
    "https://gopleader.gov",
    "https://majorityleader.gov",
    "https://majoritywhip.gov",
    "https://minorityleader.gov",
    "https://minoritywhip.gov",
    "https://republicanleader.gov",
    "https://speaker.gov",
    "https://house.gov",
    "https://jct.gov",
    "https://ppdcecc.gov",
}

# Historical archive sites — frozen content from prior administrations
ARCHIVE_SITES = {
    "https://obamawhitehouse.gov",
}


def site_category(url: str) -> str:
    """Return 'congressional', 'archive', or 'agency'."""
    if url in CONGRESSIONAL_SITES:
        return "congressional"
    if url in ARCHIVE_SITES:
        return "archive"
    return "agency"


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build(input_path: Path, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    # Separate time series per category
    by_date         = defaultdict(lambda: {"partisan": 0, "neutral": 0, "unknown": 0})
    by_date_agency  = defaultdict(lambda: {"partisan": 0, "neutral": 0, "unknown": 0})

    site_history = defaultdict(dict)   # url -> {date -> label}
    partisan_cards = []

    with input_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            url      = d["url"]
            date     = canonical_date(d.get("fetched_at", ""), d.get("source_file", ""))
            snt      = d.get("sentiment_llm") or {}
            label    = snt.get("label", "unknown")
            if label not in ("partisan", "neutral", "unknown"):
                label = "unknown"
            category = site_category(url)

            by_date[date][label] += 1
            if category == "agency":
                by_date_agency[date][label] += 1

            site_history[url][date] = label

            if label == "partisan":
                card = {
                    "url":      url,
                    "date":     date,
                    "title":    d.get("title") or url,
                    "rationale": snt.get("rationale", ""),
                    "model":    snt.get("model", ""),
                    "category": category,
                }
                quote = snt.get("partisan_quote", "").strip()
                if quote:
                    card["partisan_quote"] = quote
                partisan_cards.append(card)

    # Time series — only known snapshots, sorted
    snapshots = sorted(k for k in by_date if k in SNAPSHOT_LABELS)
    time_series = {
        "labels":   [SNAPSHOT_LABELS[s] for s in snapshots],
        "partisan": [by_date[s]["partisan"] for s in snapshots],
        "neutral":  [by_date[s]["neutral"]  for s in snapshots],
        "unknown":  [by_date[s]["unknown"]  for s in snapshots],
        # Agency-only series for the headline chart
        "agency_partisan": [by_date_agency[s]["partisan"] for s in snapshots],
        "agency_neutral":  [by_date_agency[s]["neutral"]  for s in snapshots],
        "agency_unknown":  [by_date_agency[s]["unknown"]  for s in snapshots],
    }

    # Summary stats — agency sites only for headline numbers
    latest = snapshots[-1] if snapshots else None
    summary = {
        "total_sites":      len(site_history),
        "total_records":    sum(
            by_date[s]["partisan"] + by_date[s]["neutral"] + by_date[s]["unknown"]
            for s in snapshots
        ),
        "snapshots":        len(snapshots),
        "latest_date":      SNAPSHOT_LABELS.get(latest, ""),
        "latest_partisan":  by_date_agency[latest]["partisan"] if latest else 0,
        "latest_neutral":   by_date_agency[latest]["neutral"]  if latest else 0,
        "latest_unknown":   by_date_agency[latest]["unknown"]  if latest else 0,
        "congressional_count": len(CONGRESSIONAL_SITES),
        "archive_count":       len(ARCHIVE_SITES),
    }

    # Flip tracker — agency sites only, exclude archive
    flips = []
    for url, history in site_history.items():
        if site_category(url) != "agency":
            continue
        dated = sorted(history.items())
        if len(dated) < 2:
            continue
        first_label = dated[0][1]
        last_label  = dated[-1][1]
        if first_label != last_label and "unknown" not in (first_label, last_label):
            flips.append({
                "url":        url,
                "from_label": first_label,
                "from_date":  SNAPSHOT_LABELS.get(dated[0][0], dated[0][0]),
                "to_label":   last_label,
                "to_date":    SNAPSHOT_LABELS.get(dated[-1][0], dated[-1][0]),
            })

    # Sort partisan cards newest first
    partisan_cards.sort(key=lambda x: x["date"], reverse=True)

    data = {
        "summary":        summary,
        "time_series":    time_series,
        "partisan_cards": partisan_cards,
        "flips":          flips,
    }

    data_path = out_dir / "data.json"
    with data_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    agency_partisan  = [c for c in partisan_cards if c["category"] == "agency"]
    congress_partisan = [c for c in partisan_cards if c["category"] == "congressional"]
    archive_partisan  = [c for c in partisan_cards if c["category"] == "archive"]

    print(f"Wrote {data_path}  ({data_path.stat().st_size // 1024}KB)")
    print(f"  {len(snapshots)} snapshots")
    print(f"  {len(partisan_cards)} partisan records total")
    print(f"    {len(agency_partisan)} agency")
    print(f"    {len(congress_partisan)} congressional")
    print(f"    {len(archive_partisan)} archive")
    print(f"  {len(flips)} label flips (agency only)")


# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="master_scrapes_classified.jsonl")
    p.add_argument("--out",   default="site")
    args = p.parse_args()
    build(Path(args.input), Path(args.out))


if __name__ == "__main__":
    main()
