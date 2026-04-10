#!/usr/bin/env python3
"""
Build static site data from master_scrapes_classified.jsonl.

Generates:
  site/data.json   — pre-aggregated data for the site
  site/index.html  — the static site

Usage:
    python build_site.py
    python build_site.py --input master_scrapes_classified.jsonl --out site/
    python build_site.py --input master_scrapes_gemma4_31b.jsonl --traffic dap_traffic.json
"""
import argparse
import datetime
import json
import re
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

# Notable events — drives chart annotations and callout panels in index.html
NOTABLE_EVENTS = [
    {
        "label": "Shutdown ended Nov 12, 2025",
        "chart_position": "Nov 13, 2025 AM",
        "callout": "46 agency homepages removed partisan content within hours of each other — the day after the government shutdown ended. Nearly all were Department of Justice and HHS sites.",
        "filter_date": "Nov 13, 2025 AM",
    }
]


def canonical_date(raw_date: str, source_file: str) -> str:
    return SOURCE_FILE_SNAPSHOT.get(source_file, raw_date[:10])


def dap_fetch_date(snapshot: str) -> str:
    """Strip -am/-pm/-pm suffix to get the calendar date for DAP lookup."""
    return re.sub(r"-(am|pm)$", "", snapshot)


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


def apex_domain(url: str) -> str:
    """Extract apex domain from url, e.g. 'https://irs.gov' -> 'irs.gov'."""
    m = re.match(r"https?://([^/]+)", url)
    return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build_cumulative(snapshots: list, site_history: dict, traffic: dict) -> dict:
    """
    Compute daily cumulative partisan visit estimates across the observation period.

    Logic:
    - For each consecutive snapshot pair with distinct calendar dates, find agency
      sites that are partisan at BOTH snap_a and snap_b (conservative estimate).
    - Count each calendar day from snap_a_date through snap_b_date - 1 inclusive.
    - Special case: Feb 8 (final snapshot) uses only snap_a partisan label.
    - Same-day pairs (Nov 13 AM→PM, Jan 30→Jan 30 PM, Jan 31 AM) are skipped.
    - Days with no DAP data are skipped silently (not included in output).

    Returns dict with:
      "series": list of {date, daily, cumulative}
      "total": final cumulative value
      "days_covered": count of entries
      "sites_counted": count of unique contributing URLs
    """
    contributing_urls = set()
    daily_totals = {}  # calendar date string -> int visits

    # Build consecutive pairs, skipping same-calendar-date pairs
    pairs = []
    for i in range(len(snapshots) - 1):
        snap_a = snapshots[i]
        snap_b = snapshots[i + 1]
        date_a = dap_fetch_date(snap_a)
        date_b = dap_fetch_date(snap_b)
        if date_a == date_b:
            continue  # same-day pair — skip
        pairs.append((snap_a, snap_b, date_a, date_b))

    for snap_a, snap_b, date_a, date_b in pairs:
        # Sites partisan at both endpoints (conservative — uses snap_b which is the
        # later/neutral side for same-day boundaries like Nov 13 PM)
        partisan_both = {
            url for url, history in site_history.items()
            if (site_category(url) == "agency"
                and history.get(snap_a) == "partisan"
                and history.get(snap_b) == "partisan")
        }

        # Iterate calendar days: snap_a_date through snap_b_date - 1 inclusive
        d = datetime.date.fromisoformat(date_a)
        end = datetime.date.fromisoformat(date_b)
        while d < end:
            day_str = str(d)
            day_data = traffic.get("dates", {}).get(day_str, {})
            if day_data:
                day_visits = 0
                for url in partisan_both:
                    domain = apex_domain(url)
                    v = day_data.get("domains", {}).get(domain, 0)
                    if v:
                        day_visits += v
                        contributing_urls.add(url)
                if day_visits > 0:
                    daily_totals[day_str] = daily_totals.get(day_str, 0) + day_visits
            d += datetime.timedelta(days=1)

    # Special case: Feb 8 (final snapshot, no snap_b)
    final_snap = snapshots[-1]
    final_date = dap_fetch_date(final_snap)
    partisan_final = {
        url for url, history in site_history.items()
        if (site_category(url) == "agency"
            and history.get(final_snap) == "partisan")
    }
    day_data = traffic.get("dates", {}).get(final_date, {})
    if day_data:
        day_visits = 0
        for url in partisan_final:
            domain = apex_domain(url)
            v = day_data.get("domains", {}).get(domain, 0)
            if v:
                day_visits += v
                contributing_urls.add(url)
        if day_visits > 0:
            daily_totals[final_date] = daily_totals.get(final_date, 0) + day_visits

    # Sort and compute running cumulative
    sorted_days = sorted(daily_totals.keys())
    series = []
    cumulative = 0
    for day in sorted_days:
        cumulative += daily_totals[day]
        series.append({"date": day, "daily": daily_totals[day], "cumulative": cumulative})

    return {
        "series":       series,
        "total":        cumulative,
        "days_covered": len(series),
        "sites_counted": len(contributing_urls),
    }


def build_daily_gov_totals(traffic: dict) -> list:
    """
    Compute daily total named-domain .gov traffic and 7-day trailing rolling average
    for every calendar day Oct 12 – Feb 8.

    Excludes the DAP '(other)' aggregate bucket, which produced an anomalous spike
    of ~72M visits/day during Jan 4–13 with no corresponding increase in any named
    domain — confirmed as a DAP reporting artifact.

    Returns a list of {date, total, rolling_avg_7d} dicts sorted by date.
    """
    start = datetime.date(2025, 10, 12)
    end   = datetime.date(2026, 2, 8)
    dates = []
    d = start
    while d <= end:
        dates.append(str(d))
        d += datetime.timedelta(days=1)

    totals = []
    for date_str in dates:
        day_data = traffic.get("dates", {}).get(date_str, {})
        if not day_data:
            continue  # missing from cache — skip silently
        domains = day_data.get("domains", {})
        total = sum(v for k, v in domains.items() if k != "(other)")
        totals.append((date_str, total))

    # 7-day trailing rolling average
    result = []
    for i, (date_str, total) in enumerate(totals):
        window = [t for _, t in totals[max(0, i - 6): i + 1]]
        avg = round(sum(window) / len(window))
        result.append({"date": date_str, "total": total, "rolling_avg_7d": avg})

    print(f"  daily_gov_totals: {len(result)} dates")
    if result:
        raw_vals = [r["total"] for r in result]
        avg_vals = [r["rolling_avg_7d"] for r in result]
        print(f"    raw total range:     {min(raw_vals):,} – {max(raw_vals):,}")
        print(f"    rolling avg range:   {min(avg_vals):,} – {max(avg_vals):,}")

    return result


def build(input_path: Path, out_dir: Path, traffic_path: Path = None):
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load traffic cache if provided
    traffic = None
    if traffic_path and traffic_path.exists():
        with traffic_path.open(encoding="utf-8") as f:
            traffic = json.load(f)
        print(f"Traffic data loaded: {len(traffic['dates'])} dates cached")

    # Separate time series per category
    by_date         = defaultdict(lambda: {"partisan": 0, "neutral": 0, "unknown": 0})
    by_date_agency  = defaultdict(lambda: {"partisan": 0, "neutral": 0, "unknown": 0})

    # Traffic counters — agency-only, partisan and neutral only (unknown excluded)
    by_date_agency_traffic = defaultdict(lambda: {"partisan": 0, "neutral": 0})
    traffic_coverage = defaultdict(int)  # snapshot -> count of domains with visits > 0

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

            # Traffic lookup — agency-only for chart/KPI series
            visits = 0
            if traffic and label in ("partisan", "neutral") and category == "agency":
                fetch_date = dap_fetch_date(date)
                day_data = traffic.get("dates", {}).get(fetch_date, {})
                domain = apex_domain(url)
                visits = day_data.get("domains", {}).get(domain, 0)
                if visits > 0:
                    by_date_agency_traffic[date][label] += visits
                    traffic_coverage[date] += 1

            # Card visits — all categories, omit field if no data
            card_visits = 0
            if traffic and label == "partisan":
                fetch_date = dap_fetch_date(date)
                day_data = traffic.get("dates", {}).get(fetch_date, {})
                domain = apex_domain(url)
                card_visits = day_data.get("domains", {}).get(domain, 0)

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
                if card_visits > 0:
                    card["visits"] = card_visits
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

    # Traffic series — only populated if traffic data provided
    if traffic:
        agency_partisan_visits = [by_date_agency_traffic[s]["partisan"] for s in snapshots]
        agency_neutral_visits  = [by_date_agency_traffic[s]["neutral"]  for s in snapshots]
        agency_total_visits    = [p + n for p, n in zip(agency_partisan_visits, agency_neutral_visits)]
        agency_partisan_reach_pct = [
            round(p / t * 100, 1) if t > 0 else 0.0
            for p, t in zip(agency_partisan_visits, agency_total_visits)
        ]
        time_series["agency_partisan_visits"]    = agency_partisan_visits
        time_series["agency_neutral_visits"]     = agency_neutral_visits
        time_series["agency_total_visits"]       = agency_total_visits
        time_series["agency_partisan_reach_pct"] = agency_partisan_reach_pct

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

    # Traffic summary fields
    if traffic and latest:
        reach_pct_list = time_series.get("agency_partisan_reach_pct", [])
        # Peak = snapshot with highest reach % (not raw visits — normalizes day-of-week)
        peak_idx = reach_pct_list.index(max(reach_pct_list)) if reach_pct_list else 0
        peak_snap = snapshots[peak_idx]
        summary["latest_partisan_visits"]  = by_date_agency_traffic[latest]["partisan"]
        summary["latest_total_visits"]     = (by_date_agency_traffic[latest]["partisan"] +
                                               by_date_agency_traffic[latest]["neutral"])
        summary["latest_partisan_reach_pct"] = reach_pct_list[-1] if reach_pct_list else 0.0
        summary["peak_partisan_visits"]    = by_date_agency_traffic[peak_snap]["partisan"]
        summary["peak_partisan_reach_pct"] = reach_pct_list[peak_idx]
        summary["peak_partisan_date"]      = SNAPSHOT_LABELS.get(peak_snap, peak_snap)
        summary["latest_traffic_coverage"] = traffic_coverage[latest]

    # Cumulative partisan visit estimate — only if traffic data provided
    if traffic:
        cum = build_cumulative(snapshots, site_history, traffic)
        if cum["series"]:
            time_series["cumulative_partisan_visits"] = cum["series"]
            summary["total_partisan_visits_estimated"] = cum["total"]
            summary["partisan_visits_days_covered"]    = cum["days_covered"]
            summary["partisan_visits_sites_counted"]   = cum["sites_counted"]

        gov_totals = build_daily_gov_totals(traffic)
        if gov_totals:
            time_series["daily_gov_totals"] = gov_totals

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

    # Sort partisan cards by visits descending (no-data to bottom), then by date descending
    partisan_cards.sort(key=lambda x: (-x.get("visits", 0), x["date"]), reverse=False)
    partisan_cards.sort(key=lambda x: (x.get("visits", -1) == -1, -x.get("visits", 0)))

    data = {
        "summary":        summary,
        "time_series":    time_series,
        "partisan_cards": partisan_cards,
        "flips":          flips,
        "notable_events": NOTABLE_EVENTS,
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
    if traffic:
        print(f"  Traffic data: {summary.get('latest_traffic_coverage', 0)} sites with data in latest snapshot")
        print(f"  Peak reach: {summary.get('peak_partisan_reach_pct', 0)}% on {summary.get('peak_partisan_date', '')}")
        print(f"  Latest reach: {summary.get('latest_partisan_reach_pct', 0)}% on {summary.get('latest_date', '')}")
        if "total_partisan_visits_estimated" in summary:
            total = summary["total_partisan_visits_estimated"]
            days  = summary["partisan_visits_days_covered"]
            sites = summary["partisan_visits_sites_counted"]
            print(f"  Est. cumulative partisan visits: {total:,}")
            print(f"  Days with data: {days} / 120")
            print(f"  Sites contributing: {sites}")


# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input",   default="master_scrapes_classified.jsonl")
    p.add_argument("--out",     default="site")
    p.add_argument("--traffic", default=None, help="Path to dap_traffic.json cache")
    args = p.parse_args()
    traffic_path = Path(args.traffic) if args.traffic else None
    build(Path(args.input), Path(args.out), traffic_path)


if __name__ == "__main__":
    main()
