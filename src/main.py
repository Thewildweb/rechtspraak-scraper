"""Rechtspraak.nl scraper - fetches Dutch court decisions.

Two phases:
1. Index: Scrape sitemaps to discover all ECLIs
2. Fetch: Download metadata for ECLIs not yet in uitspraken table
"""

import httpx
import clickhouse_connect
import json
import os
import sys
import time
from datetime import datetime, timezone, date
from dateutil.relativedelta import relativedelta

from .parser import parse_sitemap, parse_uitspraak


# Configuration
SITEMAP_URL = "https://uitspraken.rechtspraak.nl/sitemap/UrlSet"
CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"
REQUEST_DELAY = float(os.environ.get("REQUEST_DELAY", "1.0"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
START_YEAR = int(os.environ.get("START_YEAR", "2000"))
USER_AGENT = "OpenDataCollection.com bot - Data zonder drempels (https://opendatacollection.com)"


def log(level: str, message: str, **extra):
    """Structured JSON logging."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "message": message,
        "project": os.environ.get("PROJECT_NAME", "rechtspraak-scraper"),
        **extra,
    }
    print(json.dumps(entry), file=sys.stderr if level == "ERROR" else sys.stdout)


def get_clickhouse():
    """Get ClickHouse client."""
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )


def generate_monthly_ranges(start_year: int) -> list[tuple[str, str]]:
    """Generate monthly date ranges from start_year to today."""
    ranges = []
    current = date(start_year, 1, 1)
    end = date.today() + relativedelta(months=1)

    while current < end:
        next_month = current + relativedelta(months=1)
        ranges.append((current.isoformat(), next_month.isoformat()))
        current = next_month

    return ranges


def get_http_client() -> httpx.Client:
    """Get HTTP client with proper headers."""
    return httpx.Client(
        timeout=30.0,
        headers={"User-Agent": USER_AGENT},
    )


def fetch_sitemap(from_date: str, to_date: str) -> list[dict]:
    """Fetch sitemap for a date range and return ECLI entries."""
    url = f"{SITEMAP_URL}?from={from_date}&to={to_date}"

    try:
        with get_http_client() as client:
            resp = client.get(url)
            resp.raise_for_status()
            return parse_sitemap(resp.content)
    except httpx.HTTPError as e:
        log("ERROR", f"Failed to fetch sitemap", url=url, error=str(e))
        return []


def fetch_uitspraak(ecli: str) -> dict | None:
    """Fetch and parse a single uitspraak by ECLI."""
    url = f"{CONTENT_URL}?id={ecli}"

    try:
        with get_http_client() as client:
            resp = client.get(url)
            resp.raise_for_status()
            return parse_uitspraak(resp.content)
    except httpx.HTTPError as e:
        log("ERROR", f"Failed to fetch uitspraak", ecli=ecli, error=str(e))
        return None


def phase1_index():
    """Phase 1: Scrape all sitemaps to discover ECLIs."""
    log("INFO", f"Phase 1: Indexing sitemaps from {START_YEAR}")

    client = get_clickhouse()
    ranges = generate_monthly_ranges(START_YEAR)
    total_eclis = 0

    for i, (from_date, to_date) in enumerate(ranges):
        entries = fetch_sitemap(from_date, to_date)

        if entries:
            # Prepare rows for insertion
            rows = [
                (
                    e["ecli"],
                    datetime.fromisoformat(e["lastmod"].replace("Z", "+00:00")) if e.get("lastmod") else datetime.now(timezone.utc),
                    e["url"],
                )
                for e in entries
            ]

            client.insert(
                "rechtspraak_eclis",
                rows,
                column_names=["ecli", "last_modified", "source_url"],
            )

            total_eclis += len(entries)
            log("INFO", f"Indexed {from_date}", count=len(entries), progress=f"{i+1}/{len(ranges)}")

        time.sleep(0.5)  # Be gentle with sitemap requests

    log("INFO", f"Phase 1 complete", total_eclis=total_eclis)
    return total_eclis


def phase2_fetch():
    """Phase 2: Fetch metadata for pending ECLIs."""
    log("INFO", "Phase 2: Fetching pending uitspraken")

    client = get_clickhouse()

    while True:
        # Get batch of pending ECLIs
        pending = client.query(
            "SELECT ecli FROM rechtspraak_pending LIMIT {batch:UInt32}",
            parameters={"batch": BATCH_SIZE},
        )

        if not pending.result_rows:
            log("INFO", "No more pending ECLIs")
            break

        eclis = [row[0] for row in pending.result_rows]
        log("INFO", f"Processing batch", count=len(eclis))

        rows = []
        for ecli in eclis:
            data = fetch_uitspraak(ecli)
            time.sleep(REQUEST_DELAY)

            if data:
                rows.append((
                    data.get("ecli") or ecli,
                    data.get("case_number"),
                    data.get("decision_date"),
                    data.get("publication_date"),
                    data.get("court") or "Unknown",
                    data.get("court_type") or "OTHER",
                    data.get("procedure_type"),
                    data.get("subject_area"),
                    data.get("summary"),
                    f"{CONTENT_URL}?id={ecli}",
                    data.get("related_eclis") or [],
                    datetime.now(timezone.utc),
                ))

        if rows:
            client.insert(
                "rechtspraak_uitspraken",
                rows,
                column_names=[
                    "ecli", "case_number", "decision_date", "publication_date",
                    "court", "court_type", "procedure_type", "subject_area",
                    "summary", "content_url", "related_eclis", "scraped_at",
                ],
            )
            log("INFO", f"Inserted batch", count=len(rows))


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Rechtspraak.nl scraper")
    parser.add_argument("--phase", choices=["index", "fetch", "all"], default="all",
                        help="Which phase to run (default: all)")
    args = parser.parse_args()

    log("INFO", "Starting rechtspraak scraper", phase=args.phase)

    if args.phase in ("index", "all"):
        phase1_index()

    if args.phase in ("fetch", "all"):
        phase2_fetch()

    log("INFO", "Scraper finished")


if __name__ == "__main__":
    main()
