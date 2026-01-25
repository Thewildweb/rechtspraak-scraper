"""Rechtspraak.nl scraper - fetches Dutch court decisions.

Two phases:
1. Index: Scrape sitemaps to discover all ECLIs
2. Fetch: Download metadata for ECLIs not yet in uitspraken table

Stores:
- Metadata in ClickHouse
- Raw XML files in MinIO
"""

import httpx
import clickhouse_connect
import json
import os
import sys
import time
from datetime import datetime, timezone, date
from dateutil.relativedelta import relativedelta
from io import BytesIO

from minio import Minio

from .parser import parse_sitemap, parse_uitspraak


# Configuration
SITEMAP_URL = "https://uitspraken.rechtspraak.nl/sitemap/UrlSet"
CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"
REQUEST_DELAY = float(os.environ.get("REQUEST_DELAY", "1.0"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
START_YEAR = int(os.environ.get("START_YEAR", "2000"))
USER_AGENT = "OpenDataCollection.com bot - Data zonder drempels (https://opendatacollection.com)"

# MinIO configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9002")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "raw-data")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"
STORE_XML = os.environ.get("STORE_XML", "true").lower() == "true"


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


def get_minio():
    """Get MinIO client."""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

    # Ensure bucket exists
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        log("INFO", "Created MinIO bucket", bucket=MINIO_BUCKET)

    return client


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


def ecli_to_path(ecli: str) -> str:
    """Convert ECLI to MinIO path.

    ECLI:NL:HR:2025:123 -> rechtspraak/NL/HR/2025/ECLI_NL_HR_2025_123.xml
    """
    parts = ecli.split(":")
    if len(parts) >= 5:
        country = parts[1]
        court = parts[2]
        year = parts[3]
        safe_ecli = ecli.replace(":", "_")
        return f"rechtspraak/{country}/{court}/{year}/{safe_ecli}.xml"
    else:
        safe_ecli = ecli.replace(":", "_")
        return f"rechtspraak/other/{safe_ecli}.xml"


def store_xml(minio_client: Minio, ecli: str, xml_content: bytes) -> str:
    """Store XML content in MinIO. Returns the object path."""
    path = ecli_to_path(ecli)

    minio_client.put_object(
        MINIO_BUCKET,
        path,
        BytesIO(xml_content),
        len(xml_content),
        content_type="application/xml",
    )

    return path


def fetch_uitspraak(ecli: str, minio_client: Minio = None) -> tuple[dict | None, str | None]:
    """Fetch and parse a single uitspraak by ECLI.

    Returns (parsed_data, xml_path) where xml_path is the MinIO path if stored.
    """
    url = f"{CONTENT_URL}?id={ecli}"
    xml_path = None

    try:
        with get_http_client() as client:
            resp = client.get(url)
            resp.raise_for_status()
            xml_content = resp.content

            # Store raw XML in MinIO
            if minio_client and STORE_XML:
                try:
                    xml_path = store_xml(minio_client, ecli, xml_content)
                except Exception as e:
                    log("ERROR", "Failed to store XML", ecli=ecli, error=str(e))

            return parse_uitspraak(xml_content), xml_path

    except httpx.HTTPError as e:
        log("ERROR", f"Failed to fetch uitspraak", ecli=ecli, error=str(e))
        return None, None


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
    log("INFO", "Phase 2: Fetching pending uitspraken", store_xml=STORE_XML)

    ch_client = get_clickhouse()
    minio_client = get_minio() if STORE_XML else None

    while True:
        # Get batch of pending ECLIs
        pending = ch_client.query(
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
            data, xml_path = fetch_uitspraak(ecli, minio_client)
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
                    xml_path,  # New: store the MinIO path
                    data.get("related_eclis") or [],
                    datetime.now(timezone.utc),
                ))

        if rows:
            ch_client.insert(
                "rechtspraak_uitspraken",
                rows,
                column_names=[
                    "ecli", "case_number", "decision_date", "publication_date",
                    "court", "court_type", "procedure_type", "subject_area",
                    "summary", "content_url", "xml_path", "related_eclis", "scraped_at",
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

    log("INFO", "Starting rechtspraak scraper", phase=args.phase, store_xml=STORE_XML)

    if args.phase in ("index", "all"):
        phase1_index()

    if args.phase in ("fetch", "all"):
        phase2_fetch()

    log("INFO", "Scraper finished")


if __name__ == "__main__":
    main()
