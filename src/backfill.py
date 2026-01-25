"""Backfill XML files for existing rechtspraak records.

Downloads XML for records where xml_path IS NULL and stores in MinIO.
Inserts new rows (ReplacingMergeTree will deduplicate by ecli).
"""

import httpx
import clickhouse_connect
import json
import os
import sys
import time
from datetime import datetime, timezone
from io import BytesIO

from minio import Minio


# Configuration
CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"
REQUEST_DELAY = float(os.environ.get("REQUEST_DELAY", "1.0"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
USER_AGENT = "OpenDataCollection.com bot - Data zonder drempels (https://opendatacollection.com)"

# MinIO configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9002")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "raw-data")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"


def log(level: str, message: str, **extra):
    """Structured JSON logging."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "message": message,
        "script": "backfill",
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

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        log("INFO", "Created MinIO bucket", bucket=MINIO_BUCKET)

    return client


def get_http_client() -> httpx.Client:
    """Get HTTP client with proper headers."""
    return httpx.Client(
        timeout=30.0,
        headers={"User-Agent": USER_AGENT},
    )


def ecli_to_path(ecli: str) -> str:
    """Convert ECLI to MinIO path."""
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


def download_and_store_xml(minio_client: Minio, ecli: str) -> str | None:
    """Download XML and store in MinIO. Returns path or None on failure."""
    url = f"{CONTENT_URL}?id={ecli}"

    try:
        with get_http_client() as client:
            resp = client.get(url)
            resp.raise_for_status()
            xml_content = resp.content

            path = ecli_to_path(ecli)
            minio_client.put_object(
                MINIO_BUCKET,
                path,
                BytesIO(xml_content),
                len(xml_content),
                content_type="application/xml",
            )
            return path

    except Exception as e:
        log("ERROR", "Failed to download/store XML", ecli=ecli, error=str(e))
        return None


def main():
    """Main backfill process."""
    log("INFO", "Starting XML backfill")

    ch = get_clickhouse()
    minio = get_minio()

    # Count total to backfill
    total = ch.query("SELECT count() FROM rechtspraak_uitspraken WHERE xml_path IS NULL")
    total_count = total.result_rows[0][0]
    log("INFO", f"Records to backfill", count=total_count)

    processed = 0
    success = 0
    failed = 0

    while True:
        # Get batch of records without xml_path
        # Use FINAL to get deduplicated view
        batch = ch.query(f"""
            SELECT
                ecli, case_number, decision_date, publication_date,
                court, court_type, procedure_type, subject_area,
                summary, content_url, related_eclis
            FROM rechtspraak_uitspraken FINAL
            WHERE xml_path IS NULL
            LIMIT {BATCH_SIZE}
        """)

        if not batch.result_rows:
            log("INFO", "No more records to backfill")
            break

        log("INFO", f"Processing batch", count=len(batch.result_rows), progress=f"{processed}/{total_count}")

        rows_to_insert = []
        for row in batch.result_rows:
            ecli = row[0]
            xml_path = download_and_store_xml(minio, ecli)
            time.sleep(REQUEST_DELAY)

            if xml_path:
                # Create new row with xml_path filled in
                rows_to_insert.append((
                    row[0],   # ecli
                    row[1],   # case_number
                    row[2],   # decision_date
                    row[3],   # publication_date
                    row[4],   # court
                    row[5],   # court_type
                    row[6],   # procedure_type
                    row[7],   # subject_area
                    row[8],   # summary
                    row[9],   # content_url
                    row[10],  # related_eclis
                    datetime.now(timezone.utc),  # scraped_at (new timestamp)
                    xml_path,  # xml_path (now filled)
                ))
                success += 1
            else:
                failed += 1

            processed += 1

        # Insert new rows (ReplacingMergeTree will dedupe by ecli, keeping newest scraped_at)
        if rows_to_insert:
            ch.insert(
                "rechtspraak_uitspraken",
                rows_to_insert,
                column_names=[
                    "ecli", "case_number", "decision_date", "publication_date",
                    "court", "court_type", "procedure_type", "subject_area",
                    "summary", "content_url", "related_eclis", "scraped_at", "xml_path",
                ],
            )
            log("INFO", f"Inserted batch", count=len(rows_to_insert))

    log("INFO", "Backfill complete", processed=processed, success=success, failed=failed)


if __name__ == "__main__":
    main()
