# Rechtspraak Scraper

Dutch court decisions (uitspraken) scraper for rechtspraak.nl.

## Architecture

- **Data source**: rechtspraak.nl sitemap + content API
- **Queue**: ClickHouse `rechtspraak_pending` view (ECLIs not yet fetched)
- **Storage**:
  - Metadata → ClickHouse
  - Raw XML → MinIO (`raw-data/rechtspraak/`)

## Tables

| Table | Purpose |
|-------|---------|
| `rechtspraak_eclis` | All known ECLIs from sitemaps |
| `rechtspraak_uitspraken` | Scraped metadata + xml_path |
| `rechtspraak_pending` | View: ECLIs needing fetch |

## Storage Structure

MinIO path format:
```
raw-data/rechtspraak/{country}/{court}/{year}/{ECLI_escaped}.xml
```

Example:
```
raw-data/rechtspraak/NL/HR/2025/ECLI_NL_HR_2025_123.xml
```

## API Endpoints

- Sitemap: `https://uitspraken.rechtspraak.nl/sitemap/UrlSet?from=YYYY-MM-DD&to=YYYY-MM-DD`
- Content: `https://data.rechtspraak.nl/uitspraken/content?id=ECLI:...`

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_HOST` | localhost | ClickHouse host |
| `CLICKHOUSE_PORT` | 8123 | ClickHouse HTTP port |
| `CLICKHOUSE_USER` | default | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | | ClickHouse password |
| `MINIO_ENDPOINT` | localhost:9002 | MinIO endpoint |
| `MINIO_ACCESS_KEY` | minioadmin | MinIO access key |
| `MINIO_SECRET_KEY` | minioadmin | MinIO secret key |
| `MINIO_BUCKET` | raw-data | MinIO bucket for XML files |
| `STORE_XML` | true | Whether to store XML in MinIO |
| `START_YEAR` | 2000 | First year to index |
| `REQUEST_DELAY` | 1.0 | Seconds between content API requests |
| `BATCH_SIZE` | 100 | ECLIs per fetch batch |

## Usage

```bash
# Local development
docker compose up -d clickhouse
docker compose run --rm scraper

# Run phases separately
docker compose run --rm scraper python -m src.main --phase index
docker compose run --rm scraper python -m src.main --phase fetch

# Production (Nomad)
nomad job run /path/to/rechtspraak-scraper.nomad.hcl
```

## Progress Tracking

```sql
-- Total indexed ECLIs
SELECT count() FROM rechtspraak_eclis;

-- Total fetched
SELECT count() FROM rechtspraak_uitspraken;

-- Pending
SELECT count() FROM rechtspraak_pending;

-- By court type
SELECT court_type, count() FROM rechtspraak_uitspraken GROUP BY court_type;

-- With XML stored
SELECT count() FROM rechtspraak_uitspraken WHERE xml_path IS NOT NULL;
```

## Downloading XML Files

```bash
# Configure mc
mc alias set odc http://178.162.140.145:9002 minioadmin <password>

# List XML files
mc ls odc/raw-data/rechtspraak/ --recursive | head

# Download a single file
mc cp odc/raw-data/rechtspraak/NL/HR/2025/ECLI_NL_HR_2025_123.xml .

# Download all files for a court/year
mc cp --recursive odc/raw-data/rechtspraak/NL/HR/2025/ ./downloads/
```
