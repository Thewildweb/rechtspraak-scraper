# Rechtspraak Scraper

Dutch court decisions (uitspraken) scraper for rechtspraak.nl.

## Architecture

- **Data source**: rechtspraak.nl sitemap + content API
- **Queue**: ClickHouse `rechtspraak_pending` view (ECLIs not yet fetched)
- **Storage**: ClickHouse tables

## Tables

| Table | Purpose |
|-------|---------|
| `rechtspraak_eclis` | All known ECLIs from sitemaps |
| `rechtspraak_uitspraken` | Scraped metadata |
| `rechtspraak_pending` | View: ECLIs needing fetch |

## API Endpoints

- Sitemap: `https://uitspraken.rechtspraak.nl/sitemap/UrlSet?from=YYYY-MM-DD&to=YYYY-MM-DD`
- Content: `https://data.rechtspraak.nl/uitspraken/content?id=ECLI:...`

## Usage

```bash
# Start ClickHouse
docker compose up -d clickhouse

# Run both phases (index + fetch)
docker compose run --rm scraper

# Or run phases separately
docker compose run --rm scraper python -m src.main --phase index
docker compose run --rm scraper python -m src.main --phase fetch
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `START_YEAR` | 2000 | First year to index |
| `REQUEST_DELAY` | 1.0 | Seconds between content API requests |
| `BATCH_SIZE` | 100 | ECLIs per fetch batch |

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
```
