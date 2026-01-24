-- schema.sql
-- ClickHouse tables for Dutch court decisions (uitspraken)

-- Table 1: All known ECLIs (the "queue")
CREATE TABLE IF NOT EXISTS rechtspraak_eclis (
    ecli String,
    first_seen DateTime DEFAULT now(),
    last_modified DateTime,
    source_url String
) ENGINE = ReplacingMergeTree(first_seen)
ORDER BY ecli;

-- Table 2: Scraped metadata
CREATE TABLE IF NOT EXISTS rechtspraak_uitspraken (
    -- Identifiers
    ecli String,
    case_number Nullable(String),

    -- Dates
    decision_date Nullable(Date),
    publication_date Nullable(Date),

    -- Court/Institution
    court String,
    court_type LowCardinality(String),

    -- Classification
    procedure_type LowCardinality(Nullable(String)),
    subject_area LowCardinality(Nullable(String)),

    -- Content
    summary Nullable(String),

    -- Source URL for later full-text retrieval
    content_url String,

    -- Related cases
    related_eclis Array(String),

    -- Scraper metadata
    scraped_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(scraped_at)
ORDER BY ecli;

-- View: ECLIs that still need to be fetched
CREATE VIEW IF NOT EXISTS rechtspraak_pending AS
SELECT e.ecli, e.last_modified, e.source_url
FROM rechtspraak_eclis e
LEFT JOIN rechtspraak_uitspraken u ON e.ecli = u.ecli
WHERE u.ecli IS NULL;
