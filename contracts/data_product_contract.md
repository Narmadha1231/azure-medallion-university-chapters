# Data Product Contract: University Chapters v1

## Name, owner, and use
- **Product:** University Chapters West Coast v1
- **Technical owner:** Data Engineering
- **Purpose:** Reusable chapter-level data for analytics, mapping, and reporting.

## Interface
- **Path:** `data/gold/university_chapters/v1/`
- **Format:** Parquet
- **Grain:** One current row per `chapter_id`

| Column | Type | Nullable | Meaning |
|---|---|---:|---|
| chapter_id | string | No | Stable business key |
| chapter_name | string | Yes | Display name |
| city | string | Yes | Source city |
| state | string | No | CA, OR, or WA |
| longitude | double | No | WGS84 longitude |
| latitude | double | No | WGS84 latitude |
| dq_status | string | No | OK or WARNING |
| dq_warnings | array<string> | No | Soft-warning reason codes |
| ingest_run_id | string | No | Ingestion run identifier |
| processed_at_utc | timestamp | No | Processing timestamp |

## Freshness
Intended production SLA: refreshed daily by 06:00 UTC. This sample is manually triggered.

## Quality
- **DQ-Q1 / quarantine:** missing, non-numeric, or out-of-range coordinates. Reason: `INVALID_COORDINATES`. These rows never publish to Gold.
- **DQ-W1 / warning:** null, blank, or `UNKNOWN` city. Reason: `MISSING_OR_UNKNOWN_CITY`. These rows publish with `dq_status=WARNING`.
- An empty entire batch fails publication.
- CA dropping to zero fails publication because CA is historically expected to be present.
- OR and WA may legitimately contain zero rows.

## Versioning
Breaking schema or semantic changes publish to a new versioned path such as `v2`. Additive compatible changes may remain in v1 with documentation.

## Classification
Public source data. No PII is expected.
