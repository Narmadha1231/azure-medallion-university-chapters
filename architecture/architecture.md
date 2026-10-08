# Architecture

```mermaid
flowchart LR
  A[ArcGIS FeatureServer API or offline fixture] --> B[Bronze: raw JSON by ingest date and run ID]
  B --> C[PySpark flatten, type, scope, deduplicate]
  C -->|DQ-Q1 invalid coordinates| Q[Quarantine JSON by run ID]
  C -->|valid coordinates| S[Silver Parquet by run ID]
  S -->|OK plus WARNING rows| G[Gold v1 Parquet current snapshot]
  S --> M[Run metrics JSON]
  Q --> M
```

## Decisions
- Bronze is append-by-run for traceability.
- Silver is typed and deduplicated by `chapter_id`.
- Gold is overwritten as a current v1 snapshot, making reruns idempotent.
- Quarantine preserves the raw feature JSON and reason code.
- The fixture enables deterministic offline review; `--source api` exercises the live endpoint.
