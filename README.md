# Azure Medallion University Chapters Data Product

A clone-and-run PySpark implementation of Bronze → Silver → Gold for public ArcGIS university chapter data. It includes deterministic fixtures, live API mode, quarantine and warning behavior, run metrics, tests, architecture, and a versioned data product contract.

## What is implemented
- CA / OR / WA source scoping
- Bronze raw payload history by ingest date and run ID
- Silver flattening, typing, deduplication, DQ status, and warnings
- DQ-Q1 hard quarantine for invalid coordinates
- DQ-W1 soft warning for missing or `UNKNOWN` city
- Gold consumer snapshot with no quarantined rows
- Empty-batch and unexpected zero-CA publication guards
- Automated PySpark tests and per-run counts

## Prerequisites
- Python 3.10+
- Java 17

## Run from a clone

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=src
python -m university_chapters.pipeline --source fixture --inject-dq-fixtures
pytest -q
```

Windows PowerShell activation:

```powershell
.venv\Scripts\Activate.ps1
$env:PYTHONPATH="src"
```

## Live API run

```bash
export PYTHONPATH=src
python -m university_chapters.pipeline --source api --inject-dq-fixtures
```

The API run fails loudly on HTTP/API errors, an empty batch, or an unexpected zero count for CA. OR and WA are allowed to be empty.

## Review the outputs

```text
data/bronze/university_chapters/ingest_date=<date>/run_id=<run_id>/payload.json
data/silver/university_chapters/run_id=<run_id>/
data/gold/university_chapters/v1/
data/quarantine/university_chapters/run_id=<run_id>/
data/run_metrics/<run_id>.json
```

Example Spark inspection:

```python
spark.read.parquet("data/gold/university_chapters/v1").show(truncate=False)
spark.read.json("data/quarantine/university_chapters/run_id=<run_id>").show(truncate=False)
```

## DQ demonstration
Run with `--inject-dq-fixtures` to add:
- `CA-DQ-Q1`: invalid longitude, written only to quarantine
- `CA-DQ-W1`: `UNKNOWN` city, published to Gold with a warning

## Repository map

```text
src/university_chapters/  pipeline code
fixtures/                 deterministic source fixture
contracts/                consumer-facing contract
architecture/             Mermaid architecture and decisions
tests/                    automated DQ checks
data/                     generated medallion outputs
```

## Idempotency
Bronze and run-specific Silver/Quarantine data are preserved by run ID. Gold v1 is a current snapshot and is overwritten on each successful run, preventing duplicate accumulation.

## Production extensions
For production, replace local paths with ADLS Gen2/Delta, add managed identity and secret-store integration, orchestrate with ADF or Databricks Workflows, register Gold in a catalog, add monitoring/alerting, and implement CI/CD and environment promotion.

## Submission checklist
1. Create a GitHub repository and push this project.
2. Verify the fixture command and `pytest -q` succeed from a clean clone.
3. Keep generated `data/` outputs out of Git unless sample evidence is intentionally required.
4. Grant repository access to `dpl-tech-uk`.
5. Share the repository URL with the reviewer.
