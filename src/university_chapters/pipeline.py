import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession
from .config import Settings
from .ingest import add_synthetic_dq_rows, fetch_api, load_fixture, write_bronze
from .transforms import apply_quality, build_gold, features_to_df


def create_spark() -> SparkSession:
    return (SparkSession.builder.master("local[*]")
            .appName("university-chapters-medallion")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def run(root: Path, source: str, inject_dq_fixtures: bool, run_id: str | None = None) -> dict:
    settings = Settings(root.resolve())
    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    fixture = settings.root / "fixtures" / "university_chapters.json"
    payload = fetch_api() if source == "api" else load_fixture(fixture)
    if not payload["features"]:
        raise RuntimeError("Source returned an empty batch; Gold was not published")
    if inject_dq_fixtures:
        payload = add_synthetic_dq_rows(payload)
    bronze_path = write_bronze(payload, settings.bronze, run_id, source)
    spark = create_spark()
    try:
        source_df = features_to_df(spark, payload["features"], run_id).cache()
        if source_df.filter("state = 'CA'").count() == 0:
            raise RuntimeError("CA unexpectedly dropped to zero; Gold was not published")
        silver, quarantine = apply_quality(source_df)
        gold = build_gold(silver)
        silver_path = settings.silver / f"run_id={run_id}"
        quarantine_path = settings.quarantine / f"run_id={run_id}"
        settings.gold.mkdir(parents=True, exist_ok=True)
        silver.write.mode("overwrite").parquet(str(silver_path))
        quarantine.write.mode("overwrite").json(str(quarantine_path))
        # Gold v1 is a current snapshot, so reruns replace rather than duplicate records.
        gold.write.mode("overwrite").parquet(str(settings.gold))
        metrics = {
            "ingest_run_id": run_id,
            "source_mode": source,
            "bronze_path": str(bronze_path.relative_to(settings.root)),
            "rows_in": source_df.count(),
            "rows_quarantined": quarantine.count(),
            "rows_warned": silver.filter("dq_status = 'WARNING'").count(),
            "rows_ok": silver.filter("dq_status = 'OK'").count(),
            "rows_gold": gold.count(),
            "status": "SUCCESS",
        }
        settings.metrics.mkdir(parents=True, exist_ok=True)
        (settings.metrics / f"{run_id}.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        return metrics
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the university chapters medallion pipeline")
    parser.add_argument("--source", choices=["fixture", "api"], default="fixture")
    parser.add_argument("--inject-dq-fixtures", action="store_true")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--run-id")
    args = parser.parse_args()
    print(json.dumps(run(args.root, args.source, args.inject_dq_fixtures, args.run_id), indent=2))

if __name__ == "__main__":
    main()
