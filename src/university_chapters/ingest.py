import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import requests
from .config import API_URL, TARGET_STATES


def fetch_api(timeout: int = 30) -> dict[str, Any]:
    params = {
        "where": "State IN ('CA','OR','WA')",
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "json",
    }
    response = requests.get(API_URL, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise RuntimeError(f"ArcGIS API error: {payload['error']}")
    if "features" not in payload:
        raise RuntimeError("ArcGIS response has no 'features' array")
    return payload


def load_fixture(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if "features" not in payload:
        raise ValueError("Fixture must contain a 'features' array")
    return payload


def add_synthetic_dq_rows(payload: dict[str, Any]) -> dict[str, Any]:
    cloned = json.loads(json.dumps(payload))
    cloned["features"].extend([
        {
            "attributes": {"OBJECTID": 900001, "ChapterID": "CA-DQ-Q1", "University_Chapter": "Invalid Coordinates Fixture", "City": "Sacramento", "State": "CA"},
            "geometry": {"x": 999.0, "y": 38.58},
        },
        {
            "attributes": {"OBJECTID": 900002, "ChapterID": "CA-DQ-W1", "University_Chapter": "Missing City Fixture", "City": "UNKNOWN", "State": "CA"},
            "geometry": {"x": -121.49, "y": 38.58},
        },
    ])
    return cloned


def write_bronze(payload: dict[str, Any], bronze_root: Path, run_id: str, source_mode: str) -> Path:
    run_dir = bronze_root / f"ingest_date={run_id[:8]}" / f"run_id={run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    envelope = {
        "ingest_run_id": run_id,
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_mode": source_mode,
        "source_url": API_URL,
        "target_states": list(TARGET_STATES),
        "payload": payload,
    }
    output = run_dir / "payload.json"
    output.write_text(json.dumps(envelope, indent=2), encoding="utf-8")
    return output
