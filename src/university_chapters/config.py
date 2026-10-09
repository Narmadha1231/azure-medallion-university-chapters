from dataclasses import dataclass
from pathlib import Path

API_URL = (
    "https://services2.arcgis.com/5I7u4SJE1vUr79JC/arcgis/rest/services/"
    "UniversityChapters_Public/FeatureServer/0/query"
)
TARGET_STATES = ("CA", "OR", "WA")

@dataclass(frozen=True)
class Settings:
    root: Path

    @property
    def data(self) -> Path: return self.root / "data"
    @property
    def bronze(self) -> Path: return self.data / "bronze" / "university_chapters"
    @property
    def silver(self) -> Path: return self.data / "silver" / "university_chapters"
    @property
    def gold(self) -> Path: return self.data / "gold" / "university_chapters" / "v1"
    @property
    def quarantine(self) -> Path: return self.data / "quarantine" / "university_chapters"
    @property
    def metrics(self) -> Path: return self.data / "run_metrics"
