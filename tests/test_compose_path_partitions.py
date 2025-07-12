from datetime import datetime
from pathlib import Path

import pytz

from metro_disruptions_intelligence.processed_reader import compose_path


def test_compose_path_partition(tmp_path: Path) -> None:
    base = tmp_path / "trip_updates" / "year=2025" / "month=05" / "day=22"
    base.mkdir(parents=True)
    target = base / "trip_updates_2025-22-05-11-55.parquet"
    target.touch()
    london = pytz.timezone("Europe/London")
    ts = int(london.localize(datetime(2025, 5, 22, 11, 55)).timestamp())
    assert compose_path(ts, tmp_path, "trip_updates") == target
