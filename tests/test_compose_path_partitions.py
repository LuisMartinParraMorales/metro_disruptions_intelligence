from datetime import datetime
from pathlib import Path

import pytz

from metro_disruptions_intelligence.processed_reader import compose_path, snapshot_path

FEEDS = ["trip_updates", "vehicle_positions", "alerts"]


def test_compose_path_partition(tmp_path: Path) -> None:
    london = pytz.timezone("Europe/London")
    ts = int(london.localize(datetime(2025, 5, 22, 11, 55)).timestamp())
    for feed in FEEDS:
        base = tmp_path / feed / "year=2025" / "month=05" / "day=22"
        base.mkdir(parents=True)
        target = base / f"{feed}_2025-22-05-11-55.parquet"
        target.touch()
        assert compose_path(ts, tmp_path, feed) == target


def test_snapshot_path(tmp_path: Path) -> None:
    ts = int(datetime(2025, 5, 22, 11, 55, tzinfo=pytz.UTC).timestamp())
    base = tmp_path / "stations_feats" / "year=2025" / "month=05" / "day=22"
    base.mkdir(parents=True)
    target = base / "stations_feats_2025-22-05-11-55.parquet"
    target.touch()
    assert snapshot_path(ts, tmp_path) == target
