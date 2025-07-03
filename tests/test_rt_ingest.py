from pathlib import Path
from datetime import datetime

import pandas as pd

from metro_disruptions_intelligence.etl.ingest_rt import ingest_all_rt, union_all_feeds


FEEDS = ["alerts", "trip_updates", "vehicle_positions"]


def test_rt_ingest(tmp_path):
    processed_root = tmp_path / "processed" / "rt"
    ingest_all_rt(Path("sample_data/rt"), processed_root)

    for feed in FEEDS:
        files = list((processed_root / feed).rglob("*.parquet"))
        assert files
        df = pd.read_parquet(files[0])
        assert not df.empty

    out = processed_root.parent / "station_event.parquet"
    union_all_feeds(processed_root, out)
    assert out.exists()


def test_rt_ingest_with_range(tmp_path):
    processed_root = tmp_path / "processed" / "rt"
    src = Path("sample_data/rt")
    rename_map = {
        src / "sample_alert.json": tmp_path / "alerts" / "2001_01_01_00_00_00.json",
        src
        / "sample_trip_update.json": tmp_path
        / "trip_updates"
        / "2001_01_01_00_00_00.json",
        src
        / "sample_vehicles_position.json": tmp_path
        / "vehicle_positions"
        / "2001_01_01_00_00_00.json",
    }
    for s, d in rename_map.items():
        d.parent.mkdir(parents=True, exist_ok=True)
        d.write_text(s.read_text(), encoding="utf-8")

    ingest_all_rt(
        tmp_path,
        processed_root,
        start_time=datetime(2001, 1, 1),
        end_time=datetime(2001, 1, 2),
    )
    for feed in FEEDS:
        files = list((processed_root / feed).rglob("*.parquet"))
        assert files
