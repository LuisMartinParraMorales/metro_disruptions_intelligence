from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz

from metro_disruptions_intelligence.etl.ingest_rt import FEEDS, ingest_all_rt, union_all_feeds
from metro_disruptions_intelligence.features import SnapshotFeatureBuilder
from metro_disruptions_intelligence.processed_reader import compose_path

HEADER_JSON = '{"header": {"timestamp": 0}}'


def _write_header_only(path: Path) -> None:
    path.write_text(HEADER_JSON, encoding="utf-8")


def test_pipeline_header_only(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    for feed in FEEDS:
        feed_dir = raw_root / feed
        feed_dir.mkdir(parents=True, exist_ok=True)
        _write_header_only(feed_dir / "2020_01_01_00_00_00.json")

    processed_root = tmp_path / "processed" / "rt"
    ingest_all_rt(raw_root, processed_root)
    union_all_feeds(processed_root, processed_root.parent / "station_event.parquet")

    ts = int(datetime(2020, 1, 1, tzinfo=pytz.UTC).timestamp())
    tu = pd.read_parquet(compose_path(ts, processed_root, "trip_updates"))
    vp = pd.read_parquet(compose_path(ts, processed_root, "vehicle_positions"))

    builder = SnapshotFeatureBuilder({("R", 0): ["STOP"]})
    builder.build_snapshot_features(tu, vp, ts)
