from pathlib import Path

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

