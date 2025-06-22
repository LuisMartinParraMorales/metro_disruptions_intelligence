from pathlib import Path

import pandas as pd

from metro_disruptions_intelligence.etl.ingest_rt import ingest_all_rt, union_all_feeds
from metro_disruptions_intelligence.etl.replay_stream import replay_stream


def test_replay_stream_monotonic(tmp_path):
    processed_root = tmp_path / "processed" / "rt"
    ingest_all_rt(Path("sample_data/rt"), processed_root)
    out = processed_root.parent / "station_event.parquet"
    union_all_feeds(processed_root, out)

    timestamps = []
    for batch in replay_stream(out, batch_size=1):
        if not batch.empty:
            timestamps.append(int(batch["snapshot_timestamp"].iloc[0]))
    assert timestamps == sorted(timestamps)

