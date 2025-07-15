from pathlib import Path

import pandas as pd

from metro_disruptions_intelligence.etl.ingest_rt import FEEDS, ingest_all_rt, union_all_feeds
from metro_disruptions_intelligence.etl.write_parquet import write_df_to_partitioned_parquet


def test_union_all_feeds_streaming(tmp_path: Path) -> None:
    processed_root = tmp_path / "processed" / "rt"
    ingest_all_rt(Path("sample_data/rt"), processed_root)

    # Duplicate each feed with a shifted timestamp to create multiple partitions
    for feed in FEEDS:
        src_file = next((processed_root / feed).rglob("*.parquet"))
        df = pd.read_parquet(src_file).drop(columns=["year", "month", "day"])
        df["snapshot_timestamp"] += 86_400
        write_df_to_partitioned_parquet(df, processed_root / feed, f"extra_{feed}")

    out = processed_root.parent / "station_event.parquet"
    union_all_feeds(processed_root, out)

    expected_frames = []
    for feed in FEEDS:
        files = sorted((processed_root / feed).rglob("*.parquet"))
        if files:
            df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
            df = df.assign(feed_type=feed)
            expected_frames.append(df)
    expected = pd.concat(expected_frames, ignore_index=True)

    result = pd.read_parquet(out)
    pd.testing.assert_frame_equal(result, expected)
