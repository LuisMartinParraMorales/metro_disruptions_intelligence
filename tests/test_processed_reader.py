from pathlib import Path

import pandas as pd

from metro_disruptions_intelligence.etl.ingest_rt import ingest_all_rt
from metro_disruptions_intelligence.processed_reader import load_rt_dataset


def test_load_rt_dataset(tmp_path: Path) -> None:
    processed_root = tmp_path / "processed" / "rt"
    ingest_all_rt(Path("sample_data/rt"), processed_root)

    df = load_rt_dataset(processed_root)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert set(df["feed_type"].unique()) == {"alerts", "trip_updates", "vehicle_positions"}
