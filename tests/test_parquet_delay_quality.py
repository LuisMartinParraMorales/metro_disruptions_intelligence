from pathlib import Path

import pandas as pd


def test_may_delay_non_null() -> None:
    file = Path(
        "sample_data/rt_parquet/trip_updates/year=2025/month=05/day=06/"
        "trip_updates_2025-06-05-01-20.parquet"
    )
    df = pd.read_parquet(file)
    assert df["arrival_delay"].notna().mean() >= 0.95
    assert df["departure_delay"].notna().mean() >= 0.95
