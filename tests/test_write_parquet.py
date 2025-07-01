from datetime import datetime
from pathlib import Path

import pandas as pd

from metro_disruptions_intelligence.etl.write_parquet import write_df_to_partitioned_parquet


def test_partition_directory_structure_and_prefix(tmp_path: Path) -> None:
    ts = int(datetime(2020, 5, 3).timestamp())
    df = pd.DataFrame({"snapshot_timestamp": [ts], "value": [1]})

    out = write_df_to_partitioned_parquet(df, tmp_path, "myprefix")

    expected = tmp_path / "year=2020" / "month=05" / "day=03" / "myprefix.parquet"
    assert out == expected
    assert out.exists()
    assert out.parent.is_dir()


def test_empty_dataframe_returns_none(tmp_path: Path) -> None:
    df = pd.DataFrame({"snapshot_timestamp": [], "value": []})

    out = write_df_to_partitioned_parquet(df, tmp_path, "empty")

    assert out is None
    assert not any(tmp_path.rglob("*.parquet"))
