"""Helper for writing partitioned Parquet files."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def write_df_to_partitioned_parquet(
    df: pd.DataFrame,
    base_dir: Path,
    filename_prefix: str,
    timestamp_column: str = "snapshot_timestamp",
) -> Path | None:
    """Write ``df`` to ``base_dir`` partitioned by year/month/day.

    Parameters
    ----------
    df:
        DataFrame to serialise.
    base_dir:
        Root folder for partitions.
    filename_prefix:
        Base filename (without extension).
    timestamp_column:
        Column containing UNIX timestamps used for partitioning.

    Returns
    -------
    Optional[Path]
        Path to the written file or ``None`` if ``df`` was empty.
    """

    if df.empty:
        return None

    to_dt = lambda ts: datetime.utcfromtimestamp(int(ts))
    df2 = df.copy()
    df2["year"] = df2[timestamp_column].map(lambda ts: to_dt(ts).year)
    df2["month"] = df2[timestamp_column].map(lambda ts: to_dt(ts).month)
    df2["day"] = df2[timestamp_column].map(lambda ts: to_dt(ts).day)

    y = int(df2["year"].iloc[0])
    m = int(df2["month"].iloc[0])
    d = int(df2["day"].iloc[0])
    out_dir = base_dir / f"year={y:04d}" / f"month={m:02d}" / f"day={d:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{filename_prefix}.parquet"
    pq.write_table(pa.Table.from_pandas(df2), out_file)
    return out_file
