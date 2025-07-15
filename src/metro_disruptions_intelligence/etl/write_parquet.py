"""Helper for writing partitioned Parquet files."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytz


def write_df_to_partitioned_parquet(
    df: pd.DataFrame,
    base_dir: Path,
    filename_prefix: str,
    timestamp_column: str = "snapshot_timestamp",
    write_empty: bool = False,
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

    Returns:
    -------
    Optional[Path]
        Path to the written file or ``None`` if ``df`` was empty.
    """
    if df.empty and not write_empty:
        return None

    tz_london = pytz.timezone("Europe/London")

    def to_dt(ts: int) -> datetime:
        return datetime.fromtimestamp(int(ts), tz_london)

    df2 = df.copy()
    if not df2.empty:
        df2["year"] = df2[timestamp_column].map(lambda ts: to_dt(ts).year)
        df2["month"] = df2[timestamp_column].map(lambda ts: to_dt(ts).month)
        df2["day"] = df2[timestamp_column].map(lambda ts: to_dt(ts).day)
        year = int(df2["year"].iloc[0])
        month = int(df2["month"].iloc[0])
        day = int(df2["day"].iloc[0])
    else:
        import re

        m = re.search(r"(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})$", filename_prefix)
        if m:
            year = int(m.group(1))
            day = int(m.group(2))
            month = int(m.group(3))
        else:
            # fallback to current UTC date
            now = datetime.now(tz_london)
            year = now.year
            month = now.month
            day = now.day
    out_dir = base_dir / f"year={year:04d}" / f"month={month:02d}" / f"day={day:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{filename_prefix}.parquet"
    pq.write_table(pa.Table.from_pandas(df2), out_file)
    return out_file
