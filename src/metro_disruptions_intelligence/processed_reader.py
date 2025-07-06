"""Utilities for loading processed GTFS-realtime Parquet files."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

FEEDS = ["alerts", "trip_updates", "vehicle_positions"]


def load_rt_dataset(
    processed_root: Path,
    feeds: Iterable[str] | None = None,
    *,
    output_file: Path | str | None = None,
) -> pd.DataFrame:
    """Load realtime Parquet partitions into a single DataFrame.

    Parameters
    ----------
    processed_root:
        Directory containing ``alerts``/``trip_updates``/``vehicle_positions`` subfolders.
        Parquet partitions are discovered recursively using ``Path.rglob`` so
        the ``year=YYYY/month=MM/day=DD`` layout is handled automatically.
    feeds:
        Optional subset of feed names to load. Defaults to all three feeds.
    output_file:
        Optional Parquet file to write the concatenated dataset to. Parent
        directories are created if necessary. Uses pyarrow with ``snappy``
        compression.

    Returns:
    -------
    pandas.DataFrame
        Concatenated DataFrame with a ``feed_type`` column.
    """
    feeds = list(feeds) if feeds is not None else FEEDS
    dfs: list[pd.DataFrame] = []
    for feed in feeds:
        path = processed_root / feed
        if not path.exists():
            continue
        files = sorted(path.rglob("*.parquet"))
        if not files:
            continue
        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
        df["feed_type"] = feed
        dfs.append(df)
    result = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    if output_file is not None and not result.empty:
        out_path = Path(output_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_parquet(out_path, compression="snappy", index=False)
        logging.info("Wrote combined realtime dataset to %s", out_path)
    return result
