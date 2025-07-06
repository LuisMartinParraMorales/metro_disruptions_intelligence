"""Utilities for loading processed GTFS-realtime Parquet files."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

FEEDS = ["alerts", "trip_updates", "vehicle_positions"]


def load_rt_dataset(processed_root: Path, feeds: Iterable[str] | None = None) -> pd.DataFrame:
    """Load realtime Parquet partitions into a single DataFrame.

    Parameters
    ----------
    processed_root:
        Directory containing ``alerts``/``trip_updates``/``vehicle_positions`` subfolders.
    feeds:
        Optional subset of feed names to load. Defaults to all three feeds.

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
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()
