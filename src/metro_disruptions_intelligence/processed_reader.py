"""Utilities for loading processed GTFS-realtime Parquet files."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable
from typing import Optional

import pandas as pd
import pytz

from .utils_gtfsrt import _fname, try_parse

_TZ_LONDON = pytz.timezone("Europe/London")
# Filenames are stamped in London local time (GMT/BST); we convert to UTC
# to align with ``snapshot_timestamp`` values.

FEEDS = ["alerts", "trip_updates", "vehicle_positions"]

# ---------------------------------------------------------------------------
# Internal helpers for dealing with filename date formats
# ---------------------------------------------------------------------------


def _try_parse(path: Path) -> datetime | None:
    """Return UTC datetime parsed from ``path`` using partition hints."""
    ts_part = path.stem.split("_")[-1]
    try:
        year = int(path.parents[2].name.split("=")[-1])
        month = int(path.parents[1].name.split("=")[-1])
        day = int(path.parent.name.split("=")[-1])
    except (IndexError, ValueError):
        return None
    return try_parse(ts_part, year, month, day)


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


def discover_snapshot_minutes(root: Path, feed: str = "trip_updates") -> list[int]:
    """Return epoch seconds for every snapshot minute for ``feed``."""
    minutes: list[int] = []
    for f in (root / feed).rglob(f"{feed}_*.parquet"):
        dt = _try_parse(f)
        if dt:
            minutes.append(int(dt.timestamp()))
    return sorted(set(minutes))


def discover_all_snapshot_minutes(root: Path) -> list[int]:
    """Return epoch-seconds for every ``trip_updates`` snapshot minute."""
    return discover_snapshot_minutes(root, "trip_updates")


def compose_path(ts: int, root: Path, feed: str) -> Path:
    """Construct Parquet path for feed at ts.

    Returns the existing path for the configured day-first filename convention.
    """
    dt = datetime.fromtimestamp(ts, tz=_TZ_LONDON)

    base = root / feed / f"year={dt.year:04d}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
    return base / _fname(dt, feed)
