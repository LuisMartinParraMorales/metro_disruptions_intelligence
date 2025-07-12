"""Utilities for loading processed GTFS-realtime Parquet files."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
import pytz

_TZ_LONDON = pytz.timezone("Europe/London")
# Filenames are stamped in London local time (GMT/BST); we convert to UTC
# to align with ``snapshot_timestamp`` values.

FEEDS = ["alerts", "trip_updates", "vehicle_positions"]

# ---------------------------------------------------------------------------
# Internal helpers for dealing with filename date formats
# ---------------------------------------------------------------------------
_PATTERNS = (
    "%Y-%d-%m-%H-%M",  # day-month
    "%Y-%m-%d-%H-%M",  # month-day
)


def _try_parse(ts_part: str) -> datetime | None:
    """Return UTC datetime parsed from a London-based filename."""
    for pat in _PATTERNS:
        try:
            naive = datetime.strptime(ts_part, pat)
            local = _TZ_LONDON.localize(naive)
            return local.astimezone(pytz.UTC)
        except ValueError:
            continue
    return None


def _fname(dt: datetime, feed: str, day_first: bool) -> str:
    pat = "%Y-%d-%m-%H-%M" if day_first else "%Y-%m-%d-%H-%M"
    local = dt.astimezone(_TZ_LONDON)
    return f"{feed}_{local.strftime(pat)}.parquet"


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


def discover_all_snapshot_minutes(root: Path) -> list[int]:
    """Return epoch-seconds for every snapshot minute present on disk."""
    minutes: list[int] = []
    for f in (root / "trip_updates").rglob("trip_updates_*.parquet"):
        ts_part = f.stem.split("trip_updates_")[-1]
        dt = _try_parse(ts_part)
        if dt:
            minutes.append(int(dt.timestamp()))
    return sorted(set(minutes))


def compose_path(ts: int, root: Path, feed: str) -> Path:
    """Construct Parquet path for ``feed`` at ``ts``.

    Returns the existing path if either date format is present.
    """
    dt = datetime.fromtimestamp(ts, tz=_TZ_LONDON)

    base = root / feed / f"year={dt.year:04d}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
    path_d = base / _fname(dt, feed, day_first=True)
    if path_d.exists():
        return path_d
    return base / _fname(dt, feed, day_first=False)
