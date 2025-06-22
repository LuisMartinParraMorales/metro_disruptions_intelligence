"""Replay a processed feed as a chronological stream of DataFrames."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd
import pyarrow.dataset as ds
from pydantic import BaseModel


class ReplayConfig(BaseModel):
    path: Path
    batch_size: int = 1000
    start_ts: Optional[int] = None
    end_ts: Optional[int] = None


def replay_stream(
    path: Path, batch_size: int = 1000, start_ts: int | None = None, end_ts: int | None = None
) -> Iterator[pd.DataFrame]:
    """Yield DataFrames sorted by ``snapshot_timestamp``."""
    if path.is_file():
        dataset = ds.dataset(str(path), format="parquet")
    else:
        dataset = ds.dataset(str(path), format="parquet", partitioning="hive")

    filt = None
    if start_ts is not None:
        filt = ds.field("snapshot_timestamp") >= start_ts
    if end_ts is not None:
        cond = ds.field("snapshot_timestamp") <= end_ts
        filt = cond if filt is None else filt & cond

    tbl = dataset.to_table(filter=filt)
    df = tbl.to_pandas().sort_values("snapshot_timestamp")
    for i in range(0, len(df), batch_size):
        yield df.iloc[i : i + batch_size]


def _parse_args(argv: list[str] | None = None) -> ReplayConfig:
    parser = argparse.ArgumentParser(description="Replay a processed feed")
    parser.add_argument("path", type=Path, help="Parquet file or folder")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--start-ts", type=int)
    parser.add_argument("--end-ts", type=int)
    args = parser.parse_args(argv)
    return ReplayConfig(**{k: v for k, v in vars(args).items() if v is not None})


def main(argv: list[str] | None = None) -> None:
    cfg = _parse_args(argv)
    for df in replay_stream(cfg.path, cfg.batch_size, cfg.start_ts, cfg.end_ts):
        print(df)


if __name__ == "__main__":
    main()

