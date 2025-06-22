"""Ingest GTFS-realtime JSON feeds into partitioned Parquet files."""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

import pandas as pd
from pydantic import BaseModel

from .parse_trip_updates import parse_one_trip_update_file
from .parse_vehicle_positions import parse_one_vehicle_position_file
from .parse_alerts import parse_one_alert_file
from .write_parquet import write_df_to_partitioned_parquet

FILENAME_RE = re.compile(r"(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})_(\d{2})\.json")


class IngestRTConfig(BaseModel):
    raw_root: Path
    processed_root: Path
    union: bool = False


FEEDS = ["alerts", "trip_updates", "vehicle_positions"]


def _prefix_from_name(path: Path) -> str:
    m = FILENAME_RE.match(path.name)
    if not m:
        return path.stem
    yyyy, mm, dd, hh, mi, _ = m.groups()
    return f"{yyyy}-{mm}-{dd}-{hh}-{mi}"


def ingest_all_rt(raw_root: Path, processed_root: Path) -> None:
    """Parse all realtime JSON files under ``raw_root``."""
    for feed in FEEDS:
        raw_dir = raw_root / feed
        if raw_dir.exists():
            files = sorted(raw_dir.glob("*.json"))
        else:
            pattern = f"*{feed}*.json"
            files = sorted(raw_root.glob(pattern))
            if not files:
                if feed == "alerts":
                    files = sorted(raw_root.glob("*alert*.json"))
                elif feed == "trip_updates":
                    files = sorted(raw_root.glob("*trip_update*.json"))
                elif feed == "vehicle_positions":
                    files = sorted(raw_root.glob("*vehicle*position*.json"))

        out_dir = processed_root / feed
        out_dir.mkdir(parents=True, exist_ok=True)
        for jf in files:
            prefix = _prefix_from_name(jf)
            if feed == "trip_updates":
                df = parse_one_trip_update_file(jf)
            elif feed == "vehicle_positions":
                df = parse_one_vehicle_position_file(jf)
            else:
                df = parse_one_alert_file(jf)
            write_df_to_partitioned_parquet(df, out_dir, f"{feed}_{prefix}")
            logging.info("ingested %s -> %d rows", jf.name, len(df))


def union_all_feeds(processed_root: Path, output_parquet: Path) -> Path:
    """Concatenate all feeds into a single Parquet file."""
    dfs = []
    for feed in FEEDS:
        part_path = processed_root / feed
        files = list(part_path.rglob("*.parquet"))
        if not files:
            continue
        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
        df = df.assign(feed_type=feed)
        dfs.append(df)
    if dfs:
        pd.concat(dfs, ignore_index=True).to_parquet(output_parquet, index=False)
    return output_parquet


def _parse_args(argv: list[str] | None = None) -> IngestRTConfig:
    parser = argparse.ArgumentParser(description="Ingest realtime GTFS feeds")
    parser.add_argument("raw_root", type=Path, help="Directory with raw JSON subfolders")
    parser.add_argument(
        "--processed-root",
        type=Path,
        default=Path("data/processed/rt"),
        help="Destination directory for partitioned Parquet",
    )
    parser.add_argument(
        "--union",
        action="store_true",
        help="Also create a combined station_event.parquet file",
    )
    args = parser.parse_args(argv)
    return IngestRTConfig(**vars(args))


def main(argv: list[str] | None = None) -> None:
    cfg = _parse_args(argv)
    ingest_all_rt(cfg.raw_root, cfg.processed_root)
    if cfg.union:
        output_parquet = cfg.processed_root.parent / "station_event.parquet"
        union_all_feeds(cfg.processed_root, output_parquet)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    main()

