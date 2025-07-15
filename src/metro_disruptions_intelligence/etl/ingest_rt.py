"""Ingest GTFS-realtime JSON feeds into partitioned Parquet files."""

from __future__ import annotations

import argparse
import logging
import re
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel

from .parse_alerts import parse_one_alert_file
from .parse_trip_updates import parse_one_trip_update_file
from .parse_vehicle_positions import parse_one_vehicle_position_file
from .write_parquet import write_df_to_partitioned_parquet

# Raw realtime JSON files are named using the day-first format
# YYYY_DD_MM_HH_MM_SS.  Seconds are always zero but are kept here
# for completeness.
FILENAME_RE = re.compile(r"(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})_(\d{2})\.json")


class IngestRTConfig(BaseModel):
    """Configuration for realtime ingestion."""

    raw_root: Path
    processed_root: Path
    union: bool = False
    start_time: datetime | None = None
    end_time: datetime | None = None


FEEDS = ["alerts", "trip_updates", "vehicle_positions"]


def _file_datetime(path: Path) -> datetime | None:
    """Return the timestamp encoded in ``path`` or ``None`` if not parseable."""
    m = FILENAME_RE.match(path.name)
    if not m:
        return None
    yyyy, dd, mm, hh, mi, ss = map(int, m.groups())
    return datetime(yyyy, mm, dd, hh, mi, ss)


def _parse_cli_time(value: str) -> datetime:
    """Parse a command line datetime string."""
    patterns = (
        "%Y_%d_%m_%H_%M_%S",
        "%Y_%m_%d_%H_%M_%S",
        "%Y-%d-%mT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%d-%m",
        "%Y-%m-%d",
    )
    for fmt in patterns:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"Invalid datetime format: {value}")


def _prefix_from_name(path: Path) -> str:
    m = FILENAME_RE.match(path.name)
    if not m:
        return path.stem
    yyyy, dd, mm, hh, mi, _ = m.groups()
    # output filenames follow YYYY-DD-MM-HH-MM
    return f"{yyyy}-{dd}-{mm}-{hh}-{mi}"


def ingest_all_rt(
    raw_root: Path,
    processed_root: Path,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> None:
    """Parse realtime JSON files under ``raw_root``.

    Parameters
    ----------
    raw_root:
        Directory containing realtime JSON files organised by feed.
    processed_root:
        Destination directory for partitioned Parquet files.
    start_time, end_time:
        Optional datetime range to filter the files processed.
    """
    for feed in FEEDS:
        raw_dir = raw_root / feed
        if raw_dir.exists():
            files = sorted(raw_dir.glob("*.json"))
            if not files:
                pattern = f"*{feed}*.json"
                files = sorted(raw_root.glob(pattern))
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

        if start_time or end_time:
            filtered = []
            for jf in files:
                ts = _file_datetime(jf)
                if ts is None:
                    continue
                if start_time and ts < start_time:
                    continue
                if end_time and ts > end_time:
                    continue
                filtered.append(jf)
            files = filtered

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
    """Concatenate all feeds into a single Parquet file using streaming."""
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq

    column_order: list[str] = []
    column_types: dict[str, pa.DataType] = {}
    partitioning = ds.partitioning(
        pa.schema({"year": pa.int64(), "month": pa.int64(), "day": pa.int64()}), flavor="hive"
    )

    # Discover the full schema in feed order, mimicking pandas concat behaviour
    for feed in FEEDS:
        part_path = processed_root / feed
        if not part_path.exists():
            continue
        dataset = ds.dataset(part_path, format="parquet", partitioning=partitioning)
        for name in dataset.schema.names:
            if name not in column_order:
                column_order.append(name)
                column_types[name] = dataset.schema.field(name).type
        if "feed_type" not in column_order:
            column_order.append("feed_type")
            column_types["feed_type"] = pa.string()

    if not column_order:
        return output_parquet

    schema = pa.schema([(name, column_types[name]) for name in column_order])
    writer = pq.ParquetWriter(output_parquet, schema)

    for feed in FEEDS:
        part_path = processed_root / feed
        if not part_path.exists():
            continue
        dataset = ds.dataset(part_path, format="parquet", partitioning=partitioning)
        for fragment in dataset.get_fragments():
            table = fragment.to_table()
            df = table.to_pandas()
            df["feed_type"] = feed
            df = df.reindex(columns=column_order)
            writer.write_table(pa.Table.from_pandas(df, schema=schema, preserve_index=False))

    writer.close()
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
        "--union", action="store_true", help="Also create a combined station_event.parquet file"
    )
    parser.add_argument(
        "--start-time",
        type=str,
        default=None,
        help=(
            "Only ingest files with timestamp >= START_TIME. "
            "Format: YYYY-DD-MM[-HH-MM-SS]; underscores are also accepted."
        ),
    )
    parser.add_argument(
        "--end-time",
        type=str,
        default=None,
        help=(
            "Only ingest files with timestamp <= END_TIME. "
            "Format: YYYY-DD-MM[-HH-MM-SS]; underscores are also accepted."
        ),
    )
    args = parser.parse_args(argv)
    cfg_dict = vars(args)
    if cfg_dict["start_time"]:
        cfg_dict["start_time"] = _parse_cli_time(cfg_dict["start_time"])
    if cfg_dict["end_time"]:
        cfg_dict["end_time"] = _parse_cli_time(cfg_dict["end_time"])
    return IngestRTConfig(**cfg_dict)


def main(argv: list[str] | None = None) -> None:
    """Entry point for the ``ingest_rt`` CLI."""
    cfg = _parse_args(argv)
    ingest_all_rt(
        cfg.raw_root, cfg.processed_root, start_time=cfg.start_time, end_time=cfg.end_time
    )
    if cfg.union:
        output_parquet = cfg.processed_root.parent / "station_event.parquet"
        union_all_feeds(cfg.processed_root, output_parquet)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    main()
