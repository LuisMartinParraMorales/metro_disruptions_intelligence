"""Command line entry points for :mod:`metro_disruptions_intelligence`."""

from datetime import datetime
from pathlib import Path

import click
import pandas as pd
import pytz

from .etl.ingest_rt import _parse_cli_time, ingest_all_rt, union_all_feeds
from .etl.static_ingest import ingest_static_gtfs
from .features import SnapshotFeatureBuilder, build_route_map, write_features
from .processed_reader import compose_path, discover_all_snapshot_minutes


@click.group()
@click.version_option(package_name="metro_disruptions_intelligence")
def cli() -> None:
    """Command line interface for ``metro_disruptions_intelligence``."""
    pass


@cli.command("ingest-static")
@click.argument("gtfs_dir", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=Path("data/processed/static"),
    show_default=True,
    help="Directory to store processed Parquet and optional DuckDB",
)
@click.option(
    "--persist-duckdb", is_flag=True, help="Persist the intermediate DuckDB DB"
)
def ingest_static_cmd(gtfs_dir: Path, output_dir: Path, persist_duckdb: bool) -> None:
    """Ingest static GTFS files into Parquet tables."""
    ingest_static_gtfs(gtfs_dir, output_dir, persist_duckdb)


@cli.command("ingest-rt")
@click.argument("raw_root", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--processed-root",
    type=click.Path(path_type=Path),
    default=Path("data/processed/rt"),
    show_default=True,
    help="Destination directory for partitioned Parquet",
)
@click.option(
    "--union", is_flag=True, help="Create combined station_event.parquet file"
)
@click.option(
    "--start-time", type=str, default=None, help="Process files starting from this time"
)
@click.option(
    "--end-time", type=str, default=None, help="Process files up to this time"
)
def ingest_rt_cmd(
    raw_root: Path,
    processed_root: Path,
    union: bool,
    start_time: str | None,
    end_time: str | None,
) -> None:
    """Ingest realtime JSON feeds into Parquet tables."""
    ingest_all_rt(
        raw_root,
        processed_root,
        start_time=_parse_cli_time(start_time) if start_time else None,
        end_time=_parse_cli_time(end_time) if end_time else None,
    )
    if union:
        output_parquet = processed_root.parent / "station_event.parquet"
        union_all_feeds(processed_root, output_parquet)


@cli.command("generate-features")
@click.argument("processed_root", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output-root",
    type=click.Path(path_type=Path),
    default=Path("data/stations_features_time_series"),
    show_default=True,
    help="Directory for feature Parquet output",
)
@click.option(
    "--start-time",
    type=str,
    default=None,
    help="Process snapshots on or after this time",
)
@click.option(
    "--end-time",
    type=str,
    default=None,
    help="Process snapshots up to this time",
)
def generate_features_cmd(
    processed_root: Path,
    output_root: Path,
    start_time: str | None,
    end_time: str | None,
) -> None:
    """Generate per-minute feature Parquet files from processed realtime data."""
    start_dt = _parse_cli_time(start_time) if start_time else None
    end_dt = _parse_cli_time(end_time) if end_time else None

    route_map = build_route_map(processed_root)
    builder = SnapshotFeatureBuilder(route_map)
    minutes = discover_all_snapshot_minutes(processed_root)

    if start_dt:
        start_ts = int(start_dt.timestamp())
        minutes = [m for m in minutes if m >= start_ts]
    if end_dt:
        end_ts = int(end_dt.timestamp())
        minutes = [m for m in minutes if m <= end_ts]

    for ts in minutes:
        tu_file = compose_path(ts, processed_root, "trip_updates")
        vp_file = compose_path(ts, processed_root, "vehicle_positions")
        if not tu_file.exists() or not vp_file.exists():
            continue

        trip_now = pd.read_parquet(tu_file)
        veh_now = pd.read_parquet(vp_file)
        feats = builder.build_snapshot_features(trip_now, veh_now, ts)
        if feats.empty:
            continue

        feats = feats.reset_index()
        feats["snapshot_timestamp"] = ts

        dt = datetime.fromtimestamp(ts, tz=pytz.UTC)
        out_dir = (
            output_root
            / f"year={dt.year:04d}"
            / f"month={dt.month:02d}"
            / f"day={dt.day:02d}"
        )
        out_file = out_dir / f"stations_feats_{dt:%Y-%d-%m-%H-%M}.parquet"
        write_features(feats, out_file)
