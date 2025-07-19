"""Command line entry points for :mod:`metro_disruptions_intelligence`."""

import logging
from datetime import datetime
from pathlib import Path

import click
import pandas as pd
import pytz

from .detect.streaming_iforest import StreamingIForestDetector
from .detect.tune_iforest import run_grid_search
from .etl.ingest_rt import _parse_cli_time, ingest_all_rt, union_all_feeds
from .etl.static_ingest import ingest_static_gtfs
from .features import SnapshotFeatureBuilder, build_route_map, write_features
from .processed_reader import compose_path, discover_all_snapshot_minutes

logger = logging.getLogger(__name__)


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
@click.option("--persist-duckdb", is_flag=True, help="Persist the intermediate DuckDB DB")
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
@click.option("--union", is_flag=True, help="Create combined station_event.parquet file")
@click.option("--start-time", type=str, default=None, help="Process files starting from this time")
@click.option("--end-time", type=str, default=None, help="Process files up to this time")
def ingest_rt_cmd(
    raw_root: Path, processed_root: Path, union: bool, start_time: str | None, end_time: str | None
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
    "--start-time", type=str, default=None, help="Process snapshots on or after this time"
)
@click.option("--end-time", type=str, default=None, help="Process snapshots up to this time")
def generate_features_cmd(
    processed_root: Path, output_root: Path, start_time: str | None, end_time: str | None
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
            if not tu_file.exists():
                logger.warning("trip_updates file missing for %s", ts)
            if not vp_file.exists():
                logger.warning("vehicle_positions file missing for %s", ts)
            continue

        trip_now = pd.read_parquet(tu_file)
        veh_now = pd.read_parquet(vp_file)
        if veh_now.empty:
            logger.warning("vehicle_positions file %s contains no rows", vp_file)
        feats = builder.build_snapshot_features(trip_now, veh_now, ts)

        feats = feats.reset_index()
        feats["snapshot_timestamp"] = ts

        dt = datetime.fromtimestamp(ts, tz=pytz.UTC)
        out_dir = (
            output_root / f"year={dt.year:04d}" / f"month={dt.month:02d}" / f"day={dt.day:02d}"
        )
        out_file = out_dir / f"stations_feats_{dt:%Y-%d-%m-%H-%M}.parquet"
        write_features(feats, out_file)


@cli.command("detect-anomalies")
@click.option("--processed-root", type=click.Path(exists=True, path_type=Path), required=True)
@click.option(
    "--out-root",
    type=click.Path(path_type=Path),
    default=Path("data/anomaly_scores"),
    show_default=True,
)
@click.option("--config", "config_path", type=click.Path(path_type=Path))
@click.option("--start", "start_time", required=True, type=str)
@click.option("--end", "end_time", required=True, type=str)
def detect_anomalies_cmd(
    processed_root: Path,
    out_root: Path,
    config_path: Path | None,
    start_time: str,
    end_time: str,
) -> None:
    """Stream feature snapshots and score anomalies."""
    start_dt = _parse_cli_time(start_time)
    end_dt = _parse_cli_time(end_time)
    config = config_path if config_path else {}
    det = StreamingIForestDetector(config)

    total = 0
    anomalies = 0
    mean_accum = 0.0

    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())
    for ts in range(start_ts, end_ts, 60):
        dt = datetime.fromtimestamp(ts, tz=pytz.UTC)
        in_file = (
            processed_root
            / f"year={dt.year:04d}"
            / f"month={dt.month:02d}"
            / f"day={dt.day:02d}"
            / f"stations_feats_{dt:%Y-%d-%m-%H-%M}.parquet"
        )
        if not in_file.exists():
            continue
        df = pd.read_parquet(in_file)
        out = det.score_and_update(df)
        logger.info("scored %s -> %d rows", in_file, len(out))
        if out.empty:
            continue
        total += 1
        anomalies += int(out["anomaly_flag"].sum())
        mean_accum += float(out["anomaly_score"].mean())
        out_dir = (
            out_root
            / f"year={dt.year:04d}"
            / f"month={dt.month:02d}"
            / f"day={dt.day:02d}"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"anomaly_scores_{dt:%Y-%d-%m-%H-%M}.parquet"
        out.to_parquet(out_file, index=False)
    mean_score = mean_accum / total if total else 0.0
    click.echo(
        f"Processed {total} snapshots | anomalies {anomalies} | mean_score {mean_score:.4f}"
    )


@cli.command("tune-iforest")
@click.option("--processed-root", type=click.Path(exists=True, path_type=Path), required=True)
@click.option(
    "--config",
    "grid_yaml",
    type=click.Path(path_type=Path),
    default=Path("configs/iforest_grid.yaml"),
    show_default=True,
)
@click.option("--start", "start_time", required=True, type=str)
@click.option("--end", "end_time", required=True, type=str)
def tune_iforest_cmd(
    processed_root: Path,
    grid_yaml: Path,
    start_time: str,
    end_time: str,
) -> None:
    """Grid search hyper-parameters for StreamingIForestDetector."""
    start_dt = _parse_cli_time(start_time)
    end_dt = _parse_cli_time(end_time)
    run_grid_search(
        processed_root,
        grid_yaml,
        start_dt,
        end_dt,
        Path("data/working_data/cache"),
        results_csv=Path("data/working_data/tuning_results.csv"),
        best_yaml=Path("iforest_best.yaml"),
    )
