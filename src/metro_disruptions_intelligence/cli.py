"""Command line entry points for :mod:`metro_disruptions_intelligence`."""

from pathlib import Path

import click

from .etl.ingest_rt import ingest_all_rt, union_all_feeds
from .etl.static_ingest import ingest_static_gtfs


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
def ingest_rt_cmd(raw_root: Path, processed_root: Path, union: bool) -> None:
    """Ingest realtime JSON feeds into Parquet tables."""
    ingest_all_rt(raw_root, processed_root)
    if union:
        output_parquet = processed_root.parent / "station_event.parquet"
        union_all_feeds(processed_root, output_parquet)
