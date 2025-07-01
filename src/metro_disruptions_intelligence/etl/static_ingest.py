"""Utilities to ingest static GTFS files into Parquet tables."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb
from pydantic import BaseModel


def ingest_static_gtfs(gtfs_dir: Path, output_dir: Path, persist_duckdb: bool = False) -> Path:
    """Read GTFS CSVs from ``gtfs_dir`` and write ``station_schedule.parquet``.

    Parameters
    ----------
    gtfs_dir:
        Directory containing the GTFS text files.
    output_dir:
        Folder where ``station_schedule.parquet`` will be created.
    persist_duckdb:
        When ``True`` an on-disk DuckDB database is created alongside the
        Parquet output. Otherwise an in-memory database is used.

    Returns
    -------
    Path
        Location of the written Parquet file.
    """

    db_path = output_dir / "station_schedule.duckdb" if persist_duckdb else ":memory:"
    output_parquet = output_dir / "station_schedule.parquet"
    output_dir.mkdir(parents=True, exist_ok=True)

    logging.info("Creating duckdb database at %s", db_path)
    con = duckdb.connect(str(db_path))

    stop_times_csv = gtfs_dir / "stop_times.txt"
    trips_csv = gtfs_dir / "trips.txt"

    # First read the entire CSV so the stop_id column is interpreted as VARCHAR
    # even when the sample contains only numeric values. SAMPLE_SIZE=-1 forces
    # DuckDB to scan the whole file.
    con.execute(
        f"""
        CREATE TABLE stop_times_raw AS
            SELECT *
            FROM read_csv_auto(
                '{stop_times_csv}',
                delim=',',
                HEADER=TRUE,
                SAMPLE_SIZE=-1
            );
        """
    )

    # Transform the raw table to add numeric arrival/departure seconds.
    con.execute(
        """
        CREATE TABLE stop_times AS
            SELECT
                trip_id,
                stop_id,
                arrival_time,
                departure_time,
                stop_sequence,
                CAST(split_part(arrival_time, ':', 1) AS INT) * 3600 +
                CAST(split_part(arrival_time, ':', 2) AS INT) * 60 +
                CAST(split_part(arrival_time, ':', 3) AS INT) AS sched_arr,
                CAST(split_part(departure_time, ':', 1) AS INT) * 3600 +
                CAST(split_part(departure_time, ':', 2) AS INT) * 60 +
                CAST(split_part(departure_time, ':', 3) AS INT) AS sched_dep
            FROM stop_times_raw;
        """
    )
    con.execute(
        f"""
        CREATE TABLE trips AS
        SELECT trip_id, service_id, route_id
        FROM read_csv_auto('{trips_csv}', delim=',', HEADER=TRUE);
    """
    )
    con.execute(
        """
        CREATE TABLE station_schedule AS
        SELECT st.trip_id,
               t.service_id,
               t.route_id,
               st.stop_id,
               st.sched_arr,
               st.sched_dep,
               st.stop_sequence
        FROM stop_times st
        JOIN trips t USING (trip_id);
    """
    )
    logging.info("Writing station_schedule to %s", output_parquet)
    con.execute(f"COPY station_schedule TO '{output_parquet}' (FORMAT PARQUET);")
    con.close()
    return output_parquet


class StaticIngestConfig(BaseModel):
    """Configuration for :func:`ingest_static_gtfs`."""

    gtfs_dir: Path
    output_dir: Path = Path("data/processed/static")
    persist_duckdb: bool = False


def _parse_args(argv: list[str] | None = None) -> StaticIngestConfig:
    parser = argparse.ArgumentParser(description="Ingest static GTFS data")
    parser.add_argument("gtfs_dir", type=Path, help="Folder containing GTFS text files")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/processed/static"),
        help="Directory to store processed Parquet and optional DuckDB",
    )
    parser.add_argument(
        "--persist-duckdb",
        action="store_true",
        help="Persist the intermediate DuckDB database to disk",
    )
    ns = parser.parse_args(argv)
    return StaticIngestConfig(**vars(ns))


def main(argv: list[str] | None = None) -> None:
    """Command line entry-point."""
    cfg = _parse_args(argv)
    ingest_static_gtfs(cfg.gtfs_dir, cfg.output_dir, cfg.persist_duckdb)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    main()
