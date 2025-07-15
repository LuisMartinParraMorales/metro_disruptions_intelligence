"""Tests for :mod:`metro_disruptions_intelligence.cli`."""

import logging
from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner

from metro_disruptions_intelligence import cli
from metro_disruptions_intelligence.processed_reader import compose_path
from metro_disruptions_intelligence.utils_gtfsrt import make_fake_tu


def test_cli_group_help():
    runner = CliRunner()
    result = runner.invoke(cli.cli, ["--help"])
    assert result.exit_code == 0
    assert "ingest-static" in result.output
    assert "ingest-rt" in result.output


def test_cli_ingest_commands(tmp_path):
    runner = CliRunner()

    gtfs_path = Path("data/static")
    if gtfs_path.exists():
        result = runner.invoke(
            cli.cli, ["ingest-static", "data/static", "--output-dir", str(tmp_path / "static")]
        )
        assert result.exit_code == 0
        assert (tmp_path / "static" / "station_schedule.parquet").exists()
    else:
        pytest.skip("Static GTFS data not available")

    result = runner.invoke(
        cli.cli,
        ["ingest-rt", "sample_data/rt", "--processed-root", str(tmp_path / "rt"), "--union"],
    )
    assert result.exit_code == 0
    assert (tmp_path / "station_event.parquet").exists()


def test_generate_features_warn_empty_vehicle_positions(tmp_path, caplog):
    runner = CliRunner()

    ts = 1_000
    processed_root = tmp_path / "rt"
    tu_file = compose_path(ts, processed_root, "trip_updates")
    vp_file = compose_path(ts, processed_root, "vehicle_positions")
    tu_file.parent.mkdir(parents=True, exist_ok=True)
    vp_file.parent.mkdir(parents=True, exist_ok=True)

    make_fake_tu(ts, ts + 60).to_parquet(tu_file, index=False)
    pd.DataFrame(columns=["snapshot_timestamp", "stop_id", "direction_id"]).to_parquet(
        vp_file, index=False
    )

    output_root = tmp_path / "out"
    with caplog.at_level(logging.WARNING):
        result = runner.invoke(
            cli.cli, ["generate-features", str(processed_root), "--output-root", str(output_root)]
        )
    assert result.exit_code == 0
    assert f"vehicle_positions file {vp_file} contains no rows" in caplog.text
    assert list(output_root.rglob("stations_feats_*.parquet"))
