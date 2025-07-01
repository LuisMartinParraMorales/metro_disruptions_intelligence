"""Tests for :mod:`metro_disruptions_intelligence.cli`."""

from click.testing import CliRunner

from metro_disruptions_intelligence import cli


def test_cli_group_help():
    runner = CliRunner()
    result = runner.invoke(cli.cli, ["--help"])
    assert result.exit_code == 0
    assert "ingest-static" in result.output
    assert "ingest-rt" in result.output


def test_cli_ingest_commands(tmp_path):
    runner = CliRunner()

    result = runner.invoke(
        cli.cli, ["ingest-static", "sample_data/static", "--output-dir", str(tmp_path / "static")]
    )
    assert result.exit_code == 0
    assert (tmp_path / "static" / "station_schedule.parquet").exists()

    result = runner.invoke(
        cli.cli,
        ["ingest-rt", "sample_data/rt", "--processed-root", str(tmp_path / "rt"), "--union"],
    )
    assert result.exit_code == 0
    assert (tmp_path / "station_event.parquet").exists()
