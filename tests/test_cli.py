"""Tests for `metro_disruptions_intelligence` CLI."""

from click.testing import CliRunner

from metro_disruptions_intelligence import cli


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.cli)
    assert result.exit_code == 0
    assert "metro_disruptions_intelligence.cli.cli" in result.output
    help_result = runner.invoke(cli.cli, ["--help"])
    assert help_result.exit_code == 0
    assert (
        "Console script for metro_disruptions_intelligence.\n\nOptions:\n  "
        "--version  Show the version and exit.\n  "
        "--help     Show this message and exit.\n" in help_result.output
    )
