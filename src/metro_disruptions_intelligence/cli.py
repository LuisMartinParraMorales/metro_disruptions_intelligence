"""Console script for metro_disruptions_intelligence."""

import click


@click.version_option(package_name="metro_disruptions_intelligence")
@click.command()
def cli(args=None):
    """Console script for metro_disruptions_intelligence."""
    click.echo(
        "Replace this message by putting your code into metro_disruptions_intelligence.cli.cli"
    )
    click.echo("See click documentation at https://click.palletsprojects.com/")
    return 0
