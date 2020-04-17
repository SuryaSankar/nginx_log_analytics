"""Console script for nginx_log_analytics."""
import sys
import click


@click.command()
def main(args=None):
    """Console script for nginx_log_analytics."""
    click.echo("Replace this message by putting your code into "
               "nginx_log_analytics.cli.main")
    click.echo("See click documentation at https://click.palletsprojects.com/")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
