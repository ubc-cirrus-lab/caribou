import sys
import traceback

import botocore.exceptions
import click

from multi_x_serverless.deployment.client.cli.cli import cli


def main() -> int:
    try:
        return cli(obj={})  # pylint: disable=no-value-for-parameter
    except botocore.exceptions.NoRegionError:
        click.echo("No region specified. Please specify a region in your AWS config file.", err=True)
        return 2
    except Exception:  # pylint: disable=broad-except
        click.echo(traceback.format_exc(), err=True)
        return 2


if __name__ == "__main__":
    sys.exit(main())
