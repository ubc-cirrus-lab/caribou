import traceback
import click

import botocore.exceptions
from multi_x_serverless.deployment.client.cli.cli import cli
from multi_x_serverless.deployment.client.workflow import MultiXServerlessWorkflow


def main() -> int:
    try:
        return cli(obj={})  # pylint: disable=no-value-for-parameter
    except botocore.exceptions.NoRegionError:
        click.echo("No region specified. Please specify a region in your AWS config file.", err=True)
        return 2
    except Exception:  # pylint: disable=broad-except
        click.echo(traceback.format_exc(), err=True)
        return 2
