from .wrapper import MultiXServerlessWorkflow
import botocore.exceptions
from multi_x_serverless.deployment.client.cli.cli import cli

import click
import traceback

def main() -> int:
    try:
        return cli(obj={})
    except botocore.exceptions.NoRegionError:
        click.echo("No region specified. Please specify a region in your AWS config file.", err=True)
        return 2
    except Exception:
        click.echo(traceback.format_exc(), err=True)
        return 2
