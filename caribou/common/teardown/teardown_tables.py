import os
from typing import Any

import boto3
import botocore

from caribou.common import constants
from caribou.common.models.endpoints import Endpoints


def remove_table(dynamodb: Any, table_name: str, verbose: bool = True) -> None:
    # Check if the table already exists (If not skip deletion)
    try:
        dynamodb.describe_table(TableName=table_name)

        # If the table exists, delete it
        dynamodb.delete_table(TableName=table_name)
    except dynamodb.exceptions.ResourceNotFoundException:
        if verbose:
            print(f"Table '{table_name}' does not exists (Or already removed)")


def remove_bucket(s3: Any, s3_resource: Any, bucket_name: str) -> None:
    # Check if the bucket already exists (If not skip deletion)
    try:
        s3.head_bucket(Bucket=bucket_name)

        # If the bucket exists, delete it
        ## We need to first empty the bucket before deleting it
        bucket = s3_resource.Bucket(bucket_name)
        bucket.objects.all().delete()

        ## Finally delete the bucket
        s3.delete_bucket(Bucket=bucket_name)

        print(f"Removed legacy bucket: {bucket_name}")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "404" and e.response["Error"]["Code"] != "403":
            # If the error is not 403 forbidden or 404 not found,
            # raise the exception and notify the user
            raise


def teardown_framework_tables() -> None:
    dynamodb = boto3.client("dynamodb", region_name=constants.GLOBAL_SYSTEM_REGION)

    # Get all attributes of the constants module
    for attr in dir(constants):
        # If the attribute name ends with '_TABLE', create a DynamoDB table
        if attr.endswith("_TABLE"):
            table_name = getattr(constants, attr)

            if table_name in [constants.SYNC_MESSAGES_TABLE, constants.SYNC_PREDECESSOR_COUNTER_TABLE]:
                # Skip the sync tables (They are removed in a separate function)
                continue

            print(f"Removing table: {table_name}")
            try:
                remove_table(dynamodb, table_name)
            except Exception as e:  # pylint: disable=broad-except
                print(f"Error remove table {table_name}: {e}")


def teardown_framework_buckets() -> None:
    # Only used for legacy buckets
    s3 = boto3.client("s3", region_name=constants.GLOBAL_SYSTEM_REGION)
    s3_resource = boto3.resource("s3", region_name=constants.GLOBAL_SYSTEM_REGION)

    # Get all attributes of the constants module
    for attr in dir(constants):
        # If the attribute name ends with '_BUCKET', create an S3 bucket
        if attr.endswith("_BUCKET"):
            # Allow for the bucket name to be overridden by an environment variable
            bucket_name = os.environ.get(f"CARIBOU_OVERRIDE_{attr}", getattr(constants, attr))

            try:
                remove_bucket(s3, s3_resource, bucket_name)
            except Exception as e:  # pylint: disable=broad-except
                print(f"Error remove bucket {bucket_name}: {e}")


def remove_sync_tables_all_regions() -> None:
    # First get all the regions
    all_available_regions: list[str] = []
    try:
        available_regions_data = (
            Endpoints().get_data_collector_client().get_all_values_from_table(constants.AVAILABLE_REGIONS_TABLE)
        )
        for region_key_raw in available_regions_data.keys():
            # Keys are in forms of 'aws:eu-south-1' (For AWS regions)
            if region_key_raw.startswith("aws:"):
                region_key_aws = region_key_raw.split(":")[1]
                all_available_regions.append(region_key_aws)
    except Exception as e:  # pylint: disable=broad-except
        print(f"Error getting available regions: {e}")

    sync_tables = [constants.SYNC_MESSAGES_TABLE, constants.SYNC_PREDECESSOR_COUNTER_TABLE]
    print(f"Removing sync tables in the following regions: {all_available_regions}")
    error_regions: set[str] = set()
    for region in all_available_regions:
        dynamodb = boto3.client("dynamodb", region_name=region)

        for table_name in sync_tables:
            try:
                remove_table(dynamodb, table_name, verbose=False)
            except botocore.exceptions.ClientError as e:
                # If not UnrecognizedClientException, log the error
                # As exception also appears if the user does not have a region enabled
                # Which means that there are no tables to remove anyways
                if e.response["Error"]["Code"] != "UnrecognizedClientException":
                    print(f"Error removing table {table_name}: {e}")
                    error_regions.add(region)
            except Exception as e:  # pylint: disable=broad-except
                print(f"Unexpected error removing table {table_name}: {e}")
                error_regions.add(region)
    if len(error_regions) > 0:
        print(f"Removed from all applicable listed regions except: {error_regions}")


def main() -> None:
    # Remove any and all sync tables in all regions
    remove_sync_tables_all_regions()

    # Remove the core framework tables
    teardown_framework_tables()

    # Remove framrework buckets
    ## This is targetting legacy buckets that are not used anymore
    ## Current iteration of the framework does not use any buckets
    teardown_framework_buckets()


if __name__ == "__main__":
    main()
