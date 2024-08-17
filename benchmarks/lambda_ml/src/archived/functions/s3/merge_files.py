import os
import urllib.parse
import boto3
import numpy as np

from archived.s3 import list_bucket_objects

print('Loading function')

s3 = boto3.client('s3')


def print_conf(event, context):
    # print("Received event: " + json.dumps(event, indent=2))
    print('## ENVIRONMENT VARIABLES')
    print(os.environ)
    print('## EVENT')
    print(event)
    print('## context')
    print(context)


def handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    print('bucket = {}'.format(bucket))
    print('key = {}'.format(key))

    # Set up logging
    # logging.basicConfig(level=logging.INFO, format='%(message)s')

    update_bucket = "tmp-updates"

    sum_w = np.zeros([2, 3])
    tmp_path = weight_path = '/tmp/'

    s_3 = boto3.client('s3')

    # Retrieve the bucket's objects
    objects = list_bucket_objects(update_bucket)
    if objects is not None:
        # List the object names
        print('Objects in {}'.format(update_bucket))
        for obj in objects:
            file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
            print('file:  {}'.format(file_key))
            s_3.download_file(update_bucket, file_key, tmp_path + str(file_key))
            w = np.loadtxt(tmp_path + str(file_key))
            sum_w = sum_w + w
        print(sum_w)
    else:
        # Didn't get any keys
        print('No objects in {}'.format(update_bucket))
