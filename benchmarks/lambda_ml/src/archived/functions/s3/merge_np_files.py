import time
import urllib
import boto3
import numpy as np

from archived.s3 import list_bucket_objects

# lambda setting
tmp_bucket = "tmp-updates"


def handler(event, context):
    startTs = time.time()
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    print('bucket = {}'.format(bucket))
    print('key = {}'.format(key))

    # Set up logging
    # logging.basicConfig(level=logging.INFO, format='%(message)s')

    sum_w = np.zeros([2, 3])
    tmp_path = '/tmp/'

    s_3 = boto3.client('s3')

    #Retrieve the bucket's objects
    objects = list_bucket_objects(tmp_bucket)
    if objects is not None:
        # List the object names
        print('Objects in {}'.format(tmp_bucket))
        for obj in objects:
            file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
            print('file:  {}'.format(file_key))
            s_3.download_file(tmp_bucket, file_key, tmp_path + str(file_key))
            w = np.loadtxt(tmp_path + str(file_key))
            sum_w = sum_w + w
        print(sum_w)
    else:
        # Didn't get any keys
        print('No objects in {}'.format(tmp_bucket))
