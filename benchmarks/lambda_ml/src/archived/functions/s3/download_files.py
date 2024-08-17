import time
import os
import json
import urllib.parse
import boto3
import logging
import numpy as np


def handler(event, context):
    # Get the object from the event and show its content type
    bucket = ""
    key = ""

    print('bucket = {}'.format(bucket))
    print('key = {}'.format(key))

    tmp_path = "/tmp/"

    s_3 = boto3.client('s3')
    download_start = time.time()
    s_3.download_file(bucket, key, tmp_path + str(key))
    download_end = time.time()
    print("download file cost {} s".format(download_end - download_start))

