import urllib
import numpy as np
import torch

from archived.s3 import put_object
from archived.sync import merge_np_bytes

# lambda setting
tmp_bucket = "tmp-grads"


def handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    print('bucket = {}'.format(bucket))
    print('key = {}'.format(key))

    num_files = 5

    for i in np.arange(num_files):
        w = np.random.rand(2, 3).astype(np.float32)
        print("the {}-th numpy array".format(i))
        print(w)
        put_object(tmp_bucket, "weight_" + str(i), w.tobytes())

    arr = merge_np_bytes(tmp_bucket, num_files, np.float32, [2, 3])
    t = torch.from_numpy(arr)
    print(t)
