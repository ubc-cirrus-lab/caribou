import urllib

from archived.s3 import list_bucket_objects
from archived.s3 import delete_objects


def clear_bucket(bucket_name):
    objects = list_bucket_objects(bucket_name)
    if objects is not None:
        file_names = []
        for obj in objects:
            file_key = urllib.parse.unquote_plus(obj["Key"], encoding='utf-8')
            file_names.append(file_key)
        if len(file_names) >= 1:
            #print("delete files {} in bucket {}".format(file_names, bucket_name))
            delete_objects(bucket_name, file_names)
    return True
