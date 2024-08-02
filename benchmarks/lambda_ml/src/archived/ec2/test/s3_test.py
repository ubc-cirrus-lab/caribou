import time
import boto3


def main():
    bucket = "s3-10"
    key = "0_10"
    tmp_path = "/home/ubuntu/envs/"
    download_start = time.time()
    s_3 = boto3.client('s3')
    s_3.download_file(bucket, key, tmp_path + str(key))
    download_end = time.time()
    print("download file cost {} s".format(download_end - download_start))


if __name__ == '__main__':
    main()
