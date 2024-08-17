import argparse

import time

# ./disk_test.py --file /bigdata/dataset/s3-vertical/0_10


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', type=str, default="")
    args = parser.parse_args()
    print(args)

    read_start = time.time()
    f = open(args.file).read()
    read_end = time.time()
    print("read file cost {} s".format(read_end - read_start))


if __name__ == '__main__':
    main()
