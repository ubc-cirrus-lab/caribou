import time
import numpy as np

import boto3
from archived.s3 import put_object
from archived.s3 import clear_bucket


# lambda setting
local_dir = "/tmp/"
feature_file_name = "features_vgg19_train.npy"
label_file_name = "labels_vgg19_train.npy"

n_files = 10


def handler(event, context):
    start_time = time.time()
    bucket = event['bucket_name']
    worker_index = event['rank']
    num_workers = event['num_workers']
    key = event['file']
    tmp_bucket = event['tmp_bucket']
    merged_bucket = event['merged_bucket']
    num_epochs = event['num_epochs']
    learning_rate = event['learning_rate']
    batch_size = event['batch_size']

    print('bucket = {}'.format(bucket))
    print("file = {}".format(key))
    print('tmp bucket = {}'.format(tmp_bucket))
    print('merged bucket = {}'.format(merged_bucket))
    print('number of workers = {}'.format(num_workers))
    print('worker index = {}'.format(worker_index))
    print('num epochs = {}'.format(num_epochs))
    print('learning rate = {}'.format(learning_rate))
    print("batch size = {}".format(batch_size))

    s3 = boto3.client('s3')

    # read file from s3
    s3.download_file(bucket, feature_file_name, local_dir + str(feature_file_name))
    features_matrix = np.load(local_dir + str(feature_file_name))
    print("read features matrix cost {} s".format(time.time() - start_time))
    print("feature matrix shape = {}, dtype = {}".format(features_matrix.shape, features_matrix.dtype))
    print("feature matrix sample = {}".format(features_matrix[0]))
    row_features = features_matrix.shape[0]
    col_features = features_matrix.shape[1]

    s3.download_file(bucket, label_file_name, local_dir + str(label_file_name))
    labels_matrix = np.load(local_dir + str(label_file_name))
    print("read label matrix cost {} s".format(time.time() - start_time))
    print("label matrix shape = {}, dtype = {}".format(labels_matrix.shape, labels_matrix.dtype))
    print("label matrix sample = {}".format(labels_matrix[0:10]))
    row_labels = labels_matrix.shape[0]

    if row_features != row_labels:
        raise AssertionError("row of feature matrix is {}, but row of label matrix is {}."
                             .format(row_features, row_labels))

    features_matrix = features_matrix.flatten()
    samples_per_file = row_features / n_files

    for i in range(n_files):
        start_row = i * samples_per_file
        end_row = (i + 1) * samples_per_file
        features_file_name = "features_{}_{}".format(i, n_files)
        labels_file_name = "labels_{}_{}".format(i, n_files)
        put_object(bucket, features_file_name, features_matrix[start_row*col_features : end_row*col_features].tobytes())
        put_object(bucket, labels_file_name, labels_matrix[start_row:end_row].tobytes())

    if worker_index == 0:
        clear_bucket(merged_bucket)
        clear_bucket(tmp_bucket)

    end_time = time.time()
    print("Elapsed time = {} s".format(end_time - start_time))


if __name__ == "__main__":
    start_time = time.time()

    dataset_path = "D:\\Downloads\\shift-dataset\\"

    # features_matrix = np.load(dataset_path + feature_file_name)
    # print("read features matrix cost {} s".format(time.time() - start_time))
    # print("feature matrix shape = {}, dtype = {}".format(features_matrix.shape, features_matrix.dtype))
    # print("feature matrix sample = {}".format(features_matrix[0]))
    # row_features = features_matrix.shape[0]
    # col_features = features_matrix.shape[1]
    #
    # labels_matrix = np.load(dataset_path + label_file_name)
    # print("read label matrix cost {} s".format(time.time() - start_time))
    # print("label matrix shape = {}, dtype = {}".format(labels_matrix.shape, labels_matrix.dtype))
    # print("label matrix sample = {}".format(labels_matrix[0:10]))
    # row_labels = labels_matrix.shape[0]
    #
    # if row_features != row_labels:
    #     raise AssertionError("row of feature matrix is {}, but row of label matrix is {}."
    #                          .format(row_features, row_labels))
    #
    # #features_matrix = features_matrix.flatten()
    # samples_per_file = int(row_features/n_files)
    #
    # for i in range(n_files):
    #     start_row = i * samples_per_file
    #     end_row = (i + 1) * samples_per_file
    #     features_file_name = "features_{}_{}".format(i, n_files)
    #     labels_file_name = "labels_{}_{}".format(i, n_files)
    #     np.save(dataset_path + features_file_name,
    #             features_matrix[start_row:end_row, :])
    #     np.save(dataset_path + labels_file_name,
    #             labels_matrix[start_row:end_row])

    features_matrix = np.load(dataset_path + "features_0_10.npy")
    print("feature matrix shape = {}, dtype = {}".format(features_matrix.shape, features_matrix.dtype))
    print("feature matrix sample = {}, shape = {}".format(features_matrix[0], features_matrix[0].shape))

    labels_matrix = np.load(dataset_path + "labels_0_10.npy")
    print("feature matrix shape = {}, dtype = {}".format(labels_matrix.shape, labels_matrix.dtype))
    print("feature matrix sample = {}".format(labels_matrix[0:10]))
