from typing import Any

import json
import boto3
from sklearn import datasets
import tempfile
import os
import datetime
import tensorflow as tf
from tensorflow.keras import layers
import tarfile
from random import choice

from multi_x_serverless.deployment.client import MultiXServerlessWorkflow

workflow = MultiXServerlessWorkflow(name="regression_tuning", version="0.0.1")


@workflow.serverless_function(
    name="GetInput",
    entry_point=True,
)
def get_input(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "message" in event:
        samplesNum = event["message"]
    else:
        raise ValueError("No samples number provided")

    payload = {
        "samplesNum": samplesNum,
    }

    workflow.invoke_serverless_function(create_dataset, payload)

    return {"status": 200}


@workflow.serverless_function(name="CreateDataset")
def create_dataset(event: dict[str, Any]) -> dict[str, Any]:
    samplesNum = event["samplesNum"]

    payload = create_artificial_dataset(samplesNum)

    workflow.invoke_serverless_function(first_model, payload)

    workflow.invoke_serverless_function(second_model, payload)

    return {"status": 200}


def create_artificial_dataset(n_samples):
    s3 = boto3.client("s3")
    x, y = datasets.make_regression(
        n_samples=n_samples, n_features=1, n_informative=1, n_targets=1, bias=3.0, noise=1.0
    )
    # just make sure data is in the right format, i.e. one feature
    assert x.shape[1] == 1

    tmp_dir = tempfile.mkdtemp()

    path = os.path.join(tmp_dir, "dataset.txt")

    with open(path, "w") as _file:
        for _ in range(len(x)):
            _file.write("{}\t{}\n".format(x[_][0], y[_]))

    storage_path = datetime.now().strftime("%Y%m%d-%H%M%S") + "regression_tuning.txt"
    s3.upload_file(path, "multi-x-serverless-regression-tuning", f"regression_tuning/{storage_path}")

    payload = {
        "storage_path": storage_path,
    }

    return payload


@workflow.serverless_function(name="FirstModel")
def first_model(event: dict[str, Any]) -> dict[str, Any]:
    storage_path = event["storage_path"]

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file("multi-x-serverless-regression-tuning", f"regression_tuning/{storage_path}", os.path.join(tmp_dir, "dataset.txt"))

    with open(os.path.join(tmp_dir, "dataset.txt"), "r") as _file:
        raw_data = _file.readlines()

    dataset = [[float(_) for _ in d.strip().split("\t")] for d in raw_data]

    split_index = int(len(dataset) * 0.8)
    train_dataset = dataset[:split_index]
    test_dataset = dataset[split_index:]
    learning_rate = 0.1

    x_train = [[_[0]] for _ in train_dataset]
    y_train = [_[1] for _ in train_dataset]
    x_test = [[_[0]] for _ in test_dataset]
    y_test = [_[1] for _ in test_dataset]
    x_model = tf.keras.Sequential(
        [
            layers.Dense(
                input_shape=[
                    1,
                ],
                units=1,
            )
        ]
    )
    x_model.compile(optimizer=tf.optimizers.Adam(learning_rate=learning_rate), loss="mean_absolute_error")

    history = x_model.fit(x_train, y_train, epochs=100, validation_split=0.2)
    hist = history.history

    results = x_model.evaluate(x_test, y_test)

    model_name = datetime.now().strftime("%Y%m%d-%H%M%S") + "regression_tuning_1"

    local_tar_name = model_name + ".tar.gz"

    local_path = os.path.join(tmp_dir, model_name)

    x_model.save(local_path)

    with tarfile.open(local_tar_name, mode="w:gz") as _tar:
        _tar.add(model_name, recursive=True)

    s3.upload_file(local_tar_name, "multi-x-serverless-regression-tuning", f"regression_tuning/{local_tar_name}")

    payload = {
        "model_name": local_tar_name,
        "results": results,
    }

    workflow.invoke_serverless_function(sync_function, payload)

    return {"status": 200}


@workflow.serverless_function(name="SecondModel")
def second_model(event: dict[str, Any]) -> dict[str, Any]:
    storage_path = event["storage_path"]

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file("multi-x-serverless-regression-tuning", f"regression_tuning/{storage_path}", os.path.join(tmp_dir, "dataset.txt"))

    with open(os.path.join(tmp_dir, "dataset.txt"), "r") as _file:
        raw_data = _file.readlines()

    dataset = [[float(_) for _ in d.strip().split("\t")] for d in raw_data]

    split_index = int(len(dataset) * 0.8)
    train_dataset = dataset[:split_index]
    test_dataset = dataset[split_index:]
    learning_rate = 0.2

    x_train = [[_[0]] for _ in train_dataset]
    y_train = [_[1] for _ in train_dataset]
    x_test = [[_[0]] for _ in test_dataset]
    y_test = [_[1] for _ in test_dataset]
    x_model = tf.keras.Sequential(
        [
            layers.Dense(
                input_shape=[
                    1,
                ],
                units=1,
            )
        ]
    )
    x_model.compile(optimizer=tf.optimizers.Adam(learning_rate=learning_rate), loss="mean_absolute_error")

    history = x_model.fit(x_train, y_train,epochs=100, validation_split=0.2)
    hist = history.history

    results = x_model.evaluate(x_test, y_test)

    model_name = datetime.now().strftime("%Y%m%d-%H%M%S") + "regression_tuning_2"

    local_tar_name = model_name + ".tar.gz"

    local_path = os.path.join(tmp_dir, model_name)

    x_model.save(local_path)

    with tarfile.open(local_tar_name, mode="w:gz") as _tar:
        _tar.add(model_name, recursive=True)

    s3.upload_file(local_tar_name, "multi-x-serverless-regression-tuning", f"regression_tuning/{local_tar_name}")

    payload = {
        "model_name": local_tar_name,
        "results": results,
    }

    workflow.invoke_serverless_function(sync_function, payload)

    return {"status": 200}


@workflow.serverless_function(name="MergeFunction")
def sync_function(event: dict[str, Any]) -> dict[str, Any]:
    results = workflow.get_predecessor_data(event)

    model_names = []
    results = []

    for result in results:
        if "model_name" in result:
            model_names.append(result["model_name"])

        if "results" in result:
            results.append(result["results"])

    if len(model_names) != 2 or len(results) != 2:
        raise ValueError("An invalid number of models or results were provided")

    payload = {
        "model_names": model_names,
        "results": results,
    }

    workflow.invoke_serverless_function(join_runs, payload)

    return {"status": 200}


@workflow.serverless_function(name="JoinRuns")
def join_runs(event: dict[str, Any]) -> dict[str, Any]:
    model_names = event["model_names"]
    results = event["results"]

    best_model = choice(model_names)

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file("multi-x-serverless-regression-tuning", f"regression_tuning/{best_model}", os.path.join(tmp_dir, best_model))

    with tarfile.open(os.path.join(tmp_dir, best_model), "r:gz") as zipped_file:
        zipped_file.extractall(tmp_dir)

    model = tf.keras.models.load_model(os.path.join(tmp_dir, best_model.replace(".tar.gz", "")))

    input_model = [[0.57457947234]]

    prediction = model.predict(input_model)

    assert prediction[0][0] > 0

    return {"status": 200}
