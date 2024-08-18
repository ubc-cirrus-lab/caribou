#  Installation

The following instructions will guide you through setting up the project and running a workflow.

## Prerequisites

We are working with [poetry](https://python-poetry.org) for our dependency management, so you must install it first to run any part of our Python package.

```bash
pip install poetry
```

**Note:** for Linux Users

If this does not work for you and you are using a Linux machine, you will need to install poetry with:

```bash
apt install python3-poetry
```

Alternatively, you need to reinstall poetry:

```bash
curl -sSL https://install.python-poetry.org | python3 -
poetry self-update
```

Then, install the dependencies (run this in the root directory of the project) with:

```bash
poetry install
```

Run any file with the following command:

```bash
poetry run <executable> <args>
```

## Why do we use poetry?

Poetry is a tool for dependency management and packaging in Python.
It allows you to declare the libraries your project depends on, and it will manage (install/update) them for you.

You can either always use `poetry run` before running a file or activate the virtual environment created by poetry.
We always have to use `poetry run` because poetry creates a virtual environment for the project.
You can alternatively find the virtual environment path using `poetry env info` and activate it with `source <path>/bin/activate`, removing the need for `poetry run`.
This means all the dependencies are installed in a virtual environment, not your global one.
If you execute a file without `poetry run`, it will not find the dependencies as they are most likely not installed in your global environment.

For more information, see the [poetry documentation](https://python-poetry.org/docs/).

## AWS Account Access

To run the framework, you need an AWS account and the necessary permissions to create and manage the required resources.
In [IAM Policies](docs/iam_policies.md) we list the required permissions for any user wanting to interact with a deployed framework.

The fastest way to set up the necessary permissions is to [create a new AWS user](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html) under your account with the necessary permissions and use the access key and secret key to [login the AWS CLI](https://docs.aws.amazon.com/signin/latest/userguide/command-line-sign-in.html) of this user to interact with the framework.

## Setup AWS Environment

To set up the required tables in AWS required for the framework to run, you can use the following command:

```bash
caribou setup_tables
```

**Note:** The bucket that Caribou uses to store the resources (a feature for future provider compatibility) needs to be manually created.
Since AWS bucket names need to be unique, the currently configured bucket might already exist and be used by another version of the framework deployed somewhere else.
In this case, adapt the bucket name for the variable `DEPLOYMENT_RESOURCES_BUCKET` in the `caribou/common/constants.py` file.

## Other dependencies

Since the AWS lambda environment restricts us from using Docker, we have to deploy the workflows using [crane](https://github.com/google/go-containerregistry/tree/main/cmd/crane). For the following step to work, please install the crane as described in the [crane documentation](https://github.com/google/go-containerregistry/blob/main/cmd/crane/README.md).
