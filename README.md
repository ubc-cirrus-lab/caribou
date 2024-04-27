# Caribou

Caribou is a framework for running and deploying complex workflows multi-constraint and multi-objective aware on AWS over multiple regions.
The framework is designed to be self-adapting and self-optimizing with regards to the carbon footprint, cost, and performance of the workflows.
A workflow that is developed and deployed using Caribou can be run and solved by the framework, which will then automatically, depending on constraints and objectives, adapt the workflow deployment to optimize the workflow's performance, cost, and carbon footprint.
Optimization is done, when warranted by the type of application and frequency of its invocation, by using a suitable deployment algorithm that solves for new multi-region deployment configurations.
The overhead of the system plays a crucial role in the optimization process and sets the frequency as well as the granularity of the optimization process.
For more information, see the [design documentation](docs/design.md).

## Quick Start

The following instructions will guide you through the process of setting up the project and running a workflow.

### Prerequisites

We are working with poetry for our dependency management, so you need to install it first.

```bash
pip install poetry
```

#### Note for Linux Users

If this doesn't work for you and you are using a Linux machine, you will need to install poetry with:

```bash
apt install python3-poetry
```

Or you need to reinstall poetry:

```bash
curl -sSL https://install.python-poetry.org | python3 -
poetry self update
```

Then, install the dependencies:

```bash
poetry install
```

And run any file with:

```bash
poetry run <executable> <args>
```

### Why do we use poetry?

Poetry is a tool for dependency management and packaging in Python.
It allows you to declare the libraries your project depends on and it will manage (install/update) them for you.

The reason why we always have to use `poetry run` is because poetry creates a virtual environment for the project.
This means that all the dependencies are installed in a virtual environment and not in your global environment.
If you execute a file without `poetry run`, it will not find the dependencies as they are most likely not installed in your global environment.

For more information, see the [poetry documentation](https://python-poetry.org/docs/).

###  Setup AWS Environment

To setup the required tables and buckets in AWS, you can use the following command:

```bash
poetry run caribou setup_tables
```

### Setup a new workflow

To setup a new workflow, you need to be in a location where you want to create the new workflow and run:

```bash
poetry run caribou new_workflow <workflow_name>
```

Where `<workflow_name>` is the name of the new workflow.

### Deployment Utility

The deployment utility has an additional dependency on `docker`. To install it, follow the instructions on the [docker website](https://docs.docker.com/engine/install/).

The deployment utility can be found in `caribou/deployment/client` and can be run with (needs to be run in the same directory as the workflow you want to deploy):

```bash
poetry run caribou deploy
```

This will deploy the workflow to the correct defined home region. To change the home region you need to adjust the config in `.caribou/config.yml` and set the `home_region` to the desired region.

This will also print the unique workflow id.

### Run a workflow

To run a workflow, you can use the following command:

```bash
poetry run caribou run <workflow_id> -a '{"message": "Hello World!"}'
```

Where `<workflow_id>` is the id of the workflow you want to run.

The `-a` flag is used to pass arguments to the workflow. The arguments can be passed as any object but need to be handled in the entrypoint of the workflow by your client code.

###  List all workflows

To list all workflows, you can use the following command:

```bash
poetry run caribou list
```

###  Remove a workflow

To remove a workflow, you can use the following command:

```bash
poetry run caribou remove <workflow_id>
```

Where `<workflow_id>` is the id of the workflow you want to remove.

### Synchronize Execution Logs

To sync the logs from all the workflows to the global table, you can use the following command:

```bash
poetry run caribou log_sync
```

### Data Collecting

The data collecting can be found in `caribou/data_collector` and can be run individually with:

```bash
poetry run caribou data_collect <collector>
```

Where `<collector>` is the name of the collector you want to run. The available collectors are:

- `carbon`
- `provider`
- `performance`
- `workflow`
- `all`

The `all` and `workflow` collectors need a workflow id to be passed as an argument with `--workflow_id` or `-w`.

This manual data collecting is only necessary if you want to collect data for a specific workflow for testing purposes. The first three collectors are automatically run periodically.

The workflow collector is invoked by the manager and is used to collect data for the workflows that are currently being solved.

Important: For the data collectors to work locally, you need to set some environment variables.

```bash
export ELECTRICITY_MAPS_AUTH_TOKEN=<your_token>
export GOOGLE_API_KEY=<your_key>
```

### Find new (optimal) Deployment

Use the manager to solve for all workflows that have a check outstanding:

```bash
poetry run caribou manage_deployments
```

### Run Deployment Migrator

Since we are restricted by the AWS lambda environment to not use docker, we have to use [crane](https://github.com/google/go-containerregistry/tree/main/cmd/crane) to deploy the workflows. For the following step to work please install crane as described in the [crane documentation](https://github.com/google/go-containerregistry/blob/main/cmd/crane/README.md).

Once a new deployment has been found for a workflow, you can use the following command to deploy the new workflow:

```bash
poetry run caribou run_deployment_migrator
```

Which will check if a new deployment is required for any workflow and if so, migrate the functions according to this new deployment.

### Testing

### Compliance

To check for compliance with the code style as well as run the unit tests, you can use the following command:

```bash
poetry run ./scripts/compliance.sh
```

#### Unit Tests

To run the unit tests, use:

```bash
poetry run pytest
```

#### Integration Tests

To run the integration tests, use:

```bash
poetry run python integration_tests/run_integration_tests.py
```

### Benchmarking

Currently we have the following workflows for benchmarking:

- `benchmarks/dna_visualization`
- `benchmarks/image_processing`
- `benchmarks/map_reduce`
- `benchmarks/small_sync_example`
- `benchmarks/text_2_speech_censoring`
- `benchmarks/video_analytics`

Information on how to run each of these are in the respective workflow's README.

## Architecture

The framework architecture can be found  [here](docs/img/system_components.pdf).

##  Documentation

The documentation can be found in `docs/` and contains the following:

- [Design](docs/design.md)

## Development

## About

## License

Apache License 2.0
