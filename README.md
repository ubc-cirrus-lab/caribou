# Multi-X-Serverless

## Quick Start

We are working with poetry, so you need to install it first.

```bash
pip install poetry
```

Alternatively if this doesn't work you will need to install poetry on Linux with:

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
poetry run multi_x_serverless setup_tables
```

### Setup a new workflow

To setup a new workflow, you need to be in a location where you want to create the new workflow and run:

```bash
poetry run multi_x_serverless new_workflow <workflow_name>
```

Where `<workflow_name>` is the name of the new workflow.

### Deployment Client

The deployment client has an additional dependency on `docker`. To install it, use:

```bash
sudo apt-get install docker.io
```

The deployment client can be found in `multi_x_serverless/deployment/client` and can be run with (needs to be run in the same directory as the workflow you want to deploy):

```bash
poetry run multi_x_serverless deploy
```

This will deploy the workflow to the correct defined home region. To change the home region you need to adjust the config in `.multi-x-serverless/config.yml` and set the `home_region` to the desired region.

This will also print the unique workflow id.

### Run a workflow

To run a workflow, you can use the following command:

```bash
poetry run multi_x_serverless run <workflow_id> -a '{"message": "Hello World!"}'
```

Where `<workflow_id>` is the id of the workflow you want to run.

The `-a` flag is used to pass arguments to the workflow. The arguments can be passed as any object but need to be handled in the entrypoint of the workflow by your client code.

###  List all workflows

To list all workflows, you can use the following command:

```bash
poetry run multi_x_serverless list
```

###  Remove a workflow

To remove a workflow, you can use the following command:

```bash
poetry run multi_x_serverless remove <workflow_id>
```

Where `<workflow_id>` is the id of the workflow you want to remove.

### Datasync

To sync the logs from all the workflows to the global table, you can use the following command:

```bash
poetry run multi_x_serverless data_sync
```

### Data Collecting

The data collecting can be found in `multi_x_serverless/data_collector` and can be run individually with:

```bash
poetry run multi_x_serverless data_collect <collector>
```

Where `<collector>` is the name of the collector you want to run. The available collectors are:

- `carbon`
- `provider`
- `performance`
- `workflow`
- `all`

Important: For the data collectors to work locally, you need to set some environment variables.

```bash
export ELECTRICITY_MAPS_AUTH_TOKEN=<your_token>
export GOOGLE_API_KEY=<your_key>
```

### Solve

To solve for a workflow you can either do it manually by using the following command:

```bash
poetry run multi_x_serverless solve <workflow_id> -s <solver>
```

Where `<workflow_id>` is the id of the workflow you want to run. And the `-s` flag is used to denote running a specific solver.
Where `<solver>` is the name of the solver you want to run.
The available solver are:

- `fine-grained`
- `coarse-grained`
- `heuristic`

Or use the update checker to solve for all workflows that have been invoked enough (100 times in last month):

```bash
poetry run multi_x_serverless update_check_solver
```

### Re-Deploy

Once a workflow has been solved and a new deployment is required, you can use the following command:

```bash
poetry run multi_x_serverless update_check_deployment
```

Which will check if a new deployment is required for any workflow and if so, deploy the new workflow.

### Testing

#### Unit Tests

To run the unit tests, use:

```bash
poetry run pytest
```

#### Integration Tests

TODO (#116): Add integration tests

### Benchmarking

Currently we have the following system part benchmarks:

- Solver Benchmarks

#### Solver Benchmarks

To run the solver benchmarks, use either the following commands:

- Single process (runs the benchmark scenarios one after the other):

```bash
poetry run python benchmarks/solver_benchmarks/run_solver_benchmarks.py
```

- Multiprocess (uses all CPU cores to run the benchmark scenarios in parallel):

```bash
poetry run python benchmarks/solver_benchmarks/run_solver_benchmarks_multiprocess.py
```

This will generate a JSON in `benchmarks/solver_benchmarks/results/results.json` with the results.

To plot the results, use:

```bash
poetry run python plotting/solver_benchmark_plotting.py benchmarks/solver_benchmarks/results/results.json
```

This will generate a plot in `plotting/plots/`.

## Architecture

For the architecture, see the current [draw.io](https://app.diagrams.net/#G1rql5LiXzNiWIzN1-zJmqYMQYyUwjmrOq) diagram.

##  Documentation

The documentation can be found in `docs/` and contains the following:

- [Design](docs/design.md)

## Development

## About

##  License

Apache License 2.0
