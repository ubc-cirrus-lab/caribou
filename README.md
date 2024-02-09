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

###   Deployment Server

The deployment server can be found in `multi_x_serverless/deployment/server` and can be setup with:

```bash
poetry run ./multi_x_serverless/deployment/server/setup.sh
```

###  Deployment Client

The deployment client can be found in `multi_x_serverless/deployment/client` and can be run with:

```bash
poetry run multi_x_serverless <args>
```

To see the available commands, use:

```bash
poetry run multi_x_serverless --help
```

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

* Single process (runs the benchmark scenarios one after the other):
```bash
poetry run python benchmarks/solver_benchmarks/run_solver_benchmarks.py
```

* Multiprocess (uses all CPU cores to run the benchmark scenarios in parallel):
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
