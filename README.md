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

## Architecture

For the architecture, see the current [draw.io](https://app.diagrams.net/#G1rql5LiXzNiWIzN1-zJmqYMQYyUwjmrOq) diagram.

## Development

## About

##  License

Apache License 2.0
