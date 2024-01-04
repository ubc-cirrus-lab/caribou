# Multi-X-Serverless

## Quick Start

We are working with poetry, so you need to install it first.

```bash
pip install poetry
```

Then, install the dependencies:

```bash
poetry install
```

And run any file with:

```bash
poetry run <executable> <args>
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

### Other Serverless Functions

To deploy the serverless AWS function, use chalice (comes with the dependencies):

```bash
poetry run chalice deploy --stage <stage>
```

To run the serverless function locally, use (don't forget to disable the serverless deployment, meaning comment out for example `@app.schedule`):

```bash
poetry run python app.py
```

## Architecture

For the architecture, see the current [draw.io](https://app.diagrams.net/#G1rql5LiXzNiWIzN1-zJmqYMQYyUwjmrOq) diagram.

## Development

## About

##  License

TODO: We are inspired by the chalice project so we will probably use the same license.
