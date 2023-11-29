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
poetry run <executable> <file>
```

### Serverless

To deploy the serverless AWS function, use chalice (comes with the dependencies):

```bash
poetry run chalice deploy --stage <stage>
```

To run the serverless function locally, use (don't forget to disable the serverless deployment, meaning comment out for example `@app.schedule`):

```bash
poetry run python app.py
```

## Architecture

## Development

## About

##  License
