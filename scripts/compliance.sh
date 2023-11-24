#!/bin/bash

set -e

# Run black
echo "Running black..."
poetry run black --version
poetry run black multi_x_serverless --verbose

# Run isort
echo "Running isort..."
poetry run isort --version
poetry run isort multi_x_serverless

# Run pylint
echo "Running pylint..."
poetry run pylint --version
poetry run pylint multi_x_serverless

# Run mypy
echo "Running mypy..."
poetry run mypy --version
poetry run mypy multi_x_serverless

# Run autopep8
echo "Running autopep8..."
poetry run autopep8 --version
poetry run autopep8 --in-place --aggressive --aggressive multi_x_serverless

# Run pytest
echo "Running pytest..."
poetry run pytest multi_x_serverless --cov-reset --cache-clear --cov-fail-under=90