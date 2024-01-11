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

# Run pytest
echo "Running pytest..."
poetry run pytest --cache-clear --cov=. --cov-report=term-missing --cov-fail-under=90 > pytest-coverage.txt
