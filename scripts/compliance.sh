#!/bin/bash

set -e

# Run black
echo "Running black..."
poetry run black --version
poetry run black caribou --verbose

# Run isort
echo "Running isort..."
poetry run isort --version
poetry run isort caribou

# Run pylint
echo "Running pylint..."
poetry run pylint --version
poetry run pylint caribou

# Run mypy
echo "Running mypy..."
poetry run mypy --version
poetry run mypy caribou

# Run pytest
echo "Running pytest..."
poetry run pytest --junitxml=pytest.xml --cov-report=term-missing:skip-covered --cov-fail-under=88 --cov=caribou caribou/tests
