#  Testing

Our testing consists of compliance checks, unit tests, and integration tests. Note that all tests should be run in the root git repository.

##  Compliance

To check for compliance with the code style as well as run the unit tests, you can use the following command:

```bash
poetry run ./scripts/compliance.sh
```

## Unit Tests

To run the unit tests, use:

```bash
poetry run pytest
```

## Integration Tests

To run the integration tests, use:

```bash
poetry run python integration_tests/run_integration_tests.py
```
