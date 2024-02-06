#  Integration tests

The integrationtests work by running the `run_integration_tests.py` script. This script will run the `integration_tests` container and execute the tests inside it.

There are three scenarios that are tested:

- **Deploying a workflow**: This scenario tests the deployment of a workflow using the `deploy` command.
- **Running a workflow**: This scenario tests the execution of a workflow using the provided endpoint.
- **Solving and re-deploying a workflow**: This scenario tests the solving of a workflow from a trigger at the `SolverUpdateChecker` and then re-deploying the workflow using the trigger at the `DeploymentUpdateChecker`.

## Remote Client

The integration tests are using the `IntegrationTestRemoteClient` class to prevent any side effects on a remote location or from any real provider. The `IntegrationTestRemoteClient` keeps a local stateful representation of the remote location and provider, and it is used to assert the expected behavior of the integration tests.

## Locations

The integrationtests have a smaller subset of artificial locations that are used to test the different scenarios. These locations are _Rivendell_, _Lothlorien_, _Anduin_, and _Fangorn_.
