#  Architecture

The stylized architecture of the project can be seen in [this diagram](docs/img/system_components.pdf).

Specific architectural components are described in the following sections.

##  System Components

The system components are:

- Deployment Manager: `caribou/monitors/deployment_manager.py:DeploymentManager`
- Deployment Solver: `caribou/deployment_solver`
- Deployment Utility: `caribou/deployment/client/cli`
- Deployment Migrator: `caribou/monitors/deployment_migrator.py:DeploymentMigrator`
- Invocation Client: `caribou/endpoint/client.py:Client`
- [Metrics Manager](docs/metrics_manager.md):
  - Data Collector: `caribou/data_collector`
  - Deployment Solver Input: `caribou/deployment_solver/deployment_input`

##  Component Interaction

See the [Component Interaction](docs/component_interaction.md) document for a detailed description of how the components interact with each other.

##  Node Naming Scheme

The naming scheme of the nodes (stages in a workflow) can be found in the [Node Naming Scheme](docs/node_naming_scheme.md) document.

##  Source Code Annotations

The source code annotations can be found in the [Source Code Annotations](docs/source_code_annotation.md) document.
