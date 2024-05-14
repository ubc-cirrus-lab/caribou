# Caribou

Caribou is a framework for running and deploying complex serverless workflows multi-constraint and multi-objective aware on AWS over multiple regions.

##  Table of Contents

- [Introduction](#introduction)
- [Example](#example)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Testing](#testing)
- [Benchmarks](#benchmarks)
- [Troubleshooting](#troubleshooting)
- [Paper](#paper)
- [License](#license)

##  Introduction

Caribou is a framework for running and deploying complex serverless workflows multi-constraint and multi-objective aware on AWS over multiple regions.
The workflows can be developed in Python and deployed to AWS Lambda.
The framework is designed to be self-adapting and self-optimizing with regards to the carbon footprint, cost, and performance of the workflows.
A workflow that is developed and deployed using the Caribou Python package can be run and solved by the framework, which will then automatically, depending on constraints and objectives, adapt the workflow deployment to optimize the workflow's performance, cost, and carbon footprint.
The priority of optimization is determined by the objectives set by the workflow developer in the corresponding workflow deployment manifest.
Optimization is done, when warranted by the type of application and frequency of its invocation, by the deployment solver that solves for new multi-region deployment configurations.
The overhead of the system plays a crucial role in the optimization process and sets the frequency as well as the granularity of the optimization process.
See the [architecture](ARCHITECTURE.md) outline for more information on the framework.

##  Example

An example workflow can be found in `examples/small_sync_example` including the source code of the workflow in the `app.py` file as well as the deployment manifest in the `.caribou` directory.
More information on the example workflow can be found in the respective [README](examples/small_sync_example/README.md).

See the [Installation](#installation) section on how to get set up and the [Quick Start](#quick-start) section on how to run the example workflow.

##  Project Structure

```
> tree .
├── common                              # Common code shared between the different components
│   ├── models                          # Data models used by the different components
│   ├── setup                           # Setup code for the AWS environment
│   ├── constants.py                    # Constants used by the different components
│   └── utils.py                        # Utility functions used by the different components
├── data_collector                      # Data collector of the Metrics Manager
├── deployment                          # Deployment components
│   ├── client                          # Deployment Utility
│   ├── common                          # Common code shared between the two deployment components
│   └── server                          # Deployment Migrator functions
├── deployment_solver                   # Deployment Solver
│   ├── deployment_algorithms           # Deployment algorithms (such as the HBSS algorithm)
│   ├── deployment_input                # Input retriever and calculators of the Metrics Manager
│   └── deployment_metrics_calculator   # Metrics simulator of the Metrics Manager (Monte Carlo)
├── monitors                            # Monitoring components
│   ├── deployment_manager.py           # Deployment Manager
│   └── deployment_migrator.py          # Deployment Migrator
├── syncers                             # Data syncer components (sub-component of Metrics Manager)
│   ├── log_syncer.py                   # Log syncer (for all workflows)
│   └── log_sync_workflow.py            # Log syncer (for specific workflow)
```

##  Installation

See the [Installation](INSTALL.md) guide.

## Quick Start

See the [Quick Start](QUICK_START.md) guide.

## Testing

See the [Testing](TESTING.md) guide.

## Benchmarks

See the [Benchmarks](benchmarks/README.md) guide.

## Troubleshooting

See the [Troubleshooting](TROUBLESHOOTING.md) guide.

## Paper

If you use Caribou in your research, please cite the following paper:

**TODO:** Add paper citation

## License

Apache License 2.0
