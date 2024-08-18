# Caribou

<div align="center">

[![Build Status](https://github.com/ubc-cirrus-lab/caribou/actions/workflows/workflow.yaml/badge.svg)](https://github.com/ubc-cirrus-lab/caribou/actions/workflows/workflow.yaml) [![GitHub license](https://img.shields.io/badge/license-Apache%202-blue.svg)](https://github.com/ubc-cirrus-lab/caribou/blob/main/LICENSE)

Caribou is a framework for running and deploying complex serverless workflows multi-constraint and multi-objective aware on AWS over multiple regions.

</div>

## ⚡️ Quickstart

See the [Quick Start](QUICK_START.md) guide.

##  Introduction

Caribou is a framework for running and deploying complex serverless workflows multi-constraint and multi-objective aware on hyperscalers over multiple regions.
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

##  Installation

See the [Installation](INSTALL.md) guide.

## Testing

See the [Testing](TESTING.md) guide.

## Benchmarks

See the [Benchmarks](benchmarks/README.md) guide.

## Troubleshooting

See the [Troubleshooting](TROUBLESHOOTING.md) guide.

## Paper

If you use Caribou in your research, please cite the following paper:

**TODO:** Add paper citation

## About

Caribou is being developed at the [Cloud Infrastructure Research for Reliability, Usability, and Sustainability Lab](https://cirrus.ece.ubc.ca) at the [University of British Columbia](https://www.ubc.ca). If you have any questions or feedback, please open a GitHub issue.

## Contributing

See the [Contributing](CONTRIBUTING.md) guide.

## License

Apache License 2.0
