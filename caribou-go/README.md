# Caribou-Go

This module re-implements the simple deployment metrics calculator in Go to accelerate the deployment solving process. Specifically, we are re-writing the Monte Carlo simulation to enhance performance and speed up computations. It is integrated with Caribou through the GoDeploymentMetricsCalculator.

### Requirements

To get started, you'll need Go 1.22. You can download and install it from the [Go](https://go.dev/dl/) official website.<br>

Once Go is installed, you can build the project by running the following command:

```shell
source build_caribou.sh
```

This script will:
- Build the Go binary.
- Run unit tests.
- Create the necessary pipes for communication with GoDeploymentMetricsCalculator.

### How It Works?

This module mirrors the functionality of the `SimpleDeploymentMetricsCalculator` found in `caribou/deployment_solver/deployment_metrics_calculator`.

The Go source code is compiled into a shared library, allowing it to be called from Python. The integration between Go and Python is managed through the `GoDeploymentMetricsCalculator`.

Data is passed between the Python and Go components using named pipes. On the Python side, this communication is handled by `GoDeploymentMetricsCalculator`, while on the Go side, it's managed in `main/deploymentcalculator.go`.