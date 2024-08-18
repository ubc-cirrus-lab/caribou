# Caribou-Go
This repository implements the `caribou/deployment_solver/deployment_metrics_calculator/simple_deployment_metrics_calculator.py` in Go.<br>
This is integrated with `Caribou` through `GoDeploymentMetricsCalculator`.<br>
To run this, you need `go 1.22`, which you can install from [Go](https://go.dev/dl/) official website.<br>
Once you have Go installed, simply run:
```shell
source build_caribou.sh
```
This will build the binary, run the unit tests, and create the pipes needed to communicate with `GoDeploymentMetricsCalculator`.