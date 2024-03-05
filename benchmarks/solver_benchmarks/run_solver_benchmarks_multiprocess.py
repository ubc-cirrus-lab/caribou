import json
import multiprocessing
import os
import random
import time
from unittest.mock import Mock

import networkx as nx


from multi_x_serverless.routing.deployment_algorithms.coarse_grained_deployment_algorithm import (
    CoarseGrainedDeploymentAlgorithm,
)
from multi_x_serverless.routing.deployment_algorithms.stochastic_heuristic_deployment_algorithm import (
    StochasticHeuristicDeploymentAlgorithm,
)
from multi_x_serverless.routing.deployment_algorithms.fine_grained_deployment_algorithm import (
    FineGrainedDeploymentAlgorithm,
)
from benchmarks.solver_benchmarks.benchmark_remote_client import BenchmarkRemoteClient
from multi_x_serverless.routing.workflow_config import WorkflowConfig
from multi_x_serverless.routing.deployment_input.input_manager import InputManager
from multi_x_serverless.routing.deployment_algorithms.deployment_algorithm import DeploymentAlgorithm
from unittest.mock import patch


class SolverBenchmark:
    def __init__(self, total_nodes=10, sync_nodes=2, num_regions=2, seed=None):
        self.default_deployment_algorithm_class = FineGrainedDeploymentAlgorithm

        self._seed = seed
        self._dag = self._generate_dag(total_nodes, sync_nodes)

        self._num_regions = num_regions
        self._sync_nodes = sync_nodes

        self._config, self._regions = self._generate_config()
        self._deterministic = False

        self._benchmark_remote_client = BenchmarkRemoteClient(self._regions, seed, self._config)

    @patch("multi_x_serverless.routing.deployment_input.input_manager.Endpoints")
    @patch("multi_x_serverless.routing.deployment_algorithms.deployment_algorithm.Endpoints")
    def run_benchmark(
        self, mock_endpoints, deployment_algorithms_mock_endpoints, deployment_algorithm_class=None, number_of_runs=10
    ):
        if not deployment_algorithm_class:
            deployment_algorithm_class = self.default_deployment_algorithm_class

        print(f"Running benchmark for {deployment_algorithm_class.__name__}")
        print(
            f"Total nodes: {len(self._dag.nodes)}, Sync nodes: {self._sync_nodes}, Regions: {self._num_regions}, Number of runs: {number_of_runs}"
        )

        # Mock the remote client
        mock_endpoints.return_value.get_data_collector_client.return_value = self._benchmark_remote_client
        deployment_algorithms_mock_endpoints.return_value.get_data_collector_client.return_value = (
            self._benchmark_remote_client
        )

        # Create a WorkflowConfig instance and set its properties using the generated config
        workflow_config = Mock(spec=WorkflowConfig)
        workflow_config.start_hops = self._config["start_hops"]
        workflow_config.regions_and_providers = self._config["regions_and_providers"]
        workflow_config.instances = self._config["instances"]
        workflow_config.constraints = self._config["constraints"]
        workflow_config.workflow_id = self._config["workflow_id"]

        # Create a deployment_algorithm instance and run the benchmark
        deployment_algorithm = deployment_algorithm_class(workflow_config)

        # Time solving function
        runtimes = []
        best_costs = []
        best_runtimes = []
        best_carbons = []
        number_of_final_results = []
        for _ in range(number_of_runs):
            start_time = time.time()
            deployments = deployment_algorithm.run()
            end_time = time.time()

            print(deployments)

            best_cost = min([deployment[1] for deployment in deployments])
            best_runtime = min([deployment[2] for deployment in deployments])
            best_carbon = min([deployment[3] for deployment in deployments])

            runtimes.append(end_time - start_time)
            best_costs.append(best_cost)
            best_runtimes.append(best_runtime)
            best_carbons.append(best_carbon)
            number_of_final_results.append(len(deployments))

        result_dict = {
            "deployment_algorithm": deployment_algorithm_class.__name__,
            "runtime": sum(runtimes) / len(runtimes),
            "number of final results": sum(number_of_final_results) / len(number_of_final_results),
            "best cost": sum(best_costs) / len(best_costs),
            "best runtime": sum(best_runtimes) / len(best_runtimes),
            "best carbon": sum(best_carbons) / len(best_carbons),
            "number of regions": self._num_regions,
            "number of instances": len(self._dag.nodes),
            "number of sync nodes": self._sync_nodes,
        }
        return result_dict

    def get_dag_representation(self):
        return self._dag

    def get_config(self):
        return self._config

    def get_regions(self):
        return self._regions

    def _generate_config(self):
        config = {}

        # Generate regions
        regions = [f"p1:r{i+1}" for i in range(self._num_regions)]
        config["start_hops"] = regions[0]
        config["regions_and_providers"] = {"providers": {f"p1": None}}
        config["workflow_id"] = "benchmark_workflow"

        # Generate instances
        instances = {}
        for node in self._dag.nodes:
            instance = {
                "instance_name": f"i{node+1}",
                "function_name": f"f{node+1}",
                "succeeding_instances": [f"i{i+1}" for i in self._dag.successors(node)],
                "preceding_instances": [f"i{i+1}" for i in self._dag.predecessors(node)],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {f"p1": {"config": {"memory": 128}}},
                },
            }
            instances[f"i{node+1}"] = instance

        config["instances"] = instances

        config["constraints"] = None

        return config, regions

    def _generate_dag(self, total_nodes, merge_nodes):
        if self._seed is not None:
            random.seed(self._seed)

        G = nx.DiGraph()
        G.add_node(0)  # Add root node

        for i in range(1, total_nodes):
            possible_parents = list(range(i))  # Nodes that can be parents of the current node
            parent = random.choice(possible_parents)
            G.add_edge(parent, i)

            # Add extra edges for merge nodes
            if i < total_nodes - 1 and merge_nodes > 0:  # Don't add merge nodes for the last node
                possible_parents.remove(parent)
                if possible_parents:  # If there are still possible parents
                    extra_parent = random.choice(possible_parents)
                    G.add_edge(extra_parent, i)
                    merge_nodes -= 1

        return G


seed = 10

# Validate that the deployment_algorithms return the same results when deterministic is set to True
print("Validating deployment_algorithms")
total_nodes = 3  # CODE FOR DEBUGGING
sync_nodes = 1
num_regions = 3
deployment_algorithmBenchmark = SolverBenchmark(
    total_nodes=total_nodes, sync_nodes=sync_nodes, num_regions=num_regions, seed=seed
)

result_1 = deployment_algorithmBenchmark.run_benchmark(
    deployment_algorithm_class=FineGrainedDeploymentAlgorithm, number_of_runs=10
)
result_2 = deployment_algorithmBenchmark.run_benchmark(
    deployment_algorithm_class=StochasticHeuristicDeploymentAlgorithm, number_of_runs=10
)
result_3 = deployment_algorithmBenchmark.run_benchmark(
    deployment_algorithm_class=CoarseGrainedDeploymentAlgorithm, number_of_runs=10
)

rounding_decimals = 4
# round results
for dimension in ["best cost", "best runtime", "best carbon"]:
    result_1[dimension] = round(result_1[dimension], rounding_decimals)
    result_2[dimension] = round(result_2[dimension], rounding_decimals)
    result_3[dimension] = round(result_3[dimension], rounding_decimals)
    print(f"{dimension}: {result_1[dimension]} == {result_2[dimension]} == {result_3[dimension]}")

assert result_1["best cost"] == result_2["best cost"] == result_3["best cost"]
assert result_1["best runtime"] == result_2["best runtime"] == result_3["best runtime"]
assert result_1["best carbon"] == result_2["best carbon"] == result_3["best carbon"]

deployment_algorithmBenchmark.set_deterministic(False)

print("Validation successful")

# Benchmark all deployment_algorithms
print("Running benchmarks")
results = []
inputs = []
deployment_algorithms = []
scenarios = {"total_nodes": range(3, 9), "sync_nodes": range(1, 5), "num_regions": range(3, 10, 2)}

counter = 0
for total_nodes in scenarios["total_nodes"]:
    for sync_nodes in scenarios["sync_nodes"]:
        if sync_nodes > total_nodes - 1:
            continue
        for num_regions in scenarios["num_regions"]:
            inputs.append((total_nodes, sync_nodes, num_regions, seed))
            deployment_algorithms.append(
                SolverBenchmark(total_nodes=total_nodes, sync_nodes=sync_nodes, num_regions=num_regions, seed=seed)
            )
            counter += 1

with multiprocessing.Pool(processes=len(inputs)) as pool:
    print("Starting CoarseGrainedDeploymentAlgorithm benchmarks at ", time.ctime())
    cg_results = pool.starmap(
        SolverBenchmark.run_benchmark,
        [(deployment_algorithm, CoarseGrainedDeploymentAlgorithm) for deployment_algorithm in deployment_algorithms],
    )
    print("Starting StochasticHeuristicDeploymentAlgorithm benchmarks at ", time.ctime())
    shd_results = pool.starmap(
        SolverBenchmark.run_benchmark,
        [
            (deployment_algorithm, StochasticHeuristicDeploymentAlgorithm)
            for deployment_algorithm in deployment_algorithms
        ],
    )
    print("Starting FineGrainedDeploymentAlgorithm benchmarks at ", time.ctime())
    bfs_results = pool.starmap(
        SolverBenchmark.run_benchmark,
        [(deployment_algorithm, FineGrainedDeploymentAlgorithm) for deployment_algorithm in deployment_algorithms],
    )
    results = cg_results + shd_results + bfs_results

per_deployment_algorithm_results = {}

for result in results:
    deployment_algorithm = result["deployment_algorithm"]
    if deployment_algorithm not in per_deployment_algorithm_results:
        per_deployment_algorithm_results[deployment_algorithm] = []
    per_deployment_algorithm_results[deployment_algorithm].append(result)

# Get the directory of the current script
dir_path = os.path.dirname(os.path.realpath(__file__))

# Check if results directory exists and create it if not
os.makedirs(os.path.join(dir_path, "results"), exist_ok=True)

# Store the results in a file
with open(os.path.join(dir_path, "results/results.json"), "w") as f:
    json.dump(per_deployment_algorithm_results, f, indent=4)
