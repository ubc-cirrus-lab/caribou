import os
import time
from unittest.mock import Mock
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
import random
from multi_x_serverless.routing.models.region import Region
from multi_x_serverless.routing.solver.bfs_fine_grained_solver import BFSFineGrainedSolver
from multi_x_serverless.routing.solver.stochastic_heuristic_descent_solver import StochasticHeuristicDescentSolver
from multi_x_serverless.routing.solver_inputs.input_manager import InputManager

from multi_x_serverless.routing.workflow_config import WorkflowConfig
import json


class SolverBenchmark:
    def __init__(self, total_nodes=10, sync_nodes=2, num_regions=2, seed=None):
        self.default_solver_class = BFSFineGrainedSolver

        self._seed = seed
        self._dag = self._generate_dag(total_nodes, sync_nodes)
        self._num_regions = num_regions
        self._sync_nodes = sync_nodes

        self._config, self._regions = self._generate_config()
        self.reset_random()

    def reset_random(self):
        self._random = np.random.RandomState(self._seed)

    def mock_get_execution_cost_carbon_runtime(self, *args, **kwargs):
        return (self._random.randint(1, 100), self._random.randint(1, 100), self._random.randint(1, 100))

    def mock_get_transmission_cost_carbon_runtime(self, *args, **kwargs):
        return (self._random.randint(1, 100), self._random.randint(1, 100), self._random.randint(1, 100))

    def run_benchmark(self, solver_class=None, number_of_runs=10) -> dict:
        self.reset_random()
        if not solver_class:
            solver_class = self.default_solver_class

        # Create a WorkflowConfig instance and set its properties using the generated config
        workflow_config = Mock(spec=WorkflowConfig)
        workflow_config.start_hops = self._config["start_hops"]
        workflow_config.regions_and_providers = self._config["regions_and_providers"]
        workflow_config.instances = self._config["instances"]
        workflow_config.constraints = self._config["constraints"]

        # Create a solver instance and run the benchmark
        solver = solver_class(workflow_config, self._regions, False)

        # Mock input manager
        mock_input_manager = Mock(spec=InputManager)
        mock_input_manager.get_execution_cost_carbon_runtime.side_effect = self.mock_get_execution_cost_carbon_runtime
        mock_input_manager.get_transmission_cost_carbon_runtime.side_effect = (
            self.mock_get_transmission_cost_carbon_runtime
        )
        solver._input_manager = mock_input_manager

        # Time solving function
        runtimes = []
        best_costs = []
        best_runtimes = []
        best_carbons = []
        number_of_final_results = []
        for _ in range(number_of_runs):
            start_time = time.time()
            deployments = solver._solve(self._regions)
            end_time = time.time()

            best_cost = max([deployment[1] for deployment in deployments])
            best_runtime = max([deployment[2] for deployment in deployments])
            best_carbon = max([deployment[3] for deployment in deployments])

            runtimes.append(end_time - start_time)
            best_costs.append(best_cost)
            best_runtimes.append(best_runtime)
            best_carbons.append(best_carbon)
            number_of_final_results.append(len(deployments))

        result_dict = {
            "solver": solver_class.__name__,
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
        regions = [{"provider": f"p1", "region": f"r{i+1}"} for i in range(self._num_regions)]
        config["start_hops"] = regions[0]
        config["regions_and_providers"] = {"providers": {f"p1": None}}

        # Generate instances
        instances = []
        for node in self._dag.nodes:
            instance = {
                "instance_name": f"i{node+1}",
                "function_name": f"f{node+1}",
                "succeeding_instances": [f"i{i+1}" for i in self._dag.successors(node)],
                "preceding_instances": [f"i{i+1}" for i in self._dag.predecessors(node)],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {f"p1": None},
                },
            }
            instances.append(instance)

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

    def visualize_dag(self):
        pos = nx.spring_layout(self._dag)
        nx.draw(self._dag, pos, with_labels=True)

        # Get the directory of the current script
        dir_path = os.path.dirname(os.path.realpath(__file__))
        # Create a new directory for the image
        os.makedirs(os.path.join(dir_path, "images"), exist_ok=True)
        # Save the image in the new directory
        plt.savefig(os.path.join(dir_path, "images", "dag.png"))


seed = 10

results = []

for total_nodes in range(2, 10):
    for sync_nodes in range(1, 4):
        for num_regions in range(2, 6):
            print(f"Running benchmark with {total_nodes} nodes, {sync_nodes} sync nodes and {num_regions} regions")
            solverBenchmark = SolverBenchmark(
                total_nodes=total_nodes, sync_nodes=sync_nodes, num_regions=num_regions, seed=seed
            )
            results.append(solverBenchmark.run_benchmark(BFSFineGrainedSolver))
            results.append(solverBenchmark.run_benchmark(StochasticHeuristicDescentSolver))

per_solver_results = {}

for result in results:
    solver = result["solver"]
    if solver not in per_solver_results:
        per_solver_results[solver] = []
    per_solver_results[solver].append(result)

results_json = json.dumps(per_solver_results)

# Get the directory of the current script
dir_path = os.path.dirname(os.path.realpath(__file__))

# Store the results in a file
with open(os.path.join(dir_path, "results.json"), "w") as f:
    f.write(results_json)
