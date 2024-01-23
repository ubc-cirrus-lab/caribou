import os
import time
from unittest.mock import Mock
import networkx as nx
import matplotlib.pyplot as plt
import random
from multi_x_serverless.routing.models.region import Region
from multi_x_serverless.routing.solver.bfs_fine_grained_solver import BFSFineGrainedSolver
from multi_x_serverless.routing.solver.stochastic_heuristic_descent_solver import StochasticHeuristicDescentSolver
from multi_x_serverless.routing.solver_inputs.input_manager import InputManager

from multi_x_serverless.routing.workflow_config import WorkflowConfig


class SolverBenchmark:
    def __init__(self, total_nodes=10, merge_nodes=2, num_regions=2):
        self.default_solver_class = BFSFineGrainedSolver

        self._dag = self._generate_dag(total_nodes, merge_nodes)
        self._num_regions = num_regions

        self._config, self._regions = self._generate_config()

        self.get_execution_cost_carbon_runtime_return_values = [
            (random.randint(0, 10), random.uniform(0, 1), random.uniform(0, 1))
            for _ in range(num_regions**total_nodes*total_nodes)
        ]
        self.get_transmission_cost_carbon_runtime_return_values = [
            (random.randint(0, 10), random.uniform(0, 1), random.uniform(0, 1))
            for _ in range(num_regions**total_nodes*total_nodes)
        ]

    def run_benchmark(self, solver_class=None):
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
        mock_input_manager.get_execution_cost_carbon_runtime.side_effect = (
            self.get_execution_cost_carbon_runtime_return_values
        )
        mock_input_manager.get_transmission_cost_carbon_runtime.side_effect = (
            self.get_transmission_cost_carbon_runtime_return_values
        )
        solver._input_manager = mock_input_manager

        # Time solving function
        start_time = time.time()
        deployments = solver._solve(self._regions)
        end_time = time.time()

        print(f"Time taken to solve: {end_time - start_time} seconds")
        print(f"Number of final results: {len(deployments)}")

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


# Benchmarking parameters
total_nodes = 7
merge_nodes = 3
num_regions = 3

solverBenchmark = SolverBenchmark(total_nodes=total_nodes, merge_nodes=merge_nodes, num_regions=num_regions)

print("Running benchmark for BFSFineGrainedSolver")
solverBenchmark.run_benchmark(BFSFineGrainedSolver)

solverBenchmark.run_benchmark(StochasticHeuristicDescentSolver)

# print("Running benchmark for StochasticHeuristicDescentSolver")
# solverBenchmark.run_benchmark(StochasticHeuristicDescentSolver)
