import statistics
from multiprocessing import Manager, Process
from queue import Queue
from typing import Tuple

import numpy as np
import scipy.stats as st

from caribou.common.constants import TAIL_LATENCY_THRESHOLD
from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.deployment_metrics_calculator import (
    DeploymentMetricsCalculator,
)
from caribou.deployment_solver.models.instance_indexer import InstanceIndexer
from caribou.deployment_solver.models.region_indexer import RegionIndexer
from caribou.deployment_solver.workflow_config import WorkflowConfig


def _simulation_worker(
    input_manager: InputManager,
    workflow_config: WorkflowConfig,
    region_indexer: RegionIndexer,
    instance_indexer: InstanceIndexer,
    tail_latency_threshold: int,
    n_iterations: int,
    input_queue: Queue,
    output_queue: Queue,
) -> None:
    deployment_metrics_calculator: DeploymentMetricsCalculator = DeploymentMetricsCalculator(
        workflow_config, input_manager, region_indexer, instance_indexer, tail_latency_threshold
    )
    while True:
        received_input = input_queue.get()
        if isinstance(received_input, str) or received_input is None:
            if received_input == "exit":
                break
            input_manager.alter_carbon_setting(received_input)
            output_queue.put("OK")
            continue
        deployment = received_input
        costs_distribution_list: list[float] = []
        runtimes_distribution_list: list[float] = []
        carbons_distribution_list: list[float] = []
        for _ in range(n_iterations):
            results = deployment_metrics_calculator.calculate_workflow(deployment)
            costs_distribution_list.append(results["cost"])
            runtimes_distribution_list.append(results["runtime"])
            carbons_distribution_list.append(results["carbon"])
        output_queue.put(
            (
                costs_distribution_list,
                runtimes_distribution_list,
                carbons_distribution_list,
            )
        )


class SimpleDeploymentMetricsCalculator(DeploymentMetricsCalculator):
    def __init__(
        self,
        workflow_config: WorkflowConfig,
        input_manager: InputManager,
        region_indexer: RegionIndexer,
        instance_indexer: InstanceIndexer,
        tail_latency_threshold: int = TAIL_LATENCY_THRESHOLD,
        n_processes: int = 4,
        record_transmission_execution_carbon: bool = False,
    ):
        super().__init__(
            workflow_config,
            input_manager,
            region_indexer,
            instance_indexer,
            tail_latency_threshold,
            record_transmission_execution_carbon,
        )
        self.n_processes = n_processes
        self.batch_size = 1
        # self.batch_size = 200
        if n_processes > 1:
            self._setup(
                workflow_config,
                input_manager,
                region_indexer,
                instance_indexer,
                tail_latency_threshold,
                n_processes,
            )

    def _setup(
        self,
        workflow_config: WorkflowConfig,
        input_manager: InputManager,
        region_indexer: RegionIndexer,
        instance_indexer: InstanceIndexer,
        tail_latency_threshold: int,
        n_processes: int,
    ) -> None:
        self._manager = Manager()
        self._input_queue: Queue = self._manager.Queue()
        self._output_queue: Queue = self._manager.Queue()
        self._pool = self._init_workers(
            workflow_config,
            input_manager,
            region_indexer,
            instance_indexer,
            tail_latency_threshold,
            n_processes,
            self.batch_size // n_processes,
            self._input_queue,
            self._output_queue,
        )

    def _init_workers(
        self,
        workflow_config: WorkflowConfig,
        input_manager: InputManager,
        region_indexer: RegionIndexer,
        instance_indexer: InstanceIndexer,
        tail_latency_threshold: int,
        n_processes: int,
        n_iterations: int,
        input_queue: Queue,
        output_queue: Queue,
    ) -> list[Process]:
        pool = []
        for _ in range(n_processes):
            p = Process(
                target=_simulation_worker,
                args=(
                    input_manager,
                    workflow_config,
                    region_indexer,
                    instance_indexer,
                    tail_latency_threshold,
                    n_iterations,
                    input_queue,
                    output_queue,
                ),
            )
            p.start()
            pool.append(p)
        return pool

    def calculate_workflow_loop(
        self, deployment: list
    ) -> Tuple[list[float], list[float], list[float], list[float], list[float]]:
        costs_distribution_list: list[float] = []
        runtimes_distribution_list: list[float] = []
        carbons_distribution_list: list[float] = []
        transmission_carbon_list: list[float] = []
        execution_carbon_list: list[float] = []
        if self.n_processes > 1:
            for _ in range(self.n_processes):
                self._input_queue.put(deployment)
            for _ in range(self.n_processes):
                result = self._output_queue.get()
                costs_distribution_list.extend(result[0])
                runtimes_distribution_list.extend(result[1])
                carbons_distribution_list.extend(result[2])
            assert self._input_queue.empty()
            assert self._output_queue.empty()
        else:
            for _ in range(self.batch_size):
                results = self.calculate_workflow(deployment)
                costs_distribution_list.append(results["cost"])
                runtimes_distribution_list.append(results["runtime"])
                carbons_distribution_list.append(results["carbon"])

                if self._record_transmission_execution_carbon:
                    transmission_carbon_list.append(results["transmission_carbon"])
                    execution_carbon_list.append(results["execution_carbon"])

        return (
            costs_distribution_list,
            runtimes_distribution_list,
            carbons_distribution_list,
            transmission_carbon_list,
            execution_carbon_list,
        )

    def _perform_monte_carlo_simulation(self, deployment: list[int]) -> dict[str, float]:
        """
        Perform a Monte Carlo simulation to both the average and tail
        cost, runtime, and carbon footprint of the deployment.
        """
        costs_distribution_list: list[float] = []
        runtimes_distribution_list: list[float] = []
        carbons_distribution_list: list[float] = []

        execution_carbon_list: list[float] = []
        transmission_carbon_list: list[float] = []

        max_number_of_iterations = 1
        # max_number_of_iterations = 2000
        threshold = 0.05
        number_of_iterations = 0
        while number_of_iterations < max_number_of_iterations:
            results = self.calculate_workflow_loop(deployment)
            costs_distribution_list.extend(results[0])
            runtimes_distribution_list.extend(results[1])
            carbons_distribution_list.extend(results[2])

            if self._record_transmission_execution_carbon:
                transmission_carbon_list.extend(results[3])
                execution_carbon_list.extend(results[4])

            number_of_iterations += self.batch_size

            all_within_threshold = True

            for distribution in [runtimes_distribution_list, carbons_distribution_list, costs_distribution_list]:
                mean = np.mean(distribution)
                len_distribution = len(distribution)
                if mean and len_distribution > 1:
                    ci_low, ci_up = st.t.interval(
                        1 - threshold, len_distribution - 1, loc=mean, scale=st.sem(distribution)
                    )
                    ci_width = ci_up - ci_low
                    relative_ci_width = ci_width / mean
                    if relative_ci_width > threshold:
                        all_within_threshold = False
                        break
                elif all_within_threshold:
                    break

        # print("Min Run Time: ", min(runtimes_distribution_list))
        # print("Max Run Time: ", max(runtimes_distribution_list))
        result = {
            "average_cost": float(statistics.mean(costs_distribution_list)),
            "average_runtime": float(statistics.mean(runtimes_distribution_list)),
            "average_carbon": float(statistics.mean(carbons_distribution_list)),
            "tail_cost": float(np.percentile(costs_distribution_list, self._tail_latency_threshold)),
            "tail_runtime": float(np.percentile(runtimes_distribution_list, self._tail_latency_threshold)),
            "tail_carbon": float(np.percentile(carbons_distribution_list, self._tail_latency_threshold)),
        }

        if self._record_transmission_execution_carbon:
            result["average_execution_carbon"] = float(statistics.mean(execution_carbon_list))
            result["average_transmission_carbon"] = float(statistics.mean(transmission_carbon_list))

        return result

    def update_data_for_new_hour(self, hour_to_run: str) -> None:
        if self.n_processes == 1:
            return
        for _ in range(self.n_processes):
            self._input_queue.put(hour_to_run)
        for _ in range(self.n_processes):
            _ = self._output_queue.get()
        assert self._input_queue.empty()
        assert self._output_queue.empty()

    def __del__(self) -> None:
        if self.n_processes > 1:
            for _ in range(self.n_processes):
                self._input_queue.put("exit")
            for p in self._pool:
                p.kill()
