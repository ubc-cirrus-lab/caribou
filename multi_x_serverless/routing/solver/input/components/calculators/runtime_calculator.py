from typing import Any

from multi_x_serverless.routing.solver.input.components.calculator import InputCalculator

from multi_x_serverless.routing.solver.input.components.loaders.performance_loader import PerformanceLoader
from multi_x_serverless.routing.solver.input.components.loaders.workflow_loader import WorkflowLoader

from multi_x_serverless.routing.models.dag import DAG

class RuntimeCalculator(InputCalculator):
    _dag: DAG

    def __init__(self, performance_loader: PerformanceLoader, workflow_loader: WorkflowLoader) -> None:
        super().__init__()
        self._performance_loader: PerformanceLoader = performance_loader
        self._workflow_loader: WorkflowLoader = workflow_loader

    def calculate_runtime(self, instance_name: str, region_name: str, consider_probabilistic_invocations: bool = False) -> float:
        if consider_probabilistic_invocations:
            # TODO (#76): Implement probabilistic invocations
            raise NotImplementedError("Probabilistic invocations are not supported yet")
        else:
            return self._calculate_raw_runtime(instance_name, region_name, False)

    def calculate_latency(
        self, 
        from_instance_name: str,
        to_instance_name: str,
        from_region_name: str,
        to_region_name: str,
        consider_probabilistic_invocations: bool = False) -> float:
        if consider_probabilistic_invocations:
            # TODO (#76): Implement probabilistic invocations
            raise NotImplementedError("Probabilistic invocations are not supported yet")
        else:
            return self._calculate_raw_latency(from_instance_name, to_instance_name, from_region_name, to_region_name, False)
    
    def _calculate_raw_runtime(self, instance_name: str, region_name: str, use_tail_runtime: bool = False) -> float:
        self._workflow_loader.get_runtime(instance_name, region_name, use_tail_runtime)
        return 0

    def _calculate_raw_latency(self, from_instance_name: str, to_instance_name: str, from_region_name: str, to_region_name: str, use_tail_runtime: bool = False) -> float:
        return 0