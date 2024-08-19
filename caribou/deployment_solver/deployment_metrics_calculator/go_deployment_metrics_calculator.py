import ctypes
import json
from typing import Any

from caribou.common.constants import GO_PATH, TAIL_LATENCY_THRESHOLD
from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.deployment_metrics_calculator import (
    DeploymentMetricsCalculator,
)
from caribou.deployment_solver.models.instance_indexer import InstanceIndexer
from caribou.deployment_solver.models.region_indexer import RegionIndexer
from caribou.deployment_solver.workflow_config import WorkflowConfig

SEND_GO = f"{GO_PATH}/data_py_go"
REC_GO = f"{GO_PATH}/data_go_py"


def send_to_go(channel_path: str, command: str, data: Any) -> None:
    with open(channel_path, "w", encoding="utf-8") as ch:
        pkt = json.dumps({"command": command, "data": data})
        ch.write(pkt + "\n")
        ch.flush()


def receive_from_go(channel_path: str) -> Any:
    with open(channel_path, "r", encoding="utf-8") as ch:
        data = json.load(ch)
        return data


class GoDeploymentMetricsCalculator(DeploymentMetricsCalculator):
    def __init__(
        self,
        workflow_config: WorkflowConfig,
        input_manager: InputManager,
        region_indexer: RegionIndexer,
        instance_indexer: InstanceIndexer,
        tail_latency_threshold: int = TAIL_LATENCY_THRESHOLD,
        record_transmission_execution_carbon: bool = False,
    ):
        # Not all variables are relevant for other parts
        super().__init__(
            workflow_config,
            input_manager,
            region_indexer,
            instance_indexer,
            tail_latency_threshold,
            record_transmission_execution_carbon,
        )
        self._caribougo = ctypes.CDLL(f"{GO_PATH}/caribougo.so")
        self.setup_go()

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_manager": self._input_manager.to_dict(),
            "tail_latency_threshold": self._tail_latency_threshold,
            "successor_dictionary": self._successor_dictionary,
            "prerequisites_dictionary": self._prerequisites_dictionary,
            "topological_order": self._topological_order,
            "home_region_index": self._home_region_index,
            "record_transmission_execution_carbon": self._record_transmission_execution_carbon,
        }

    def setup_go(self) -> None:
        go_data = json.dumps(self.to_dict())
        self._caribougo.start(str(GO_PATH).encode('utf-8'))
        self._caribougo.goRead()
        send_to_go(SEND_GO, "Setup", go_data)
        receive_from_go(REC_GO)

    def calculate_deployment_metrics(self, deployment: list[int]) -> dict[str, float]:
        self._caribougo.goRead()
        go_data = json.dumps(deployment)
        send_to_go(SEND_GO, "CalculateDeploymentMetrics", go_data)
        ret_data = receive_from_go(REC_GO)
        return ret_data["data"]

    def update_data_for_new_hour(self, hour_to_run: str) -> None:
        self._caribougo.goRead()
        send_to_go(SEND_GO, "UpdateDataForNewHour", hour_to_run)
        _ = receive_from_go(REC_GO)
