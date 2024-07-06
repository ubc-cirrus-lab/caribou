import ctypes
import json
import pdb
import random
import time
from abc import ABC

from caribou.common.constants import TAIL_LATENCY_THRESHOLD
from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.deployment_metrics_calculator import \
    DeploymentMetricsCalculator
from caribou.deployment_solver.deployment_metrics_calculator.models.workflow_instance import WorkflowInstance
from caribou.deployment_solver.models.dag import DAG
from caribou.deployment_solver.models.instance_indexer import InstanceIndexer
from caribou.deployment_solver.models.region_indexer import RegionIndexer
from caribou.deployment_solver.workflow_config import WorkflowConfig

GO_PATH = "/Users/pjavanrood/Documents/Code/caribou-go"
CaribouGo = ctypes.CDLL(f"{GO_PATH}/caribougo.so")
SEND_GO = f"{GO_PATH}/data_py_go"
REC_GO = f"{GO_PATH}/data_go_py"


def send_to_go(channel_path, command, data):
    with open(channel_path, "w") as ch:
        pkt = json.dumps({
            "command": command,
            "data": data
        })
        ch.write(pkt + '\n')
        ch.flush()


def receive_from_go(channel_path):
    with open(channel_path, "r") as ch:
        data = json.load(ch)
        return data


class GoDeploymentMetricsCalculator(DeploymentMetricsCalculator):
    def __init__(self, workflow_config: WorkflowConfig, input_manager: InputManager, region_indexer: RegionIndexer,
                 instance_indexer: InstanceIndexer, tail_latency_threshold: int = TAIL_LATENCY_THRESHOLD,
                 record_transmission_execution_carbon: bool = False):
        # Not all variables are relevant for other parts
        super().__init__(workflow_config, input_manager, region_indexer, instance_indexer, tail_latency_threshold,
                         record_transmission_execution_carbon)
        # pdb.set_trace()
        self.setup_go()

    def toDict(self):
        return {
            "input_manager": self._input_manager.toDict(),
            "tail_latency_threshold": self._tail_latency_threshold,
            "successor_dictionary": self._successor_dictionary,
            "prerequisites_dictionary": self._prerequisites_dictionary,
            "topological_order": self._topological_order,
            "home_region_index": self._home_region_index,
            "record_transmission_execution_carbon": self._record_transmission_execution_carbon,
        }

    def setup_go(self):
        go_data = json.dumps(self.toDict())
        open(f"{GO_PATH}/go_data.json", "w").write(json.dumps(self.toDict(), indent=4))
        CaribouGo.start()
        CaribouGo.goRead()
        send_to_go(SEND_GO, "Setup", go_data)
        receive_from_go(REC_GO)

    def calculate_deployment_metrics(self, deployment: list[int]) -> dict[str, float]:
        start = time.time()
        # Get average and tail cost/carbon/runtime from Monte Carlo simulation
        # pdb.set_trace()
        CaribouGo.goRead()
        go_data = json.dumps(deployment)
        send_to_go(SEND_GO, "CalculateDeploymentMetrics", go_data)
        ret_data = receive_from_go(REC_GO)
        # print(f"Took: {time.time() - start}")
        # pdb.set_trace()
        return ret_data["data"]

    def update_data_for_new_hour(self, hour_to_run: str) -> None:
        pass
