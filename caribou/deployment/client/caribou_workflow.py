# from __future__ import annotations

# import ast
# import base64
# import binascii
# import json
# import logging
# import time
# import uuid
# from datetime import datetime
# from typing import Any, Callable, Optional

# from caribou.common.constants import GLOBAL_TIME_ZONE, LOG_VERSION, TIME_FORMAT, WORKFLOW_PLACEMENT_DECISION_TABLE
# from caribou.common.models.endpoints import Endpoints
# from caribou.common.models.remote_client.remote_client_factory import RemoteClientFactory
# from caribou.common.utils import get_function_source
# from caribou.deployment.client.caribou_function import CaribouFunction

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)
# logger.addHandler(logging.StreamHandler())


# class CustomEncoder(json.JSONEncoder):
#     def default(self, o: Any) -> Any:
#         if isinstance(o, bytes):
#             return "b64:" + base64.b64encode(o).decode()
#         return json.JSONEncoder.default(self, o)


# class CustomDecoder(json.JSONDecoder):
#     def decode(self, s, _w=json.decoder.WHITESPACE.match):  # type: ignore
#         decoded_dict = super().decode(s)
#         self.decode_values(decoded_dict)
#         return decoded_dict

#     def decode_values(self, item: Any) -> None:
#         if isinstance(item, dict):
#             for key, value in item.items():
#                 if isinstance(value, str) and value.startswith("b64:"):
#                     try:
#                         # Remove the prefix before decoding
#                         item[key] = base64.b64decode(value[4:])
#                     except (TypeError, binascii.Error):
#                         pass
#                 else:
#                     self.decode_values(value)
#         elif isinstance(item, list):
#             for i, value in enumerate(item):
#                 if isinstance(value, str) and value.startswith("b64:"):
#                     try:
#                         # Remove the prefix before decoding
#                         item[i] = base64.b64decode(value[4:])
#                     except (TypeError, binascii.Error):
#                         pass
#                 else:
#                     self.decode_values(value)


# class CaribouWorkflow:
#     """
#     CaribouWorkflow class that is used to register functions as a collection of connected serverless functions.

#     Every workflow must have an instance of this class.
#     The instance is used to register functions as serverless functions.

#     Every workflow must have one function that is the entry point for the workflow. This function must be registered
#     with the `serverless_function` decorator with the `entry_point` parameter set to `True`.

#     :param name: The name of the workflow.
#     """

#     def __init__(self, name: str, version: str):
#         self.name = name
#         self.version = version
#         self.functions: dict[str, CaribouFunction] = {}
#         self._registered_functions: dict[str, Callable[..., Any]] = {}
#         self._registered_function_names: set = set()
#         self._run_id_to_successor_index: dict[str, int] = {}
#         self._function_names: set[str] = set()
#         self._endpoint = Endpoints()
#         self._current_workflow_placement_decision: dict[str, Any] = {}
#         self.function_map: dict[str, Callable[..., Any]] = {}

#     def get_run_id(self) -> str:
#         return self._current_workflow_placement_decision["run_id"]

#     def get_successors(self, function: CaribouFunction) -> list[CaribouFunction]:
#         """
#         Get the functions that are called by this function.
#         """
#         source_code = get_function_source(function.function_callable)
#         tree = ast.parse(source_code)
#         function_calls = [
#             node
#             for node in ast.walk(tree)
#             if isinstance(node, ast.Call) and getattr(node.func, "attr", None) == "invoke_serverless_function"
#         ]
#         successors: list[CaribouFunction] = []
#         for call in function_calls:
#             function_name = call.args[0].id  # type: ignore
#             successor = next(
#                 (func for func in self.functions.values() if func.function_callable.__name__ == function_name), None
#             )
#             if successor:
#                 successors.append(successor)
#             else:
#                 raise RuntimeError(f"Could not find function with name {function_name}, was the function registered?")
#         return successors

#     def invoke_serverless_function(self, function_name: str, payload: dict) -> Any:
#         """
#         Invoke a registered serverless function by its name with the provided payload.

#         Args:
#             function_name (str): The name of the function to invoke.
#             payload (dict): The payload to pass to the function.

#         Returns:
#             Any: The result of the function invocation.
#         """
#         if function_name not in self._registered_functions:
#             raise RuntimeError(f"Function '{function_name}' is not registered in the workflow")

#         function_wrapper = self._registered_functions[function_name]
#         func = function_wrapper.function

#         # Log the invocation
#         self.log_for_retrieval(
#             f"INVOKING: Function ({function_name}) with PAYLOAD_SIZE "
#             f"({len(json.dumps(payload, cls=CustomEncoder).encode('utf-8')) / (1024 ** 3)}) GB",
#             self._current_workflow_placement_decision["run_id"],
#         )

#         # Invoke the function
#         result = func(payload)

#         # Log the result
#         self.log_for_retrieval(
#             f"RESULT: Function ({function_name}) returned RESULT_SIZE "
#             f"({len(json.dumps(result, cls=CustomEncoder).encode('utf-8')) / (1024 ** 3)}) GB",
#             self._current_workflow_placement_decision["run_id"],
#         )

#         return result


#     def _inform_sync_node_of_conditional_non_execution(
#         self, workflow_placement_decision: dict[str, Any], successor_instance_name: str, current_instance_name: str
#     ) -> None:
#         # If the successor is a sync node, we need to inform the platform that the function has finished.
#         if successor_instance_name.split(":", maxsplit=2)[1] == "sync":
#             self._inform_and_invoke_sync_node(
#                 workflow_placement_decision, successor_instance_name, current_instance_name
#             )

#         # If the successor has dependent sync predecessors,
#         # we need to inform the platform that the function has finished.
#         for instance in workflow_placement_decision["instances"].values():
#             if instance["instance_name"] == successor_instance_name and "dependent_sync_predecessors" in instance:
#                 for predecessor_and_sync in instance.get("dependent_sync_predecessors", []):
#                     predecessor = predecessor_and_sync[0]
#                     sync_node = predecessor_and_sync[1]

#                     self._inform_and_invoke_sync_node(workflow_placement_decision, sync_node, predecessor)
#                 break

#     def _inform_and_invoke_sync_node(
#         self, workflow_placement_decision: dict[str, Any], successor_instance_name: str, predecessor_instance_name: str
#     ) -> None:
#         provider, region, identifier = self.get_successor_workflow_placement_decision(
#             successor_instance_name, workflow_placement_decision
#         )

#         expected_counter = len(
#             set(
#                 workflow_placement_decision.get("instances", {})
#                 .get(successor_instance_name, {})
#                 .get("preceding_instances", [])
#             )
#         )

#         reached_states = RemoteClientFactory.get_remote_client(provider, region).set_predecessor_reached(
#             predecessor_name=predecessor_instance_name,
#             sync_node_name=successor_instance_name,
#             workflow_instance_id=workflow_placement_decision["run_id"],
#             direct_call=False,
#         )

#         # If all the predecessors have been reached and any of them have directly reached the sync node
#         # then we can call the sync node
#         if len(reached_states) == expected_counter and any(reached_states):
#             successor_workflow_placement_decision = self.get_successor_workflow_placement_decision_dictionary(
#                 workflow_placement_decision, successor_instance_name
#             )
#             payload_wrapper = {}
#             payload_wrapper["workflow_placement_decision"] = successor_workflow_placement_decision
#             json_payload = json.dumps(payload_wrapper)

#             RemoteClientFactory.get_remote_client(provider, region).invoke_function(
#                 message=json_payload,
#                 identifier=identifier,
#                 workflow_instance_id=workflow_placement_decision["run_id"],
#                 sync=False,
#             )

#         log_message = (
#             f"INFORMING_SYNC_NODE: INSTANCE ({predecessor_instance_name}) informing "
#             f"SYNC_NODE ({successor_instance_name}) of non-execution"
#         )
#         self.log_for_retrieval(
#             log_message,
#             workflow_placement_decision["run_id"],
#         )

#     def get_successor_workflow_placement_decision(
#         self, successor_instance_name: str, workflow_placement_decision: dict[str, Any]
#     ) -> tuple[str, str, str]:
#         if (
#             "send_to_home_region" in workflow_placement_decision and workflow_placement_decision["send_to_home_region"]
#         ) or ("current_deployment" not in workflow_placement_decision["workflow_placement"]):
#             provider_region = workflow_placement_decision["workflow_placement"]["home_deployment"][
#                 successor_instance_name
#             ]["provider_region"]
#             identifier = workflow_placement_decision["workflow_placement"]["home_deployment"][successor_instance_name][
#                 "identifier"
#             ]
#         else:
#             provider_region = workflow_placement_decision["workflow_placement"]["current_deployment"]["instances"][
#                 workflow_placement_decision["time_key"]
#             ][successor_instance_name]["provider_region"]
#             identifier = workflow_placement_decision["workflow_placement"]["current_deployment"]["instances"][
#                 workflow_placement_decision["time_key"]
#             ][successor_instance_name]["identifier"]

#         return provider_region["provider"], provider_region["region"], identifier

#     # This method is used to get the name of the next successor instance and its workflow_placement decision.
#     def get_successor_instance_name(
#         self,
#         function: Callable[..., Any],
#         workflow_placement_decision: dict[str, Any],
#     ) -> tuple[str, dict[str, Any]]:
#         # Get the name of the successor function
#         successor_function_name = self.functions[function.original_function.__name__].name  # type: ignore

#         # Set the current instance name based on whether it is the entry point or not
#         current_instance_name = workflow_placement_decision["current_instance_name"]

#         # Get the name of the next instance based on the current instance and successor function name
#         next_instance_name = self.get_next_instance_name(
#             current_instance_name, workflow_placement_decision, successor_function_name
#         )

#         # Create the successor workflow_placement decision by copying the original workflow_placement decision
#         # and updating the current instance name to the next instance name
#         successor_workflow_placement_decision = self.get_successor_workflow_placement_decision_dictionary(
#             workflow_placement_decision, next_instance_name
#         )

#         # Return the next instance name and the successor workflow_placement decision
#         return next_instance_name, successor_workflow_placement_decision

#     def get_successor_workflow_placement_decision_dictionary(
#         self, workflow_placement_decision: dict[str, Any], next_instance_name: str
#     ) -> dict[str, Any]:
#         # Copy the workflow_placement decision
#         successor_workflow_placement_decision = workflow_placement_decision.copy()
#         # Update the current instance name to the next instance name
#         successor_workflow_placement_decision["current_instance_name"] = next_instance_name
#         return successor_workflow_placement_decision

#     # This method is used to get the name of the next successor instance based on the current instance name,
#     # workflow_placement decision, and successor function name.
#     def get_next_instance_name(
#         self, current_instance_name: str, workflow_placement_decision: dict[str, Any], successor_function_name: str
#     ) -> str:
#         # Get the workflow_placement decision for the current instance
#         if current_instance_name not in workflow_placement_decision["instances"]:
#             raise RuntimeError(
#                 f"Could not find current instance name {current_instance_name} in {workflow_placement_decision['instances']}"  # pylint: disable=line-too-long
#             )
#         instance = workflow_placement_decision["instances"][current_instance_name]
#         successor_instances = instance["succeeding_instances"]
#         # If there is only one successor instance, return it
#         if len(successor_instances) == 1:
#             if (
#                 successor_instances[0].split(":", maxsplit=1)[0]
#                 == f"{self.name}-{self.version.replace('.', '_')}-{successor_function_name}"
#             ):
#                 return successor_instances[0]
#             raise RuntimeError(
#                 f"Could not find successor instance for successor function name {successor_function_name} in {successor_instances}"  # pylint: disable=line-too-long
#             )
#         # If there are multiple successor instances, return the first one that matches the successor function
#         # name and has the correct index
#         for successor_instance in successor_instances:
#             if (
#                 successor_instance.split(":", maxsplit=1)[0]
#                 == f"{self.name}-{self.version.replace('.', '_')}-{successor_function_name}"
#             ):
#                 if successor_instance.split(":", maxsplit=2)[1] == "sync":
#                     return successor_instance
#                 if successor_instance.split(":", maxsplit=2)[1].split("_")[-1] == str(
#                     self._run_id_to_successor_index.get(workflow_placement_decision["run_id"], 0)
#                 ):
#                     if workflow_placement_decision["run_id"] not in self._run_id_to_successor_index:
#                         self._run_id_to_successor_index[workflow_placement_decision["run_id"]] = 0
#                     self._run_id_to_successor_index[workflow_placement_decision["run_id"]] += 1
#                     return successor_instance
#         raise RuntimeError(
#             f"Could not find successor instance for successor function name {successor_function_name} in {successor_instances}"  # pylint: disable=line-too-long
#         )

#     def get_workflow_placement_decision(self) -> dict[str, Any]:
#         """
#         The structure of the workflow placement decision is explained in the
#         `docs/design.md` file under `Workflow Placement Decision`.
#         """
#         return self._current_workflow_placement_decision

#     def get_predecessor_data(self) -> list[dict[str, Any]]:
#         """
#         Get the data returned by the predecessor functions.

#         This method is only invoked if the sync function was
#         called which means all predecessor functions have finished.
#         """
#         (
#             provider,
#             region,
#             current_instance_name,
#             workflow_instance_id,
#         ) = self.get_current_instance_provider_region_instance_name()

#         client = RemoteClientFactory.get_remote_client(provider, region)

#         response = client.get_predecessor_data(current_instance_name, workflow_instance_id)

#         return [json.loads(message, cls=CustomDecoder) for message in response]

#     def get_current_instance_provider_region_instance_name(self) -> tuple[str, str, str, str]:
#         workflow_placement_decision = self.get_workflow_placement_decision()

#         if "current_instance_name" not in workflow_placement_decision:
#             raise RuntimeError(
#                 "Could not get current instance name, is this the entry point? Entry point cannot be sync function"
#             )

#         if "run_id" not in workflow_placement_decision:
#             raise RuntimeError(
#                 "Could not get workflow instance id, is this the entry point? Entry point cannot be sync function"
#             )

#         current_instance_name = workflow_placement_decision["current_instance_name"]
#         workflow_instance_id = workflow_placement_decision["run_id"]

#         key = self._get_deployment_key(workflow_placement_decision)

#         if key == "home_deployment":
#             provider_region = workflow_placement_decision["workflow_placement"][key][current_instance_name][
#                 "provider_region"
#             ]
#         else:
#             time_key = workflow_placement_decision["time_key"]
#             provider_region = workflow_placement_decision["workflow_placement"][key]["instances"][time_key][
#                 current_instance_name
#             ]["provider_region"]
#         return provider_region["provider"], provider_region["region"], current_instance_name, workflow_instance_id

#     def _get_deployment_key(self, workflow_placement_decision: dict[str, Any]) -> str:
#         key = "home_deployment"
#         if workflow_placement_decision.get("send_to_home_region", False) or (
#             "current_deployment" not in workflow_placement_decision["workflow_placement"]
#         ):
#             return key

#         # Check if the deployment is not expired
#         deployment_expiry_time = workflow_placement_decision["workflow_placement"]["current_deployment"]["expiry_time"]
#         if deployment_expiry_time is not None:
#             # If the deployment is expired, return the home deployment
#             if datetime.now(GLOBAL_TIME_ZONE) <= datetime.strptime(deployment_expiry_time, TIME_FORMAT):
#                 key = "current_deployment"

#         return key

#     def get_workflow_placement_decision_from_platform(self) -> dict[str, Any]:
#         """
#         Get the workflow_placement decision from the platform.
#         """
#         result = self._endpoint.get_deployment_algorithm_workflow_placement_decision_client().get_value_from_table(
#             WORKFLOW_PLACEMENT_DECISION_TABLE, f"{self.name}-{self.version}"
#         )
#         if result is not None:
#             return json.loads(result)

#         raise RuntimeError("Could not get workflow_placement decision from platform")

#     def register_function(
#         self,
#         function: Callable[..., Any],
#         name: str,
#         entry_point: bool,
#         regions_and_providers: dict,
#         environment_variables: list[dict[str, str]],
#     ) -> None:
#         """
#         Register a function as a serverless function.

#         Where the function is deployed depends on the workflow_placement decision which will be made by the
#         deployment algorithm.

#         At this point we only need to register the function with the wrapper, the actual deployment will be done
#         later by the deployment manager.
#         """
#         wrapper = CaribouFunction(function, name, entry_point, regions_and_providers, environment_variables)

#         if name in self._registered_functions:
#             raise RuntimeError(f"Function with the name {name} already registered")
        
#         self._registered_functions[name] = wrapper

#         if function.__name__ in self._registered_function_names:
#             raise RuntimeError(f"Function with function name {function.__name__} already registered")

#         self._registered_function_names.add(function.__name__)

#     def serverless_function(
#         self,
#         name: Optional[str] = None,
#         entry_point: bool = False,
#         regions_and_providers: Optional[dict] = None,
#         environment_variables: Optional[list[dict[str, str]]] = None,
#     ) -> Callable[..., Any]:
#         if regions_and_providers is None:
#             regions_and_providers = {}

#         if environment_variables is None:
#             environment_variables = []
#         else:
#             if not isinstance(environment_variables, list):
#                 raise RuntimeError("environment_variables must be a list of dicts")
#             for env_variable in environment_variables:
#                 if not isinstance(env_variable, dict):
#                     raise RuntimeError("environment_variables must be a list of dicts")
#                 if "key" not in env_variable or "value" not in env_variable:
#                     raise RuntimeError("environment_variables must be a list of dicts with keys 'key' and 'value'")
#                 if not isinstance(env_variable["key"], str):
#                     raise RuntimeError("environment_variables must be a list of dicts with 'key' as a string")
#                 if not isinstance(env_variable["value"], str):
#                     raise RuntimeError("environment_variables must be a list of dicts with 'value' as a string")

#         def _register_handler(func: Callable[..., Any]) -> Callable[..., Any]:
#             handler_name = name if name is not None else func.__name__

#             def wrapper(*args, **kwargs):
#                 argument_raw = args[0]

#                 if (
#                     "Records" in argument_raw
#                     and len(argument_raw["Records"]) == 1
#                     and "Sns" in argument_raw["Records"][0]
#                     and "Message" in argument_raw["Records"][0]["Sns"]
#                 ):
#                     argument = json.loads(argument_raw["Records"][0]["Sns"]["Message"], cls=CustomDecoder)
#                 else:
#                     try:
#                         argument = json.loads(argument_raw, cls=CustomDecoder)
#                     except json.JSONDecodeError as e:
#                         raise RuntimeError(
#                             f"Could not get message from argument {argument_raw}, there should be meta information in the message"
#                         ) from e

#                 if isinstance(argument, dict):
#                     transmission_taint = argument.get("transmission_taint", "N/A")
#                 else:
#                     transmission_taint = "N/A"

#                 if entry_point:
#                     send_to_home_region = False
#                     time_invoked_at_client = None
#                     pre_loaded_workflow_placement_decision = None
#                     if isinstance(argument, dict) and "workflow_placement_decision" in argument:
#                         time_invoked_at_client = argument.get("time_request_sent", None)
#                         pre_loaded_workflow_placement_decision = argument.get("workflow_placement_decision", None)
#                         send_to_home_region = pre_loaded_workflow_placement_decision.get("send_to_home_region", False)
#                         argument = argument.get("input_data", {})

#                     init_latency = "N/A"
#                     if time_invoked_at_client:
#                         datetime_invoked_at_client = datetime.strptime(time_invoked_at_client, TIME_FORMAT)
#                         datetime_now = datetime.now(GLOBAL_TIME_ZONE)
#                         time_difference_datetime = datetime_now - datetime_invoked_at_client
#                         init_latency = str(time_difference_datetime.total_seconds())
#                     if pre_loaded_workflow_placement_decision:
#                         workflow_placement_decision = pre_loaded_workflow_placement_decision
#                     else:
#                         workflow_placement_decision = self.get_workflow_placement_decision_from_platform()

#                     workflow_placement_decision["run_id"] = uuid.uuid4().hex
#                     workflow_placement_decision["send_to_home_region"] = send_to_home_region
#                     workflow_placement_decision["time_key"] = self._get_time_key(workflow_placement_decision)

#                     if len(args) == 0:
#                         return func()
#                     payload = argument

#                     log_message = (
#                         f"ENTRY_POINT: : Entry Point INSTANCE "
#                         f'({workflow_placement_decision["current_instance_name"]}) '
#                         f'of workflow {f"{self.name}-{self.version}"} called with PAYLOAD_SIZE '
#                         f'({len(json.dumps(payload, cls=CustomEncoder).encode("utf-8")) / (1024**3)}) GB and '
#                         f"INIT_LATENCY ({init_latency}) s"
#                     )
#                     self.log_for_retrieval(
#                         log_message,
#                         workflow_placement_decision["run_id"],
#                     )
#                 else:
#                     if "workflow_placement_decision" not in argument:
#                         raise RuntimeError("Could not get workflow_placement decision from message")
#                     workflow_placement_decision = argument["workflow_placement_decision"]
#                     payload = argument.get("payload", {})

#                 log_message = (
#                     f'INVOKED: INSTANCE ({workflow_placement_decision["current_instance_name"]}) '
#                     f"called with TAINT ({transmission_taint})"
#                 )
#                 self.log_for_retrieval(
#                     log_message,
#                     workflow_placement_decision["run_id"],
#                 )

#                 self._current_workflow_placement_decision = workflow_placement_decision
#                 self._run_id_to_successor_index[workflow_placement_decision["run_id"]] = 0

#                 cpu_model = self.get_cpu_info()
#                 self.log_for_retrieval(
#                     f"CPU_MODEL: {cpu_model}",
#                     workflow_placement_decision["run_id"],
#                 )

#                 # Routing logic
#                 function_name = argument.get("function_name")
#                 if function_name:
#                     # Lookup the registered function based on the function name
#                     registered_function = self.get_registered_function(function_name)
#                     if registered_function:
#                         start_time = time.time()
#                         result = registered_function(payload, **kwargs)
#                         end_time = time.time()

#                         log_message = (
#                             f'EXECUTED: INSTANCE ({workflow_placement_decision["current_instance_name"]}) '
#                             f"executed EXECUTION_TIME ({end_time - start_time})"
#                         )
#                         self.log_for_retrieval(
#                             log_message,
#                             workflow_placement_decision["run_id"],
#                         )
#                         return result
#                     else:
#                         raise RuntimeError(f"Function {function_name} is not registered.")
#                 else:
#                     raise RuntimeError("Function name not provided in the arguments.")

#             wrapper.original_function = func
#             self.register_function(func, handler_name, entry_point, regions_and_providers, environment_variables)
#             return wrapper

#         return _register_handler
    
#     def get_registered_function(self, function_name: str) -> Optional[Callable[..., Any]]:
#         """Retrieve a registered function by name."""
#         for function_wrapper in self.functions.values():
#             if function_wrapper.name == function_name:
#                 return function_wrapper.function
#         return None
    
#     def lambda_handler(event: dict, context: Any) -> Any:
#         """
#         AWS Lambda handler function that routes the invocation to the correct function within the workflow.
#         The function to be invoked is determined based on the 'function_name' field in the event payload.
#         """
#         # Log the incoming event for debugging
#         print(f"Received event: {json.dumps(event)}")

#         # Extract the function name from the event payload
#         try:
#             message = json.loads(event["Records"][0]["Sns"]["Message"])
#             function_name = message.get("function_name")
#         except (KeyError, json.JSONDecodeError) as e:
#             print(f"Error extracting function name: {e}")
#             raise RuntimeError("Invalid event payload")

#         if not function_name:
#             raise RuntimeError("Function name not specified in the event payload")

#         try:
#             func = getattr(CaribouWorkflow, function_name)
#         except AttributeError:
#             raise RuntimeError(f"Function '{function_name}' not found in the workflow")

#         payload = message.get("payload", {})
#         result = func(payload)

#         return {
#             "statusCode": 200,
#             "body": json.dumps(result)
#         }
    
#     def log_for_retrieval(self, message: str, run_id: str) -> None:
#         """
#         Log a message for retrieval by the platform.
#         """
#         logger.info(
#             "TIME (%s) RUN_ID (%s) MESSAGE (%s) LOG_VERSION (%s)",
#             datetime.now(GLOBAL_TIME_ZONE).strftime(TIME_FORMAT),
#             run_id,
#             message,
#             LOG_VERSION,
#         )

#     def _get_time_key(self, workflow_placement_decision: dict[str, Any]) -> str:
#         if "time_key" in workflow_placement_decision:
#             return workflow_placement_decision["time_key"]
#         if "current_deployment" not in workflow_placement_decision["workflow_placement"]:
#             return "N/A"

#         all_time_keys = workflow_placement_decision["workflow_placement"]["current_deployment"]["time_keys"]

#         current_hour_of_day = datetime.now(GLOBAL_TIME_ZONE).hour

#         previous_time_key = max(time_key for time_key in all_time_keys if int(time_key) <= current_hour_of_day)

#         return previous_time_key

#     def get_cpu_info(self) -> str:
#         # Log the CPU model, workflow name, and the request ID
#         cpu_model = ""
#         with open("/proc/cpuinfo", encoding="utf-8") as f:
#             for line in f:
#                 if "model name" in line:
#                     cpu_model = line.split(":")[1].strip()  # Extracting and cleaning the model name
#                     break  # No need to continue the loop once the model name is found
#         return cpu_model

from __future__ import annotations

import ast
import base64
import binascii
import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Optional

from caribou.common.constants import GLOBAL_TIME_ZONE, LOG_VERSION, TIME_FORMAT, WORKFLOW_PLACEMENT_DECISION_TABLE
from caribou.common.models.endpoints import Endpoints
from caribou.common.models.remote_client.remote_client_factory import RemoteClientFactory
from caribou.common.utils import get_function_source
from caribou.deployment.client.caribou_function import CaribouFunction

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


class CustomEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, bytes):
            return "b64:" + base64.b64encode(o).decode()
        return json.JSONEncoder.default(self, o)


class CustomDecoder(json.JSONDecoder):
    def decode(self, s, _w=json.decoder.WHITESPACE.match):  # type: ignore
        decoded_dict = super().decode(s)
        self.decode_values(decoded_dict)
        return decoded_dict

    def decode_values(self, item: Any) -> None:
        if isinstance(item, dict):
            for key, value in item.items():
                if isinstance(value, str) and value.startswith("b64:"):
                    try:
                        # Remove the prefix before decoding
                        item[key] = base64.b64decode(value[4:])
                    except (TypeError, binascii.Error):
                        pass
                else:
                    self.decode_values(value)
        elif isinstance(item, list):
            for i, value in enumerate(item):
                if isinstance(value, str) and value.startswith("b64:"):
                    try:
                        # Remove the prefix before decoding
                        item[i] = base64.b64decode(value[4:])
                    except (TypeError, binascii.Error):
                        pass
                else:
                    self.decode_values(value)


class CaribouWorkflow:
    """
    CaribouWorkflow class that is used to register functions as a collection of connected serverless functions.

    Every workflow must have an instance of this class.
    The instance is used to register functions as serverless functions.

    Every workflow must have one function that is the entry point for the workflow. This function must be registered
    with the `serverless_function` decorator with the `entry_point` parameter set to `True`.

    :param name: The name of the workflow.
    """

    def __init__(self, name: str, version: str):
        self.name = name
        self.version = version
        self.functions: dict[str, CaribouFunction] = {}
        self._run_id_to_successor_index: dict[str, int] = {}
        self._function_names: set[str] = set()
        self._endpoint = Endpoints()
        self._current_workflow_placement_decision: dict[str, Any] = {}

    def get_run_id(self) -> str:
        return self._current_workflow_placement_decision["run_id"]

    def get_successors(self, function: CaribouFunction) -> list[CaribouFunction]:
        """
        Get the functions that are called by this function.
        """
        source_code = get_function_source(function.function_callable)
        tree = ast.parse(source_code)
        function_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and getattr(node.func, "attr", None) == "invoke_serverless_function"
        ]
        successors: list[CaribouFunction] = []
        for call in function_calls:
            function_name = call.args[0].id  # type: ignore
            successor = next(
                (func for func in self.functions.values() if func.function_callable.__name__ == function_name), None
            )
            if successor:
                successors.append(successor)
            else:
                raise RuntimeError(f"Could not find function with name {function_name}, was the function registered?")
        return successors

    def invoke_serverless_function(
        self,
        function: Callable[..., Any],
        payload: Optional[dict | Any] = None,
        conditional: bool = True,
    ) -> None:
        """
        Invoke a serverless function which is part of this workflow.
        """
        # If the function from which this function is called is the entry point obtain current
        # workflow_placement decision
        # If not, the workflow_placement decision was stored in the message received from the
        # predecessor function
        # Post message to SNS -> return
        # Do not wait for response
        workflow_placement_decision = self.get_workflow_placement_decision()

        current_instance_name = workflow_placement_decision["current_instance_name"]

        successor_instance_name, successor_workflow_placement_decision_dictionary = self.get_successor_instance_name(
            function, workflow_placement_decision
        )

        if not conditional:
            # For the sync nodes we still need to inform the platform that the function has finished.
            self._inform_sync_node_of_conditional_non_execution(
                workflow_placement_decision, successor_instance_name, current_instance_name
            )

            # We don't call the function if it is conditional and the condition is not met.
            log_message = (
                f"CONDITIONAL_NON_EXECUTION: INSTANCE ({current_instance_name}) calling "
                f"SUCCESSOR ({successor_instance_name})"
            )
            self.log_for_retrieval(
                log_message,
                workflow_placement_decision["run_id"],
            )
            return

        # Wrap the payload and add the workflow_placement decision
        payload_wrapper = {}
        if payload:
            payload_wrapper["payload"] = payload

        transmission_taint = uuid.uuid4().hex

        payload_wrapper["workflow_placement_decision"] = successor_workflow_placement_decision_dictionary
        payload_wrapper["transmission_taint"] = transmission_taint
        json_payload = json.dumps(payload_wrapper, cls=CustomEncoder)

        provider, region, identifier = self.get_successor_workflow_placement_decision(
            successor_instance_name, workflow_placement_decision
        )

        is_successor_sync_node = successor_instance_name.split(":", maxsplit=2)[1] == "sync"

        expected_counter = -1
        if is_successor_sync_node:
            expected_counter = len(
                set(workflow_placement_decision["instances"][successor_instance_name]["preceding_instances"])
            )

        RemoteClientFactory.get_remote_client(provider, region).invoke_function(
            message=json_payload,
            identifier=identifier,
            workflow_instance_id=workflow_placement_decision["run_id"],
            sync=is_successor_sync_node,
            function_name=successor_instance_name,
            expected_counter=expected_counter,
            current_instance_name=current_instance_name,
        )

        log_message = (
            f"INVOKING_SUCCESSOR: INSTANCE ({current_instance_name}) calling "
            f"SUCCESSOR ({successor_instance_name}) with PAYLOAD_SIZE "
            f"({len(json_payload.encode('utf-8')) / (1024**3)}) GB and TAINT ({transmission_taint})"
        )
        self.log_for_retrieval(
            log_message,
            workflow_placement_decision["run_id"],
        )

    def _inform_sync_node_of_conditional_non_execution(
        self, workflow_placement_decision: dict[str, Any], successor_instance_name: str, current_instance_name: str
    ) -> None:
        # If the successor is a sync node, we need to inform the platform that the function has finished.
        if successor_instance_name.split(":", maxsplit=2)[1] == "sync":
            self._inform_and_invoke_sync_node(
                workflow_placement_decision, successor_instance_name, current_instance_name
            )

        # If the successor has dependent sync predecessors,
        # we need to inform the platform that the function has finished.
        for instance in workflow_placement_decision["instances"].values():
            if instance["instance_name"] == successor_instance_name and "dependent_sync_predecessors" in instance:
                for predecessor_and_sync in instance.get("dependent_sync_predecessors", []):
                    predecessor = predecessor_and_sync[0]
                    sync_node = predecessor_and_sync[1]

                    self._inform_and_invoke_sync_node(workflow_placement_decision, sync_node, predecessor)
                break

    def _inform_and_invoke_sync_node(
        self, workflow_placement_decision: dict[str, Any], successor_instance_name: str, predecessor_instance_name: str
    ) -> None:
        provider, region, identifier = self.get_successor_workflow_placement_decision(
            successor_instance_name, workflow_placement_decision
        )

        expected_counter = len(
            set(
                workflow_placement_decision.get("instances", {})
                .get(successor_instance_name, {})
                .get("preceding_instances", [])
            )
        )

        reached_states = RemoteClientFactory.get_remote_client(provider, region).set_predecessor_reached(
            predecessor_name=predecessor_instance_name,
            sync_node_name=successor_instance_name,
            workflow_instance_id=workflow_placement_decision["run_id"],
            direct_call=False,
        )

        # If all the predecessors have been reached and any of them have directly reached the sync node
        # then we can call the sync node
        if len(reached_states) == expected_counter and any(reached_states):
            successor_workflow_placement_decision = self.get_successor_workflow_placement_decision_dictionary(
                workflow_placement_decision, successor_instance_name
            )
            payload_wrapper = {}
            payload_wrapper["workflow_placement_decision"] = successor_workflow_placement_decision
            json_payload = json.dumps(payload_wrapper)

            RemoteClientFactory.get_remote_client(provider, region).invoke_function(
                message=json_payload,
                identifier=identifier,
                workflow_instance_id=workflow_placement_decision["run_id"],
                sync=False,
            )

        log_message = (
            f"INFORMING_SYNC_NODE: INSTANCE ({predecessor_instance_name}) informing "
            f"SYNC_NODE ({successor_instance_name}) of non-execution"
        )
        self.log_for_retrieval(
            log_message,
            workflow_placement_decision["run_id"],
        )

    def get_successor_workflow_placement_decision(
        self, successor_instance_name: str, workflow_placement_decision: dict[str, Any]
    ) -> tuple[str, str, str]:
        if (
            "send_to_home_region" in workflow_placement_decision and workflow_placement_decision["send_to_home_region"]
        ) or ("current_deployment" not in workflow_placement_decision["workflow_placement"]):
            provider_region = workflow_placement_decision["workflow_placement"]["home_deployment"][
                successor_instance_name
            ]["provider_region"]
            identifier = workflow_placement_decision["workflow_placement"]["home_deployment"][successor_instance_name][
                "identifier"
            ]
        else:
            provider_region = workflow_placement_decision["workflow_placement"]["current_deployment"]["instances"][
                workflow_placement_decision["time_key"]
            ][successor_instance_name]["provider_region"]
            identifier = workflow_placement_decision["workflow_placement"]["current_deployment"]["instances"][
                workflow_placement_decision["time_key"]
            ][successor_instance_name]["identifier"]

        return provider_region["provider"], provider_region["region"], identifier

    # This method is used to get the name of the next successor instance and its workflow_placement decision.
    def get_successor_instance_name(
        self,
        function: Callable[..., Any],
        workflow_placement_decision: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        # Get the name of the successor function
        successor_function_name = self.functions[function.original_function.__name__].name  # type: ignore

        # Set the current instance name based on whether it is the entry point or not
        current_instance_name = workflow_placement_decision["current_instance_name"]

        # Get the name of the next instance based on the current instance and successor function name
        next_instance_name = self.get_next_instance_name(
            current_instance_name, workflow_placement_decision, successor_function_name
        )

        # Create the successor workflow_placement decision by copying the original workflow_placement decision
        # and updating the current instance name to the next instance name
        successor_workflow_placement_decision = self.get_successor_workflow_placement_decision_dictionary(
            workflow_placement_decision, next_instance_name
        )

        # Return the next instance name and the successor workflow_placement decision
        return next_instance_name, successor_workflow_placement_decision

    def get_successor_workflow_placement_decision_dictionary(
        self, workflow_placement_decision: dict[str, Any], next_instance_name: str
    ) -> dict[str, Any]:
        # Copy the workflow_placement decision
        successor_workflow_placement_decision = workflow_placement_decision.copy()
        # Update the current instance name to the next instance name
        successor_workflow_placement_decision["current_instance_name"] = next_instance_name
        return successor_workflow_placement_decision

    # This method is used to get the name of the next successor instance based on the current instance name,
    # workflow_placement decision, and successor function name.
    def get_next_instance_name(
        self, current_instance_name: str, workflow_placement_decision: dict[str, Any], successor_function_name: str
    ) -> str:
        # Get the workflow_placement decision for the current instance
        if current_instance_name not in workflow_placement_decision["instances"]:
            raise RuntimeError(
                f"Could not find current instance name {current_instance_name} in {workflow_placement_decision['instances']}"  # pylint: disable=line-too-long
            )
        instance = workflow_placement_decision["instances"][current_instance_name]
        successor_instances = instance["succeeding_instances"]
        # If there is only one successor instance, return it
        if len(successor_instances) == 1:
            if (
                successor_instances[0].split(":", maxsplit=1)[0]
                == f"{self.name}-{self.version.replace('.', '_')}-{successor_function_name}"
            ):
                return successor_instances[0]
            raise RuntimeError(
                f"Could not find successor instance for successor function name {successor_function_name} in {successor_instances}"  # pylint: disable=line-too-long
            )
        # If there are multiple successor instances, return the first one that matches the successor function
        # name and has the correct index
        for successor_instance in successor_instances:
            if (
                successor_instance.split(":", maxsplit=1)[0]
                == f"{self.name}-{self.version.replace('.', '_')}-{successor_function_name}"
            ):
                if successor_instance.split(":", maxsplit=2)[1] == "sync":
                    return successor_instance
                if successor_instance.split(":", maxsplit=2)[1].split("_")[-1] == str(
                    self._run_id_to_successor_index.get(workflow_placement_decision["run_id"], 0)
                ):
                    if workflow_placement_decision["run_id"] not in self._run_id_to_successor_index:
                        self._run_id_to_successor_index[workflow_placement_decision["run_id"]] = 0
                    self._run_id_to_successor_index[workflow_placement_decision["run_id"]] += 1
                    return successor_instance
        raise RuntimeError(
            f"Could not find successor instance for successor function name {successor_function_name} in {successor_instances}"  # pylint: disable=line-too-long
        )

    def get_workflow_placement_decision(self) -> dict[str, Any]:
        """
        The structure of the workflow placement decision is explained in the
        `docs/design.md` file under `Workflow Placement Decision`.
        """
        return self._current_workflow_placement_decision

    def get_predecessor_data(self) -> list[dict[str, Any]]:
        """
        Get the data returned by the predecessor functions.

        This method is only invoked if the sync function was
        called which means all predecessor functions have finished.
        """
        (
            provider,
            region,
            current_instance_name,
            workflow_instance_id,
        ) = self.get_current_instance_provider_region_instance_name()

        client = RemoteClientFactory.get_remote_client(provider, region)

        response = client.get_predecessor_data(current_instance_name, workflow_instance_id)

        return [json.loads(message, cls=CustomDecoder) for message in response]

    def get_current_instance_provider_region_instance_name(self) -> tuple[str, str, str, str]:
        workflow_placement_decision = self.get_workflow_placement_decision()

        if "current_instance_name" not in workflow_placement_decision:
            raise RuntimeError(
                "Could not get current instance name, is this the entry point? Entry point cannot be sync function"
            )

        if "run_id" not in workflow_placement_decision:
            raise RuntimeError(
                "Could not get workflow instance id, is this the entry point? Entry point cannot be sync function"
            )

        current_instance_name = workflow_placement_decision["current_instance_name"]
        workflow_instance_id = workflow_placement_decision["run_id"]

        key = self._get_deployment_key(workflow_placement_decision)

        if key == "home_deployment":
            provider_region = workflow_placement_decision["workflow_placement"][key][current_instance_name][
                "provider_region"
            ]
        else:
            time_key = workflow_placement_decision["time_key"]
            provider_region = workflow_placement_decision["workflow_placement"][key]["instances"][time_key][
                current_instance_name
            ]["provider_region"]
        return provider_region["provider"], provider_region["region"], current_instance_name, workflow_instance_id

    def _get_deployment_key(self, workflow_placement_decision: dict[str, Any]) -> str:
        key = "home_deployment"
        if workflow_placement_decision.get("send_to_home_region", False) or (
            "current_deployment" not in workflow_placement_decision["workflow_placement"]
        ):
            return key

        # Check if the deployment is not expired
        deployment_expiry_time = workflow_placement_decision["workflow_placement"]["current_deployment"]["expiry_time"]
        if deployment_expiry_time is not None:
            # If the deployment is expired, return the home deployment
            if datetime.now(GLOBAL_TIME_ZONE) <= datetime.strptime(deployment_expiry_time, TIME_FORMAT):
                key = "current_deployment"

        return key

    def get_workflow_placement_decision_from_platform(self) -> dict[str, Any]:
        """
        Get the workflow_placement decision from the platform.
        """
        result = self._endpoint.get_deployment_algorithm_workflow_placement_decision_client().get_value_from_table(
            WORKFLOW_PLACEMENT_DECISION_TABLE, f"{self.name}-{self.version}"
        )
        if result is not None:
            return json.loads(result)

        raise RuntimeError("Could not get workflow_placement decision from platform")

    def register_function(  # pylint: disable=too-many-statements
        self,
        function: Callable[..., Any],
        name: str,
        entry_point: bool,
        regions_and_providers: dict,
        environment_variables: list[dict[str, str]],
    ) -> None:
        """
        Register a function as a serverless function.

        Where the function is deployed depends on the workflow_placement decision which will be made by the
        deployment algorithm.

        At this point we only need to register the function with the wrapper, the actual deployment will be done
        later by the deployment manager.
        """
        wrapper = CaribouFunction(function, name, entry_point, regions_and_providers, environment_variables)
        if function.__name__ in self.functions:
            raise RuntimeError(f"Function with function name {function.__name__} already registered")
        if name in self._function_names:
            raise RuntimeError(f"Function with given name {name} already registered")
        self._function_names.add(name)
        self.functions[function.__name__] = wrapper

    def serverless_function(  # pylint: disable=too-many-statements
        self,
        name: Optional[str] = None,
        entry_point: bool = False,
        regions_and_providers: Optional[dict] = None,
        environment_variables: Optional[list[dict[str, str]]] = None,
    ) -> Callable[..., Any]:
        """
        Decorator to register a function as a Lambda function.

        :param name: The name of the Lambda function. Defaults to the name of the function being decorated.
        :param entry_point: Whether this function is the entry point for the workflow.

        The following is mildly complicated, but it is necessary to make the decorator work.

        The three layers of functions are used to create a decorator with arguments.

        Outermost function (serverless_function):
            This is the decorator factory. It takes in arguments for the decorator and returns the actual
            decorator function.
            The arguments passed to this function are used to configure the behavior of the decorator.
            In this case, name, entry_point, regions_and_providers, and providers are used to configure
            the Lambda function.

        Middle function (_register_handler):
            This is the actual decorator function. It takes in a single argument, which is the function to be decorated.
            It returns a new function that wraps the original function and modifies its behavior.
            In this case, _register_handler takes in func, which is the function to be decorated, and returns wrapper,
            which is a new function that wraps func.
            The middle function is responsible for creating the wrapper function and returning it as well as registering
            the function with the workflow.

        Innermost function (wrapper):
            This is the wrapper function that modifies the behavior of the original function.
            It takes the same arguments as the original function and can modify these arguments
            before calling the original function.
            It can also modify the return value of the original function.
            In this case, wrapper unwraps the arguments of func and retrieves the workflow_placement decision
            and calls func with the modified unwrapped payload.
        """
        if regions_and_providers is None:
            regions_and_providers = {}

        if environment_variables is None:
            environment_variables = []
        else:
            if not isinstance(environment_variables, list):
                raise RuntimeError("environment_variables must be a list of dicts")
            for env_variable in environment_variables:
                if not isinstance(env_variable, dict):
                    raise RuntimeError("environment_variables must be a list of dicts")
                if "key" not in env_variable or "value" not in env_variable:
                    raise RuntimeError("environment_variables must be a list of dicts with keys 'key' and 'value'")
                if not isinstance(env_variable["key"], str):
                    raise RuntimeError("environment_variables must be a list of dicts with 'key' as a string")
                if not isinstance(env_variable["value"], str):
                    raise RuntimeError("environment_variables must be a list of dicts with 'value' as a string")

        def _register_handler(func: Callable[..., Any]) -> Callable[..., Any]:
            handler_name = name if name is not None else func.__name__

            def wrapper(*args, **kwargs):  # type: ignore  # pylint: disable=unused-argument, too-many-branches
                # Modify args and kwargs here as needed
                argument_raw = args[0]

                if (
                    "Records" in argument_raw
                    and len(argument_raw["Records"]) == 1
                    and "Sns" in argument_raw["Records"][0]
                    and "Message" in argument_raw["Records"][0]["Sns"]
                ):
                    argument = json.loads(argument_raw["Records"][0]["Sns"]["Message"], cls=CustomDecoder)
                else:
                    try:
                        argument = json.loads(argument_raw, cls=CustomDecoder)
                    except json.JSONDecodeError as e:
                        raise RuntimeError(
                            f"Could not get message from argument {argument_raw}, there should be meta information in the message"  # pylint: disable=line-too-long
                        ) from e

                if isinstance(argument, dict):
                    transmission_taint = argument.get("transmission_taint", "N/A")
                else:
                    transmission_taint = "N/A"

                if entry_point:
                    send_to_home_region = False
                    time_invoked_at_client = None
                    pre_loaded_workflow_placement_decision = None
                    if isinstance(argument, dict) and "workflow_placement_decision" in argument:
                        time_invoked_at_client = argument.get("time_request_sent", None)
                        pre_loaded_workflow_placement_decision = argument.get("workflow_placement_decision", None)
                        send_to_home_region = pre_loaded_workflow_placement_decision.get("send_to_home_region", False)
                        argument = argument.get("input_data", {})

                    init_latency = "N/A"
                    if time_invoked_at_client:
                        datetime_invoked_at_client = datetime.strptime(time_invoked_at_client, TIME_FORMAT)
                        datetime_now = datetime.now(GLOBAL_TIME_ZONE)
                        time_difference_datetime = datetime_now - datetime_invoked_at_client
                        # Get s from the time difference
                        init_latency = str(time_difference_datetime.total_seconds())
                    if pre_loaded_workflow_placement_decision:
                        workflow_placement_decision = pre_loaded_workflow_placement_decision
                    else:
                        workflow_placement_decision = (
                            self.get_workflow_placement_decision_from_platform()
                        )  # pylint: disable=line-too-long

                    # This is the first function to be called, so we need to generate a run id
                    # This run id will be used to identify the workflow instance
                    workflow_placement_decision["run_id"] = uuid.uuid4().hex
                    workflow_placement_decision["send_to_home_region"] = send_to_home_region
                    workflow_placement_decision["time_key"] = self._get_time_key(workflow_placement_decision)

                    if len(args) == 0:
                        return func()
                    payload = argument

                    log_message = (
                        f"ENTRY_POINT: : Entry Point INSTANCE "
                        f'({workflow_placement_decision["current_instance_name"]}) '
                        f'of workflow {f"{self.name}-{self.version}"} called with PAYLOAD_SIZE '
                        f'({len(json.dumps(payload, cls=CustomEncoder).encode("utf-8")) / (1024**3)}) GB and '
                        f"INIT_LATENCY ({init_latency}) s"
                    )
                    self.log_for_retrieval(
                        log_message,
                        workflow_placement_decision["run_id"],
                    )
                else:
                    # Get the workflow_placement decision from the message received from the predecessor function
                    if "workflow_placement_decision" not in argument:
                        raise RuntimeError("Could not get workflow_placement decision from message")
                    workflow_placement_decision = argument["workflow_placement_decision"]
                    payload = argument.get("payload", {})

                log_message = (
                    f'INVOKED: INSTANCE ({workflow_placement_decision["current_instance_name"]}) '
                    f"called with TAINT ({transmission_taint})"
                )
                self.log_for_retrieval(
                    log_message,
                    workflow_placement_decision["run_id"],
                )

                self._current_workflow_placement_decision = workflow_placement_decision
                self._run_id_to_successor_index[workflow_placement_decision["run_id"]] = 0

                cpu_model = self.get_cpu_info()
                self.log_for_retrieval(
                    f"CPU_MODEL: {cpu_model}",
                    workflow_placement_decision["run_id"],  # type: ignore
                )

                # Call the original function with the modified arguments
                start_time = time.time()
                result = func(payload)
                end_time = time.time()

                log_message = (
                    f'EXECUTED: INSTANCE ({workflow_placement_decision["current_instance_name"]}) '
                    f"executed EXECUTION_TIME ({end_time - start_time})"
                )
                self.log_for_retrieval(
                    log_message,
                    workflow_placement_decision["run_id"],
                )

                return result

            wrapper.original_function = func  # type: ignore
            self.register_function(func, handler_name, entry_point, regions_and_providers, environment_variables)
            return wrapper

        return _register_handler

    def log_for_retrieval(self, message: str, run_id: str) -> None:
        """
        Log a message for retrieval by the platform.
        """
        logger.info(
            "TIME (%s) RUN_ID (%s) MESSAGE (%s) LOG_VERSION (%s)",
            datetime.now(GLOBAL_TIME_ZONE).strftime(TIME_FORMAT),
            run_id,
            message,
            LOG_VERSION,
        )

    def _get_time_key(self, workflow_placement_decision: dict[str, Any]) -> str:
        if "time_key" in workflow_placement_decision:
            return workflow_placement_decision["time_key"]
        if "current_deployment" not in workflow_placement_decision["workflow_placement"]:
            return "N/A"

        all_time_keys = workflow_placement_decision["workflow_placement"]["current_deployment"]["time_keys"]

        current_hour_of_day = datetime.now(GLOBAL_TIME_ZONE).hour

        previous_time_key = max(time_key for time_key in all_time_keys if int(time_key) <= current_hour_of_day)

        return previous_time_key

    def get_cpu_info(self) -> str:
        # Log the CPU model, workflow name, and the request ID
        cpu_model = ""
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            for line in f:
                if "model name" in line:
                    cpu_model = line.split(":")[1].strip()  # Extracting and cleaning the model name
                    break  # No need to continue the loop once the model name is found
        return cpu_model