import importlib
import json
import logging
from typing import Any, Dict, Optional, Union

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _parse_event(event: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Parse and normalize the event data."""
    if isinstance(event, str):
        try:
            return json.loads(event)
        except json.JSONDecodeError:
            return {"payload": event}
    return event


def _get_payload(event: Dict[str, Any]) -> Any:
    """Extract and parse the payload from the event."""
    payload = event.get("payload", {})
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return payload
    return payload


def _find_target_function(workflow: Any, target_name: Optional[str] = None) -> tuple[Any, str]:
    """Find the target function in the workflow."""
    if target_name:
        for func_name, caribou_func in workflow.functions.items():
            if caribou_func.name == target_name:
                return caribou_func.wrapped_function, func_name
    else:
        for func_name, caribou_func in workflow.functions.items():
            if caribou_func.entry_point:
                return caribou_func.wrapped_function, caribou_func.name
    raise ValueError(f"Function {target_name} not found in workflow")


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """
    Generic Lambda handler that dynamically routes to the appropriate function based on the target in the payload.
    Handles both direct invocations and SNS-triggered invocations.

    The expected payload structure (after SNS unwrapping if needed):
    {
        "workflow_placement_decision": {...},
        "transmission_taint": "...",
        "number_of_hops_from_client_request": 1,
        "target": "function_name",  # Target is at root level
        "payload": {...}  # Payload is at root level
    }
    """
    try:
        # Handle SNS-triggered invocations
        if "Records" in event and len(event["Records"]) == 1 and "Sns" in event["Records"][0]:
            sns_message = event["Records"][0]["Sns"]["Message"]
            event = _parse_event(sns_message)
        else:
            event = _parse_event(event)

        # Import and get workflow
        app = importlib.import_module("app")
        workflow = app.workflow

        # Get payload and target function
        payload = _get_payload(event)
        target_function_name = event.get("target") if isinstance(event, dict) else None
        target_function, func_name = _find_target_function(workflow, target_function_name)

        # Call the target function
        result = target_function(event)

        return {"statusCode": 200, "body": json.dumps(result)}

    except (ValueError, json.JSONDecodeError, ImportError) as e:
        logger.error("Error in generic handler: %s", str(e), exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
