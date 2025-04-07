import importlib
import json
import logging
from typing import Any, Dict

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
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
        # First check if this is an SNS-triggered invocation
        if "Records" in event and len(event["Records"]) == 1 and "Sns" in event["Records"][0]:
            logger.info("Processing SNS-triggered invocation")
            sns_message = event["Records"][0]["Sns"]["Message"]
            try:
                # Try to parse the SNS message as JSON
                event = json.loads(sns_message)
            except json.JSONDecodeError:
                # If it's not JSON, treat it as a string
                event = sns_message
        
        # If event is still a string, try to parse it as JSON
        if isinstance(event, str):
            try:
                event = json.loads(event)
            except json.JSONDecodeError:
                pass  # Keep as string if not JSON
        
        app = importlib.import_module("app")
        workflow = app.workflow
        
        # Get the actual payload data (after SNS unwrapping)
        if isinstance(event, dict):
            # Payload can be either a string or dict, try to parse if string
            payload_str = event.get("payload", {})
            if isinstance(payload_str, str):
                try:
                    payload = json.loads(payload_str)
                except json.JSONDecodeError:
                    payload = payload_str
            else:
                payload = payload_str
        else:
            payload = {}
        
        logger.info("Function payload: %s", json.dumps(payload, indent=2))
        
        # Get the target function name from root level
        target_function_name = None
        if isinstance(event, dict):
            target_function_name = event.get("target")
        
        # Find the target function in the workflow's registered functions
        target_function = None
        if target_function_name:
            logger.info("Looking for function %s in workflow's registered functions", target_function_name)
            for func_name, caribou_func in workflow.functions.items():
                logger.info("Checking function: %s (name: %s)", func_name, caribou_func.name)
                if caribou_func.name == target_function_name:
                    target_function = caribou_func.wrapped_function
                    logger.info("Found target function: %s", func_name)
                    break
        else:
            # If no target specified, find the entry point function
            logger.info("No target specified, looking for entry point function")
            for func_name, caribou_func in workflow.functions.items():
                logger.info("Checking function: %s (name: %s)", func_name, caribou_func.name)
                if caribou_func.entry_point:
                    target_function = caribou_func.wrapped_function
                    target_function_name = caribou_func.name
                    logger.info("Found entry point function: %s", func_name)
                    break
        
        if not target_function:
            raise ValueError(f"Function {target_function_name} not found in workflow")
        
        # Call the target function with the event
        logger.info("Calling target function with event...")
        result = target_function(event)
        logger.info("Function call completed successfully")
        
        return {
            "statusCode": 200,
            "body": json.dumps(result)
        }
        
    except Exception as e:
        logger.error("Error in generic handler: %s", str(e), exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e)
            })
        }