from typing import Any

import logging
from caribou.data_collector.components.carbon.carbon_collector import CarbonCollector
from caribou.data_collector.components.performance.performance_collector import PerformanceCollector
from caribou.data_collector.components.provider.provider_collector import ProviderCollector
from caribou.data_collector.components.workflow.workflow_collector import WorkflowCollector
from caribou.endpoint.client import Client
from caribou.deployment.client import __version__ as MULTI_X_SERVERLESS_VERSION
from caribou.monitors.deployment_manager import DeploymentManager
from caribou.monitors.deployment_migrator import DeploymentMigrator
from caribou.syncers.log_syncer import LogSyncer
# from caribou.common.setup.setup_tables import main as setup_tables_func


# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logging level

def caribou_cli(event: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    action = event.get("action", None)
    if not action:
        logger.error("No action specified")
        return {"status": 400, "message": "No action specified"}
    
    logger.info(f"Received request with action: {action}")
    action_handlers = {
        "run": handle_run,
        "list": handle_list_workflows,
        "version": handle_list_caribou_version,
        "data_collect": handle_data_collect,
        "log_sync": handle_log_sync,
        "manage_deployments": handle_manage_deployments,
        "remove": handle_remove_workflow,
        # "setup_tables": handle_setup_framework_tables, # No reason to include this in the remote CLI
        "run_deployment_migrator": handle_run_deployment_migrator
    }
    
    handler = action_handlers.get(action, handle_default)
    return handler(event)

def handle_run_deployment_migrator(event: dict[str, Any]) -> dict[str, Any]:
    function_deployment_monitor = DeploymentMigrator()
    function_deployment_monitor.check()

    return {"status": 200, "message": "Deployment migrator completed"}

# def handle_setup_framework_tables(event: dict[str, Any]) -> dict[str, Any]:
#     setup_tables_func()
#     return {"status": 200, "message": "Tables setup completed"}

def handle_remove_workflow(event: dict[str, Any]) -> dict[str, Any]:
    workflow_id = event.get("workflow_id", None)
    if workflow_id is None:
        logger.error("No workflow_id specified")
        return {"status": 400, "message": "No workflow_id specified"}
    
    client = Client(workflow_id)
    client.remove()

    return {"status": 200, "message": f"Workflow {workflow_id} removed"}

def handle_manage_deployments(event: dict[str, Any]) -> dict[str, Any]:
    deployment_manager = DeploymentManager()
    deployment_manager.check()
    return {"status": 200, "message": "Deployment check completed"}

def handle_log_sync(event: dict[str, Any]) -> dict[str, Any]:
    log_syncer = LogSyncer()
    log_syncer.sync()
    return {"status": 200, "message": "Log sync completed"}

def handle_data_collect(event: dict[str, Any]) -> dict[str, Any]:
    collector = event.get("collector", None)
    workflow_id = event.get("workflow_id", None)

    if collector is None:
        logger.error("No collector specified")
        return {"status": 400, "message": "No collector specified"}
    elif collector not in ("provider", "carbon", "performance", "workflow", "all"):
        logger.error("Invalid collector specified, Allowed values are provider, carbon, performance, workflow, all")
        return {
            "status": 400, 
            "message": "Invalid collector specified, Allowed values are provider, carbon, performance, workflow, all"
        }
    
    if collector in ("provider", "all"):
        provider_collector = ProviderCollector()
        provider_collector.run()
    if collector in ("carbon", "all"):
        carbon_collector = CarbonCollector()
        carbon_collector.run()
    if collector in ("performance", "all"):
        performance_collector = PerformanceCollector()
        performance_collector.run()
    if collector in ("workflow", "all"):
        if workflow_id is None:
            logger.error("Workflow_id must be provided for the workflow and all collectors.")
            return {"status": 400, "message": "Workflow_id must be provided for the workflow and all collectors."}
        workflow_collector = WorkflowCollector()
        workflow_collector.run_on_workflow(workflow_id)

    return {"status": 200, "ran_collector": collector, "workflow_id": workflow_id}

def handle_list_caribou_version(event: dict[str, Any]) -> dict[str, Any]:
    return {"status": 200, "version": MULTI_X_SERVERLESS_VERSION}

def handle_list_workflows(event: dict[str, Any]) -> dict[str, Any]:
    client = Client()
    workflows = client.list_workflows()
    return {"status": 200, "workflows": workflows}

def handle_run(event: dict[str, Any]) -> dict[str, Any]:
    workflow_id = event.get("workflow_id", None)
    argument = event.get("argument", None)

    if workflow_id:
        client = Client(workflow_id)
        logger.info(f"Running workflow with ID: {workflow_id}")
        run_id: str
        if argument:
            run_id = client.run(argument)
        else:
            run_id = client.run()

    return {"status": 200, "run_id": run_id}

def handle_default(event: dict[str, Any]) -> dict[str, Any]:
    print("Unknown action")
    return {"status": 400, "message": "Unknown action"}
