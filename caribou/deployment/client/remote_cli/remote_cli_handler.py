import logging
from typing import Any, Optional

from caribou.data_collector.components.carbon.carbon_collector import CarbonCollector
from caribou.data_collector.components.performance.performance_collector import PerformanceCollector
from caribou.data_collector.components.provider.provider_collector import ProviderCollector
from caribou.data_collector.components.workflow.workflow_collector import WorkflowCollector
from caribou.deployment.client import __version__ as CARIBOU_VERSION
from caribou.endpoint.client import Client
from caribou.monitors.deployment_manager import DeploymentManager
from caribou.monitors.deployment_migrator import DeploymentMigrator
from caribou.syncers.log_syncer import LogSyncer

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logging level


def caribou_cli(event: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:  # pylint: disable=unused-argument
    action = event.get("action", None)
    if not action:
        logger.error("No action specified")
        return {"status": 400, "message": "No action specified"}

    logger.info("Received request with action: %s", action)
    action_handlers = {
        "run": handle_run,
        "list": handle_list_workflows,
        "version": handle_list_caribou_version,
        "data_collect": handle_data_collect,
        "log_sync": handle_log_sync,
        "manage_deployments": handle_manage_deployments,
        "remove": handle_remove_workflow,
        "run_deployment_migrator": handle_run_deployment_migrator,
    }

    handler = action_handlers.get(action, handle_default)
    return handler(event)


def handle_run_deployment_migrator(event: dict[str, Any]) -> dict[str, Any]:  # pylint: disable=unused-argument
    function_deployment_monitor = DeploymentMigrator()
    function_deployment_monitor.check()

    return {"status": 200, "message": "Deployment migrator completed"}


def handle_remove_workflow(event: dict[str, Any]) -> dict[str, Any]:
    workflow_id = event.get("workflow_id", None)
    if workflow_id is None:
        logger.error("No workflow_id specified")
        return {"status": 400, "message": "No workflow_id specified"}

    client = Client(workflow_id)
    client.remove()

    return {"status": 200, "message": f"Workflow {workflow_id} removed"}


def handle_manage_deployments(event: dict[str, Any]) -> dict[str, Any]:
    deployment_metrics_calculator_type: str = event.get("deployment_metrics_calculator_type", "simple")

    if deployment_metrics_calculator_type not in ("simple", "go"):
        logger.error("Invalid deployment_metrics_calculator_type specified. Allowed values are 'simple', 'go'")
        return {
            "status": 400,
            "message": "Invalid deployment_metrics_calculator_type specified. Allowed values are 'simple', 'go'",
        }

    logger.info("Deployment check started, using %s calculator", deployment_metrics_calculator_type)
    # Declare the deployment manager with parameter of lambda_timeout=True (This
    # sets an early termination for the deployment manager to avoid lambda timeout)
    deployment_manager = DeploymentManager(deployment_metrics_calculator_type, lambda_timeout=True)

    deployment_manager.check()
    return {
        "status": 200,
        "message": f"Deployment check completed, using {deployment_metrics_calculator_type} calculator",
    }


def handle_log_sync(event: dict[str, Any]) -> dict[str, Any]:  # pylint: disable=unused-argument
    log_syncer = LogSyncer()
    log_syncer.sync()
    return {"status": 200, "message": "Log sync completed"}


def handle_data_collect(event: dict[str, Any]) -> dict[str, Any]:
    collector = event.get("collector", None)
    workflow_id = event.get("workflow_id", None)

    if collector is None:
        logger.error("No collector specified")
        return {"status": 400, "message": "No collector specified"}
    if collector not in ("provider", "carbon", "performance", "workflow", "all"):
        logger.error("Invalid collector specified, Allowed values are provider, carbon, performance, workflow, all")
        return {
            "status": 400,
            "message": "Invalid collector specified, Allowed values are provider, carbon, performance, workflow, all",
        }

    logger.info("Performing data collection for collector: %s", collector)

    if collector in ("provider", "all"):
        provider_collector = ProviderCollector()
        provider_collector.run()
    if collector in ("carbon", "all"):
        carbon_collector = CarbonCollector()
        carbon_collector.run()
    if collector in ("performance", "all"):
        performance_collector = PerformanceCollector()
        performance_collector.run()
    if collector in ("workflow"):
        if workflow_id is None:
            logger.error("Workflow_id must be provided for the workflow collector.")
            return {"status": 400, "message": "Workflow_id must be provided for the workflow collector."}
        workflow_collector = WorkflowCollector()
        workflow_collector.run_on_workflow(workflow_id)

    return {"status": 200, "ran_collector": collector, "workflow_id": workflow_id}


def handle_list_caribou_version(event: dict[str, Any]) -> dict[str, Any]:  # pylint: disable=unused-argument
    return {"status": 200, "version": CARIBOU_VERSION}


def handle_list_workflows(event: dict[str, Any]) -> dict[str, Any]:  # pylint: disable=unused-argument
    client = Client()
    workflows = client.list_workflows()
    return {"status": 200, "workflows": workflows}


def handle_run(event: dict[str, Any]) -> dict[str, Any]:
    workflow_id = event.get("workflow_id", None)
    argument = event.get("argument", None)

    run_id: Optional[str] = None
    if workflow_id:
        client = Client(workflow_id)
        logger.info("Running workflow with ID: %s", workflow_id)

        if argument:
            run_id = client.run(argument)
        else:
            run_id = client.run()

    return {"status": 200, "run_id": run_id}


def handle_default(event: dict[str, Any]) -> dict[str, Any]:  # pylint: disable=unused-argument
    logger.error("Unknown action")
    return {"status": 400, "message": "Unknown action"}
