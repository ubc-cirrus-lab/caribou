import pathlib
from datetime import timezone

# Caribou Wrapper Constants
## Determines the percentage of requests ALWAYS
## being sent to the home region (Entire workflow)
HOME_REGION_THRESHOLD = 0.1  # 10% of the time run in home region

# Configure max hops from client
## If the request has been forwarded or gone through X hops from Client, it is dropped
MAXIMUM_HOPS_FROM_CLIENT_REQUEST = 20

# Workflow Placement Tables
WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE = "workflow_placement_solver_staging_area_table"
WORKFLOW_PLACEMENT_DECISION_TABLE = "workflow_placement_decision_table"

# Solver Tables
DEPLOYMENT_MANAGER_RESOURCE_TABLE = "deployment_manager_resource_table"

# Solver Update Checker Tables
DEPLOYMENT_MANAGER_WORKFLOW_INFO_TABLE = "deployment_manager_workflow_info_table"

# Deployment Manager Tables
DEPLOYMENT_RESOURCES_TABLE = "deployment_resources_table"
DEPLOYMENT_RESOURCES_BUCKET = "caribou-deployment-resources"

# Syncronization Node Tables
SYNC_MESSAGES_TABLE = "sync_messages_table"
SYNC_PREDECESSOR_COUNTER_TABLE = "sync_predecessor_counter_table"

# Image names
CARIBOU_WORKFLOW_IMAGES_TABLE = "caribou_workflow_images_table"

# Global System Region
GLOBAL_SYSTEM_REGION = "us-west-2"

# Remote CLI Information (Eg. Function, repo name, policy name, etc.)
REMOTE_CARIBOU_CLI_FUNCTION_NAME = "caribou_cli"
REMOTE_CARIBOU_CLI_IAM_POLICY_NAME = "caribou_deployment_policy"

# Integration Test System Region
INTEGRATION_TEST_SYSTEM_REGION = "rivendell"

# Data Collector Tables
## General Tables
AVAILABLE_REGIONS_TABLE = "available_regions_table"

## Carbon Tables
CARBON_REGION_TABLE = "carbon_region_table"

## Performance Tables
PERFORMANCE_REGION_TABLE = "performance_region_table"

## Provider Tables
PROVIDER_REGION_TABLE = "provider_region_table"
PROVIDER_TABLE = "provider_table"

## Workflow Tables
WORKFLOW_INSTANCE_TABLE = "workflow_instance_table"

# Database Syncer Tables
WORKFLOW_SUMMARY_TABLE = "workflow_summary_table"

# Solver Input (Loader) Default Values
## Carboon Loader
### Default to Average USA Carbon Intensity of Electric Grid
### Contiguous United States Carbon intensity of energy grid
### of Consumption in 2023 according to Electric Maps - gCO2e/kWh
SOLVER_INPUT_GRID_CARBON_DEFAULT = 410

## Datacenter Loader
SOLVER_INPUT_AVERAGE_MEMORY_POWER_DEFAULT = 0.0003725
SOLVER_INPUT_PUE_DEFAULT = 1.11
SOLVER_INPUT_CFE_DEFAULT = 0.0
SOLVER_INPUT_COMPUTE_COST_DEFAULT = 1.66667e-05  # of x86_64 architecture Ohio region
SOLVER_INPUT_INVOCATION_COST_DEFAULT = 2e-07  # of x86_64 architecture Ohio region
SOLVER_INPUT_TRANSMISSION_COST_DEFAULT = 0.09  # Global data transfer cost
SOLVER_INPUT_MIN_CPU_POWER_DEFAULT = 0.00074
SOLVER_INPUT_MAX_CPU_POWER_DEFAULT = 0.0035
SOLVER_INPUT_SNS_REQUEST_COST_DEFAULT = 0.50 / 1000000  # 0.50 USD per 1 million requests (At Ohio region)
SOLVER_INPUT_DYNAMODB_READ_COST_DEFAULT = 0.25 / 1000000  # 0.25 USD per 1 million read request unit (At Ohio region)
SOLVER_INPUT_DYNAMODB_WRITE_COST_DEFAULT = 1.25 / 1000000  # 1.25 USD per 1 million write request unit (At Ohio region)
SOLVER_INPUT_ECR_MONTHLY_STORAGE_COST_DEFAULT = 0.10  # 0.10 USD per 1 GB per month (At Ohio region)

## Performance Loader
SOLVER_INPUT_RELATIVE_PERFORMANCE_DEFAULT = 1.0

# Future TODO: Change this to a more accurate value via a benchmark,
# this should be the average transmission latency for intra region transmission.
SOLVER_HOME_REGION_TRANSMISSION_LATENCY_DEFAULT = 0.22

## Workflow Loader
SOLVER_INPUT_RUNTIME_DEFAULT = -1.0  # Denotes that the runtime is not available
SOLVER_INPUT_LATENCY_DEFAULT = -1.0  # Denotes that the latency is not available

SOLVER_INPUT_DATA_TRANSFER_SIZE_DEFAULT = 0.0
SOLVER_INPUT_INVOCATION_PROBABILITY_DEFAULT = 0.0  # If it is missing, the invocation is never called in the workflow
SOLVER_INPUT_PROJECTED_MONTHLY_INVOCATIONS_DEFAULT = 0.0
SYNC_SIZE_DEFAULT = 1.0 / 1024**2  # 1 KB in GB
SNS_SIZE_DEFAULT = 1.0 / 1024**2  # 1 KB in GB


SOLVER_INPUT_VCPU_DEFAULT = -1.0  # Denotes that the vCPU is not available
SOLVER_INPUT_ARCHITECTURE_DEFAULT = "x86_64"

# Carbon Transmission Cost Calculator Constants
DFM = 7.564e-6
DFI = 5.762e-3

# Deployment Optimization Monitor Constants
CARBON_INTENSITY_TO_INVOCATION_SECOND_ESTIMATE = 0.001
COARSE_GRAINED_DEPLOYMENT_ALGORITHM_CARBON_PER_INSTANCE_INVOCATION_ESTIMATE = 0.00001
STOCHASTIC_HEURISTIC_DEPLOYMENT_ALGORITHM_CARBON_PER_INSTANCE_INVOCATION_ESTIMATE = 0.0001
MIGRATION_COST_ESTIMATE = 0.0001
DEFAULT_MONITOR_COOLDOWN = 60 * 60 * 24
MINIMAL_SOLVE_THRESHOLD = 10
DISTANCE_FOR_POTENTIAL_MIGRATION = 4000

# Logging
LOG_VERSION = "0.0.4"

# Tail latency threshold
TAIL_LATENCY_THRESHOLD = 95

# Average USA Carbon Intensity of Electric Grid
## Contiguous United States Carbon intensity of energy grid
## of Consumption in 2023 according to Electric Maps - gCO2e/kWh
AVERAGE_USA_CARBON_INTENSITY = 410

# datetime
GLOBAL_TIME_ZONE = timezone.utc
TIME_FORMAT = "%Y-%m-%d %H:%M:%S,%f%z"
TIME_FORMAT_DAYS = "%Y-%m-%d%z"

# Log-Syncer parameters
## Forgetting factors
FORGETTING_TIME_DAYS = 30  # 30 days
FORGETTING_NUMBER = 5000  # 5000 invocations
KEEP_ALIVE_DATA_COUNT = 10  # Keep sample it is part of any of the 10 samples for any execution or transmission
MIN_TIME_BETWEEN_SYNC = 15  # In Minutes

## Grace period for the log-syncer
## Used as lambda insights can be delayed
BUFFER_LAMBDA_INSIGHTS_GRACE_PERIOD = 15  # In minutes

## Successor task types
REDIRECT_ONLY_TASK_TYPE = "REDIRECT_ONLY"
INVOKE_SUCCESSOR_ONLY_TASK_TYPE = "INVOKE_SUCCESSOR_ONLY"
SYNC_UPLOAD_AND_INVOKE_TASK_TYPE = "SYNC_UPLOAD_AND_INVOKE"
SYNC_UPLOAD_ONLY_TASK_TYPE = "SYNC_UPLOAD_ONLY"
CONDITIONALLY_NOT_INVOKE_TASK_TYPE = "CONDITIONALLY_NOT_INVOKE"

# Caribou Wrapper parameters
## max workers for async invocations
MAX_WORKERS = 1

## Orchastration transfer size limitation
MAX_TRANSFER_SIZE = 256000  # In bytes


# Caribou Go Path
GO_PATH = pathlib.Path(__file__).parents[2].resolve() / "caribou-go"

# AWS Lambda Timeout
AWS_TIMEOUT_SECONDS = (
    800  # Lambda functions must terminate in 900 seconds, we leave some time as buffer time (For other operations)
)

# TTL for dynamodb synchronization tables
SYNC_TABLE_TTL_ATTRIBUTE_NAME = "cb_ttl_expiration_time"
SYNC_TABLE_TTL = 86400  # Equivalent to 1 day (24 hours) in seconds
