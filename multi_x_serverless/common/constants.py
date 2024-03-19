import pytz

# Workflow Placement Tables
WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE = "workflow_placement_solver_staging_area_table"
WORKFLOW_PLACEMENT_DECISION_TABLE = "workflow_placement_decision_table"

# Solver Tables
DEPLOYMENT_OPTIMIZATION_MONITOR_RESOURCE_TABLE = "deployment_optimization_monitor_resource_table"

# Solver Update Checker Tables
DEPLOYMENT_OPTIMIZATION_MONITOR_WORKFLOW_INFO_TABLE = "deployment_optimization_monitor_workflow_info_table"

# Deployment Manager Tables
DEPLOYMENT_MANAGER_RESOURCE_TABLE = "deployment_manager_resources_table"

# Syncronization Node Tables
SYNC_MESSAGES_TABLE = "sync_messages_table"
SYNC_PREDECESSOR_COUNTER_TABLE = "sync_predecessor_counter_table"

# Image names
MULTI_X_SERVERLESS_WORKFLOW_IMAGES_TABLE = "multi_x_serverless_workflow_images_table"

# Code Resources Bucket
MULTI_X_SERVERLESS_CODE_RESOURCES_BUCKET = "multi-x-serverless-code-resource-bucket"

# Global System Region
GLOBAL_SYSTEM_REGION = "us-west-2"

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
SOLVER_INPUT_GRID_CARBON_DEFAULT = 500.0

## Datacenter Loader
SOLVER_INPUT_AVERAGE_CPU_POWER_DEFAULT = 100.0
SOLVER_INPUT_AVERAGE_MEMORY_POWER_DEFAULT = 100.0
SOLVER_INPUT_PUE_DEFAULT = 1.0
SOLVER_INPUT_CFE_DEFAULT = 0.0
SOLVER_INPUT_COMPUTE_COST_DEFAULT = 100.0
SOLVER_INPUT_INVOCATION_COST_DEFAULT = 100.0
SOLVER_INPUT_TRANSMISSION_COST_DEFAULT = 100.0

## Performance Loader
SOLVER_INPUT_RELATIVE_PERFORMANCE_DEFAULT = 1.0
SOLVER_INPUT_TRANSMISSION_LATENCY_DEFAULT = 1000.0

## Workflow Loader
SOLVER_INPUT_RUNTIME_DEFAULT = -1.0  # Denotes that the runtime is not available
SOLVER_INPUT_LATENCY_DEFAULT = -1.0  # Denotes that the latency is not available

SOLVER_INPUT_DATA_TRANSFER_SIZE_DEFAULT = 0.0
SOLVER_INPUT_INVOCATION_PROBABILITY_DEFAULT = 0.0  # If it is missing, the invocation is never called in the workflow
SOLVER_INPUT_PROJECTED_MONTHLY_INVOCATIONS_DEFAULT = 0.0

SOLVER_INPUT_VCPU_DEFAULT = -1.0  # Denotes that the vCPU is not available
SOLVER_INPUT_ARCHITECTURE_DEFAULT = "x86_64"

# Carbon Transmission Cost Calculator Constants
# KWH_PER_GB_ESTIMATE = 0.1
KWH_PER_KM_GB_ESTIMATE = 0.005
CARBON_TRANSMISSION_CARBON_METHOD = "latency"  # Or latency
KWH_PER_S_GB_ESTIMATE = 0.005

# Deployment Optimization Monitor Constants
CARBON_INTENSITY_TO_INVOCATION_SECOND_ESTIMATE = 0.001
COARSE_GRAINED_DEPLOYMENT_ALGORITHM_CARBON_PER_INSTANCE_INVOCATION_ESTIMATE = 0.00001
STOCHASTIC_HEURISTIC_DEPLOYMENT_ALGORITHM_CARBON_PER_INSTANCE_INVOCATION_ESTIMATE = 0.0001
MIGRATION_COST_ESTIMATE = 0.0001
DEFAULT_MONITOR_COOLDOWN = 60 * 60 * 24
MINIMAL_SOLVE_THRESHOLD = 10
DISTANCE_FOR_POTENTIAL_MIGRATION = 4000

# Logging
LOG_VERSION = "0.0.3"

# Tail latency threshold
TAIL_LATENCY_THRESHOLD = 95

# datetime
GLOBAL_TIME_ZONE = pytz.utc
TIME_FORMAT = "%Y-%m-%d %H:%M:%S,%f%z"
TIME_FORMAT_DAYS = "%Y-%m-%d%z"

# Forgetting factors
FORGETTING_TIME_DAYS = 30  # 30 days
FORGETTING_NUMBER = 5000  # 5000 invocations
KEEP_ALIVE_DATA_COUNT = 10  # Keep sample it is part of any of the 10 samples for any execution or transmission
