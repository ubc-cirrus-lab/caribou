# Component Interaction

The components of the system interact with each other in a specific order.
As well as over defined interfaces.
The following section will outline the order in which the components interact with each other and the interfaces that are used for the interaction.

## Component Interaction Order

![Component Interaction Order](./img/component_interaction_overview.png)

The following is the order in which the different components interact with each other:

1. The Deployment Utility uploads an initial version of the [Workflow Placement Decision](#workflow-placement-decision), the [Deployment Manager Resource](#deployment-manager-resource) of this workflow, as well as the [Workflow Config](#workflow-config) for the Deployment Manager to the corresponding tables in the distributed key-value store.
It aditionally uploads the [Deployment Package](#deployment-package) (source code) of the workflow to the distributed blob store.
2. The Deployment Manager is informed of a new workflow to be solved.
3. The Deployment Manager triggers the Deployment Solver to solve the workflow.
4. The Deployment Solver updates the workflow placement decision with the current placement of the function instances in a staging distributed key-value store.
5. The Deployment Migrator checks the staging distributed key-value store for updates to the workflow placement decision and re-deploys function instances if necessary.
6. The Deployment Manager uploads the updated workflow placement decision to the distributed key-value store.

During a workflow invocation, the initial function instance will download the workflow placement decision from the distributed key-value store and add the `run_id`.
Subsequent functions will receive the workflow placement decision from the previous function instance which updated the `current_instance_name` to the name of the current function instance.

## Workflow Placement Decision

The workflow placement decision is a dictionary of information with regards to current function instance placement.
This information is used both to determine the current function instance, as well as to determine where the next function instance is to be called.
Additonally, this information is used to deploy functions to new regions and providers.
The dictionary contains the following information:

```json
{
  "run_id": "test_run_id",
  "workflow_placement": {
    "current_deployment": {
      "instances": {
        "function_name:entry_point:0": {
          "provider_region": {
            "provider": "aws",
            "region": "region"
          },
          "identifier": "test_identifier",
          "function_identifier": "test_function_identifier"
        },
        "function_name_2:function_name_0_0:1": {
          "provider_region": {
            "provider": "aws",
            "region": "region"
          },
          "identifier": "test_identifier",
          "function_identifier": "test_function_identifier"
        },
      },
      "metrics": {
        ...
      },
      "expiry_time": "2021-05-01T00:00:00"
    },
    "home_deployment": {
      "instances": { ... },
      "metrics": { ... },
    }
  },
  "current_instance_name": "function_name:entry_point:0",
  "instances": {
    "workflow_id-function_name:entry_point:0": {
      "instance_name": "workflow_id-function_name:entry_point:0",
      "succeeding_instances": ["function_name_2:function_name_0_0:1"],
      "preceding_instances": [],
      "dependent_sync_predecessors": [],
    },
    "function_name_2:function_name_0_0:1": {
      "instance_name": "function_name_2:function_name_0_0:1",
      "succeeding_instances": [],
      "preceding_instances": ["function_name:entry_point:0"],
      "dependent_sync_predecessors": [],
    }
  }
}
```

Different parts of this dictionary are provided by different components of the system.

- The `run_id` is set by the initial function of the workflow.
- The `workflow_placement` is set by the Deployment Solver and contains the current placement of the function instances.
  - The `provider_region` is either set at initial deployment or is set by the Deployment Solver  in the staging area and moved over at deployment and contains the provider and region of the function instance.
  - The `identifier` is a unique identifier for the messaging queue instance at a provider.
    This is used to identify the calling point of the function instance.
    This is provided and updated by the deployment utilities.
  - The `function_identifier` is a unique identifier for the function instance.
- The `current_instance_name` is initially set by the deployment utilities as the name of the entry point function.
  It is then updated by the function instances to identify the successor function instance.
- The `instances` is set and updated by the deployment utilities and contains the information about the workflow DAG.
  - The `instance_name` is the name of the function instance.
  - The `function_name` is the name of the function.
    This name always contains the provider and region of the function that this instance is deployed to.
    Thus this also has to be updated upon re-deployment.
  - The `succeeding_instances` is a list of the names of the succeeding function instances.
  - The `preceding_instances` is a list of the names of the preceding function instances.
  - The `dependent_sync_predecessors` is a list of the names of the synchronization nodes that the function instance is required to update in case of a conditional call.

## Deployment Manager Resource

The deployment manager resource is a dictionary of information with regards to the deployment of the workflow.
This is used by the deployment utilities to facilitate the re-deployment of function instances.
The structure of the deployment manager resource is as follows:

```json
{
  "workflow_id": "test_workflow_id",
  "workflow_function_descriptions": "[{\"name\": \"workflow_id-function_name_2_provider-region\", \"entry_point\":...}]",
  "deployment_config": "{\"workflow_name\": \"workflow_name\",...}",
  "deployed_regions": "{\"workflow_id-function_name_2_provider-region\": {\"provider\": \"provider\", \"region\": \"region\"},...}",
}
```

Different parts of this dictionary are provided by different components of the system.

- The `workflow_id` is set at initial deployment by the deployment utilities.
  It is the concatenation of the workflow name and version.
- The `workflow_function_descriptions` is a list of functions that are part of the workflow.
  It is set at initial deployment by the deployment utilities.
  At re-deployment, if a function was re-deployed to a new region, the new function (new in the sense that the name is updated to the new provider and region) is added to the list.
- The `deployment_config` is the deployment config of the workflow.
  It is set at initial deployment by the deployment utilities.
- The `deployed_regions` is a dictionary of the deployed regions of the workflow.
  It is set at initial deployment by the deployment utilities.
  At re-deployment, if a function was re-deployed to a new region, the new region is added to the dictionary.
  This is theoretically not necessary as the information is also contained in the `workflow_function_descriptions`, however, it is more efficient to have this information in a separate dictionary.

## Solver Manager Resource

The solver manager checks whether to trigger the Deployment Solver to solve the workflow.
The information required for this component is stored in the `solver_update_checker_resources_table`.
The structure of the solver manager resource is as follows:

```json
{
  "workflow_id": "image_processing_light-0.0.1", 
  "workflow_config": "{\"workflow_name\": ..."
}
```

This information is set by the deployment client and is used by the Deployment Manager to determine whether to trigger the Deployment Solver to solve the workflow.

## Workflow Config

The workflow config is a dictionary of information with regards to the workflow.
An example of the structure of the workflow config is as follows:

```json
{
  "workflow_name": "image_processing_light",
  "workflow_version": "0.0.1",
  "workflow_id": "image_processing_light-0.0.1",
  "instances": {
    "image_processing_light-0_0_1-GetInput:entry_point:0": {
      "instance_name": "image_processing_light-0_0_1-GetInput:entry_point:0",
      "regions_and_providers": {
        "allowed_regions": [
          { "provider": "aws", "region": "us-east-1" },
          { "provider": "aws", "region": "us-east-2" },
          { "provider": "aws", "region": "us-west-1" },
          { "provider": "aws", "region": "us-west-2" }
        ],
        "disallowed_regions": null,
        "providers": { "aws": { "config": { "timeout": 60, "memory": 128 } } }
      },
      "succeeding_instances": [
        "image_processing_light-0_0_1-Flip:image_processing_light-0_0_1-GetInput_0_0:1"
      ],
      "preceding_instances": [],
      "dependent_sync_predecessors": []
    },
    "image_processing_light-0_0_1-Flip:image_processing_light-0_0_1-GetInput_0_0:1": {
      "instance_name": "image_processing_light-0_0_1-Flip:image_processing_light-0_0_1-GetInput_0_0:1",
      "regions_and_providers": {
        "allowed_regions": [
          { "provider": "aws", "region": "us-east-1" },
          { "provider": "aws", "region": "us-east-2" },
          { "provider": "aws", "region": "us-west-1" },
          { "provider": "aws", "region": "us-west-2" }
        ],
        "disallowed_regions": null,
        "providers": { "aws": { "config": { "timeout": 60, "memory": 128 } } }
      },
      "succeeding_instances": [],
      "preceding_instances": [
        "image_processing_light-0_0_1-GetInput:entry_point:0"
      ],
      "dependent_sync_predecessors": []
    }
  },
  "home_region": { "provider": "aws", "region": "us-east-1" },
  "estimated_invocations_per_month": 1000000,
  "constraints": {
    "hard_resource_constraints": {
      "cost": { "type": "absolute", "value": 1000 },
      "runtime": { "type": "absolute", "value": 1000 },
      "carbon": { "type": "absolute", "value": 1000 }
    },
    "soft_resource_constraints": {
      "cost": null,
      "runtime": null,
      "carbon": null
    },
    "priority_order": ["cost", "runtime", "carbon"]
  },
  "regions_and_providers": {
    "allowed_regions": [
      { "provider": "aws", "region": "us-east-1" },
      { "provider": "aws", "region": "us-east-2" },
      { "provider": "aws", "region": "us-west-1" },
      { "provider": "aws", "region": "us-west-2" }
    ],
    "disallowed_regions": [],
    "providers": { "aws": { "config": { "timeout": 60, "memory": 128 } } }
  },
  "num_calls_in_one_month": 0,
  "solver": ""
}
```

## Deployment Package

The deployment package is the source code of the workflow.
It is packaged as a zip file.
And is uploaded to the distributed blob store.
