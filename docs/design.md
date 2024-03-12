# Design Document

In this document we will discuss the design decisions that have been made for the project.

## Table of Contents

1. [Dataflow DAG Model](#dataflow-dag-model)
   1. [Physical Representation](#physical-representation)
   2. [Logical Representation](#logical-representation)
      1. [Logical Node Naming Scheme](#logical-node-naming-scheme)
      2. [Connection to Physical Representation](#connection-to-physical-representation)
   3. [Discussion](#discussion)
2. [Source Code Annotation](#source-code-annotation)
3. [Synchronization Node](#synchronization-node)
    1. [Implementation](#implementation)
4. [Component Interaction](#component-interaction)
    1. [Component Interaction Order](#component-interaction-order)
    2. [Workflow Placement Decision](#workflow-placement-decision)
5. [Data Collectors](#data-collectors)
    1. [Provider Collector](#provider-collector)
    2. [Carbon Collector](#carbon-collector)
    3. [Performance Collector](#performance-collector)
    4. [Workflow Collector](#workflow-collector)
6. [Deployment Algorithm Inputs](#deployment-algorithm-inputs)
7. [Deployment Algorithms](#deployment-algorithms)
    1. [Coarse Grained](#coarse-grained)
    2. [Stochastic Heuristic Descent](#stochastic-heuristic-descent)
    3. [Fine Grained](#fine-grained)
8. [References](#references)

## Dataflow DAG Model

Each workflow has two representations in our model.
We distinguish between the physical and the logical representation of a workflow.
Following we will quickly go over these two representations and discuss why we chose this representation.

### Physical Representation

The physical representation is the actually deployed functions.
Physical in this sense referes to actual source code being deployed at a cloud provider.
This is the representation that the deployment utilities work with because the only thing which matters for them is which function source code is deployed in what region and at which provider.
The physical representation consists of a set of nodes with no edges:

$P_f = \{n_1, n_2, ..., n_n\}$

This representation is quite simple in our case as the source code for each function is the same, the full workflow folder, however, the handler name (the function name within the package) is different.

### Logical Representation

Our logical representation is, similar to a dataflow DAG [1], the representation of the data flow between the physical instances of our functions.
The dataflow DAG in our case represents calls from functions to other functions with specific input data (thus the name dataflow).
Thus the nodes represent functions or _computations_ and the edges represent data movement between the functions.

The logical representation consists of a set of nodes and a set of edges, where the nodes are of three different types:

- Initial node: The initial node is the node that is called by the user.
It has no incoming edges.
- Intermediate node: An intermediate node is a node that is called by another node.
It has exactly one incoming edge.
- Synchronization node: A synchronization node is a node that is called by multiple other nodes.
It has one or more incoming edges.

The types of nodes are known at deployment time from static code analysis and are not changed during the execution of the workflow.
Likweise, the edges are known at deployment time and are not changed during the execution of the workflow.
A node is represented by a call from one function to another by the corresponding workflow annotation.
See also the section on [Source Code Annotation](#source-code-annotation).
The logical representation is a directed acyclic graph (DAG) with nodes and edges:

$L_f = (N, E)$

where $N$ is the set of nodes and $E$ is the set of edges.

#### Logical Node Naming Scheme

The naming scheme of the nodes is as follows:

- Initial node: `<function_name>:entry_point:0`
- Intermediate node: `<function_name>:<predecessor_function_name>_<predecessor_index>_<successor_of_predecessor_index>:<index_in_dag>`
  Where `<predecessor_function_name>` is the name of the predecessor function, `<predecessor_index>` is the index of the predecessor function in the dag, `<successor_of_predecessor_index>` is the index of the successor of the predecessor function (when a function calls multiple times the same function, this index is used to distinguish between the different calls), and `<index_in_dag>` is the index of the node in the dag in a topological order of dataflow.
- Synchronization node: `<function_name>:sync:<index_in_dag>`

This naming scheme is used to uniquely identify a node in the logical representation.
The scheme has the implication that colons cannot be allowed in the function names.

#### Connection to Physical Representation

The logical representation is connected to the physical representation by the following rules:

- Each node in the logical representation is represented by a physical node, however, the same physical node might be present multiple times in the logical representation.

Thus we also speak of a _pyhsical node_ and its _logical instances_.

There are two exceptions with regards to physical nodes in the logical representation:

- The initial node is present only once in the logical representation.
- A synchronization node is present only once in the logical representation.

### Discussion

These representations gave been chosen because they are simple and easy to understand.
The logical representation is a DAG, which is a well known data structure and easy to work with.
The representation of a DAG was chosen because it opens the avenues for graph optimizations in the deployment algorithm.
We ended up choosing a graph instead of a multigraph because we do not need to represent multiple edges between two nodes.
The dataflow between the functions is furthermore directed and acyclic because we do not allow for loops in the workflow.

The physical representation is a set of nodes because we do not need to represent any edges between the nodes.
The physical representation is furthermore a set because we do not need to represent the same node multiple times.
This notation is only used for the deployment utilities and is hidden from the deployment algorithm.

## Source Code Annotation

In an initial version of the project we used a JSON config based approach towards transmitting the workflow information to the deployment utilities.
However, this approach decoupled the workflow from the source code and thus made it hard to reason about the workflow.
Furthermore, it made the static code analysis more complicated as we had to parse the JSON config file and match the functions to the config file.
Thus we decided to use source code annotations instead.
The source code annotations are used to annotate the functions that are part of the workflow.

The source code annotations allow us to easily reason about the workflow and to extract both the physical and the logical representation of the workflow.
There is still an additional configuration file that is used to transmit information such as environment variables and the name of the workflow to the deployment utilities.
However, this information is decoupled from the source code and thus does not affect the static code analysis.

Currently, we support the following annotations:

- At the beginning of the workflow the user has to register the workflow with the following annotation:

```python
workflow = MultiXServerlessWorkflow("workflow_name")
```

- At the beginning of each function the user has to register the function with the following annotation:

```python
@workflow.serverless_function(
    name="First-Function",
    entry_point=True,
    regions_and_providers={
        "allowed_regions": [
          {
            "provider": "aws",
            "region": "us-east-1",
          }
        ],
        "disallowed_regions": None,
        "providers": {
            "aws": {
                "config": {
                    "timeout": 60,
                    "memory": 128,
                },
            },
        },
    },
    environment_variables=[
        {
            "key": "example_key",
            "value": "example_value"
        }
    ],
)
```

The meaning of the different parameters is as follows:

- `name`: The name of the function.
This is the name that is used directly in the physical representation of the workflow and is also used to identify the function in the logical representation of the workflow (see also the section on [Logical Node Naming Scheme](#logical-node-naming-scheme)).
- `entry_point`: A boolean flag that indicates whether the function is the entry point of the workflow.
There can only be one entry point in a workflow.
- `regions_and_providers`: A dictionary that contains the regions and providers that the function can be deployed to.
This can be used to override the global settings in the `config.yml`.
If none or an empty dictionary is provided, the global config takes precedence.
The dictionary has two keys:
  - `allowed_regions`: A list of regions that the function can be deployed to.
  If this list is empty, the function can be deployed to any region.
  - `disallowed_regions`: A list of regions that the function cannot be deployed to.
  If this list is empty, the function can be deployed to any region.
  - `providers`: A list of providers that the function can be deployed to.
  This can be used to override the global settings in the `config.yml`.
  If a list of providers is specified at the function level this takes precedence over the global configurations.
  If none or an empty list is provided, the global config takes precedence.
  Each provider is a dictionary with two keys:
    - `name`: The name of the provider.
    - `config`: A dictionary that contains the configuration for the specific provider.
- `environment_variables`: This parameter represents a list of dictionaries, each designed for setting environment variables specifically for a function. Users must adhere to a structured format within each dictionary. This format requires two entries: "key" and "value". The "key" entry should contain the name of the environment variable, serving as an identifier. The "value" entry holds the corresponding value assigned to that variable.

- Within a function, a user can register a call to another function with the following annotation:

```python
workflow.invoke_serverless_function(second_function, payload)
```

The payload is a dictionary that contains the input data for the function.
The payload is optional and can be omitted if the function does not require any input data.

- Additionally, there is the option of conditionally calling a function.
This is done with the following annotation:

```python
workflow.invoke_serverless_function(second_function, payload, condition)
```

The condition is a boolean expression that is evaluated at runtime.
If the condition evaluates to true, the function is called, otherwise it is not called.

- Finally, there is the option of synchronizing multiple predecessor calls at a synchronization node.
The responses from the predecessor calls are then passed to the synchronization node as a list of responses.
This is done with the following annotation:

```python
responses: list[Any] = workflow.get_predecessor_data()
```

Using this annotation within a function has an important implication with regards to when the entire function is being executed.
The entire function is only executed once all predecessor calls have been completed and the data has been synchronized.
This is important to keep in mind when designing the workflow.
Any code within the function preceding the annotation is also executed only once all predecessor calls have been completed and the data has been synchronized.

## Synchronization Node

The synchronization nodes have a special semantic.
The definition of a synchronization node is that, compared to all other nodes, there are one or more predecessor nodes that call the synchronization node.
The synchronization node will receive the payload (responses) from all predecessor nodes and can then handle the responses according to the user defined logic.
This logic has an important implication for the logical representation of the DAG as it means that since the physical representation does not define on what specific predecessors we are waiting on the synchronization node waits for all predecessors and thus there can only be one logical instance of a synchronization node in the logical representation.
Hence also the slightly different naming scheme of the synchronization nodes (see also the section on [Logical Node Naming Scheme](#logical-node-naming-scheme)).

### Implementation

The logic of the synchronization node is implemented as follows:

1. When a predecessor calls a synchronization node, the predecessor will add its response to a list of responses in a distributed key-value store in the region of the synchronization node.
2. The predecessor will then atomically add its name to the list of predecessors that have called the synchronization node.
3. The new length of the list is then checked against the number of predecessors of the synchronization node.
4. If the counter is equal to the number of predecessors, the synchronization node will be called. Otherwise, the predecessor will not call the synchronization node.
This ensures that the synchronization node is only called when all predecessors have called the synchronization node.

A special consideration is made with regards to conditional calls to successors.
If a conditional call results in the predecessor not calling a successor, the predecessor knows whether any successor of the function not called would have called the synchronization node.
If this is the case, the predecessor will add the name of the corresponding successor to the list of predecessors that have called the synchronization node.
This ensures that the synchronization node is called even if some of the predecessors do not call the synchronization node due to conditional calls.

As previously mentioned, the code in the synchronization node is only executed once all predecessors have written their responses to the distributed key-value store and the counter has been incremented to the number of predecessors, i.e., the synchronization node is only called once all predecessors have called the synchronization node.

## Component Interaction

The components of the system interact with each other in a specific order.
As well as over defined interfaces.
The following section will outline the order in which the components interact with each other and the interfaces that are used for the interaction.

### Component Interaction Order

![Component Interaction Order](./img/component_interaction_overview.png)

The following is the order in which the different components interact with each other:

1. The deployment client uploads an initial version of the [Workflow Placement Decision](#workflow-placement-decision), the [Deployment Manager Resource](#deployment-manager-resource) of this workflow, as well as the [Workflow Config](#workflow-config) for the deployment algorithm update checker to the corresponding tables in the distributed key-value store.
It aditionally uploads the [Deployment Package](#deployment-package) (source code) of the workflow to the distributed blob store.
2. The deployment algorithm update checker is informed of a new workflow to be solved.
3. The deployment algorithm update checker triggers the deployment algorithm to solve the workflow.
4. The deployment algorithm updates the workflow placement decision with the current placement of the function instances in a staging distributed key-value store.
5. The deployment update checker checks the staging distributed key-value store for updates to the workflow placement decision and re-deploys function instances if necessary.
6. The deployment server uploads the updated workflow placement decision to the distributed key-value store.

During a workflow execution, the initial function instance will download the workflow placement decision from the distributed key-value store and add the `run_id`.
Subsequent functions will receive the workflow placement decision from the previous function instance which updated the `current_instance_name` to the name of the current function instance.

### Workflow Placement Decision

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
- The `workflow_placement` is set by the deployment algorithm and contains the current placement of the function instances.
  - The `provider_region` is either set at initial deployment or is set by the deployment algorithm  in the staging area and moved over at deployment and contains the provider and region of the function instance.
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

### Deployment Manager Resource

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

### Solver Manager Resource

The solver manager checks whether to trigger the deployment algorithm to solve the workflow.
The information required for this component is stored in the `solver_update_checker_resources_table`.
The structure of the solver manager resource is as follows:

```json
{
  "workflow_id": "image_processing_light-0.0.1", 
  "workflow_config": "{\"workflow_name\": ..."
}
```

This information is set by the deployment client and is used by the deployment algorithm update checker to determine whether to trigger the deployment algorithm to solve the workflow.

### Workflow Config

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

### Deployment Package

The deployment package is the source code of the workflow.
It is packaged as a zip file.
And is uploaded to the distributed blob store.

## Data Collectors

The Data Collectors are responsible for aggregating information needed by various instances of Solvers to solve for the optimal cost, carbon, and/or runtime.
This information is gathered through various different sources and both internal and external APIs.
This section will outline the various Data Collectors, their responsibilities, and also the format of their outputs.

![Data Collector High Level Architecture](./img/data_collector_architecture.png)

There are four main types of data collectors, each accessing different sources of input data, managing their own output databases, and running or triggering at different intervals and/or trigger conditions.
The four main collectors are the Provider Collector, Carbon Collector, Performance Collector, and Workflow Collector.

All the following tables are stored in a DynamoDB database.
In the following, we will outline the responsibilities of each Data Collector, the database tables they manage, and the information stored in each table.
The information stored in each table has one key and one or more values.
All keys are strings.
The type of the key and values are denoted by the following abbreviations:

- S: String
- N: Number

If a string type is used but it has multiple child values, the string is a JSON string.

### Provider Collector

The Provider Collector is responsible for collecting information through external APIs regarding the cost, location, and offered services of all available data center regions, as well as identifying which data center regions are currently available.
This collector is pivotal for determining the availability of data centers, and it must be run at least once before the other data collectors can be functional, as it sets data center regions to be put under consideration.
Since pricing information and offered services of data center regions rarely change, this Collector should be run very infrequently, likely once per week or longer.
The frequency with which this collector should run needs further investigation.

This collector is responsible for managing the three database tables: `available_regions_table`, `at_provider_table`, and `provider_region_table`.

#### Available Regions Table

The `available_regions_table` is responsible for managing the list of available regions that may be viable for consideration by Solver instances and denotes which regions must be updated by the other Collectors.
This table is primarily responsible for listing the basic information of regions needed for the other Data Collectors, as well as the timestamp of when the region was last updated by each Data Collector (with the exception of the Workflow Data Collector, as that is Workflow-specific).
The Provider Collector must not override the timestamp of other Data Collectors.
The keys and information stored in this table are as follows:

- Key: `<provider_unique_id>:<region_name>`
- Value (S):
  - Available services at the data center region.
  - The geographical location of the data center region (in longitude and latitude).
- Provider Collector timestamp (N):
  - Timestamp of when the Provider Collector was last run for this data center region.
- Carbon Collector timestamp (N):
  - Timestamp of when the Carbon Collector was last run for this data center region.
- Performance Collector timestamp (N):
  - Timestamp of when the Performance Collector was last run for this data center region.

##### Available Regions Table Example

The example will be one item in the available regions table with all its keys:

```json
{
  "key": "aws:eu-south-1",
  "provider_collector": 1620000000,
  "carbon_collector": 1620000000,
  "performance_collector": 1620000000,
  "value": {
    "name": "Europe (Milan)",
    "provider": "aws",
    "code": "eu-south-1",
    "latitude": 45.4642035,
    "longitude": 9.189982
  }
}
```

#### At Provider Table

The `provider_table` is responsible for managing information regarding provider-level information. The keys and information stored in this table are as follows:

- Key: `<provider_unique_id>`
- Value (S):
  - Remaining free tier at provider (invocation/execution).

##### At Provider Table Example

The example will be one item in the at provider table with all its keys:

```json
{
  "key": "aws",
  "value": {}
}
```

#### Provider Region Table

The `provider_region_table` is responsible for managing region specific information.
The keys and information stored in this table are as follows:

- Key: `<provider_unique_id>:<region_name>`
- Value (S):
  - Execution Cost:
    - Values for each configuration (or services).
  - Power-efficiency-related information (PUE, CFE).
  - Average Memory and CPU compute power.
  - Transmission Cost:
    - Egress Global Data Transfer Cost (outgoing to different providers).
    - Egress Provider Data Transfer Cost (outgoing to different regions within the same provider).

Note: Data Transfer Cost and complexities of this warrant further investigation and thus associated storage information regarding such may be subject to change.

##### Provider Region Table Example

```json
{
  "execution_cost": {
    "invocation_cost": {
      "arm64": 2.3e-7,
      "x86_64": 2.3e-7,
      "free_tier_invocations": 1000000
    },
    "compute_cost": {
      "arm64": 1.56138e-5,
      "x86_64": 1.95172e-5,
      "free_tier_compute_gb_s": 400000
    },
    "unit": "USD"
  },
  "transmission_cost": {
    "global_data_transfer": 0.09,
    "provider_data_transfer": 0.02,
    "unit": "USD/GB"
  },
  "pue": 1.15,
  "cfe": 0.9,
  "average_memory_power": 3.92e-6,
  "average_cpu_power": 0.00212,
  "available_architectures": ["arm64", "x86_64"]
}
```

### Carbon Collector

The Carbon Collector is responsible for calculating and refreshing carbon transmission and execution information for all available data center regions that are part of the `available_regions_table`, which was first populated by the Provider Collector.
Since carbon information changes frequently (where the Electric Maps API refreshes grid carbon information every hour), this collector may be run frequently, perhaps in the order of hours.
The frequency with which this collector should run needs further investigation.

This collector is responsible for managing the `carbon_region_table` database table.
It is also responsible for updating the timestamp of carbon-updated regions in the Carbon Collector timestamp field of the `available_regions_table` table.

#### Carbon Region Table

The `carbon_region_table` is responsible for managing carbon region-specific information. The keys and information stored in this table are as follows:

- Key: `<provider_unique_id>:<region_name>`
- Value (S):
  - Execution Carbon per kWh (gCO2e/kWh)
  - To Region `<provider_unique_id>:<region_name>`
    - Region-to-region Data Transfer Carbon Impact (gCO2e/GB)

Note: Perhaps this may be expanded in the future if we are incorporating more execution or transmission carbon estimation techniques.

##### Carbon Region Table Example

```json
{
  "carbon_intensity": 482,
  "unit": "gCO2eq/kWh",
  "transmission_carbon": {
    "aws:eu-south-1": {"carbon_intensity": 48.2, "unit": "gCO2eq/GB"},
    "aws:eu-central-1": {
      "carbon_intensity": 1337.9261964617801,
      "unit": "gCO2eq/GB"
    },
    "aws:us-west-2": {
      "carbon_intensity": 21269.19652594863,
      "unit": "gCO2eq/GB"
    },
    ...
  }
}
```

### Performance Collector

The Performance Collector is responsible for aggregating performance benchmarks to determine the relative performance differences of running workloads between different data center regions.
Similar to the Carbon Collector, this is only done for all available data center regions that are part of the `available_regions_table`, which was first populated by the Provider Collector.
Depending on the results of our investigation into the change in performance variability between data center regions and across providers, the frequency of this collector may need to be considered.
For now, this Collector should be run much more frequently than the Provider Collector but perhaps less frequently than the Carbon Collector.
Again, the frequency with which this collector should run needs further investigation.

This collector is responsible for managing the  `performance_region_table` database table.
It is also responsible for updating the timestamp of performance-updated regions in the Performance Collector timestamp field of the `available_regions_table` table.

#### Performance Region Table

The `performance_region_table` is responsible for managing performance region-specific information of our benchmarking application.
The keys and information stored in this table are as follows:

- Key: `<provider_unique_id>:<region_name>`
- Value (S):
  - Execution time of performance tests in various regions.
  - To Region `<provider_unique_id>:<region_name>`
    - Region-to-region Estimated latency in terms of data transfer size (s/GB).

Note: Perhaps in the future, we should also consider provider-level performance differences with a different database table.

##### Performance Region Table Example

```json
 {
    "relative_performance": 1,
    "transmission_latency": {
      "aws:region1": {"average_latency": 0.005, "tail_latency": 0.007, "unit": "s"},
      "aws:region2": {"average_latency": 0.05, "tail_latency": 0.07, "unit": "s"},
    },
},
```

### Workflow Collector

The Workflow Collector is responsible for aggregating runtime and invocation probability of each instance of one or more workflows and also includes the actual execution and transmission time of running workflow instances in a given region.
Similar to the Carbon and Performance Collector, it should only consider regions that are part of the `available_regions_table` and discard any information of workflow running in regions not in the available region list.
This Workflow Collector is different from the other collectors, as this Collector should not look at all workflows but perhaps for specific workflows that will soon need to have its deployment plan updated by the Solver.
This Collector should be run very frequently, and triggered by the Solver Update Checker.

This collector is responsible for managing the "workflow_instance_table" database table.
Unlike the other Data Collectors, the Workflow Collector should not and will not have or require updating any timestamp of the `available_regions_table` table.

#### Workflow Collector Input Table

The Workflow Collector is responsible for extracting information from the `workflow_summary_table`, which is managed by the Datastore Syncer. The Datastore Syncer should retrieve all the invocations log of the workflow from locally data centers and then remove the local entries only after finishing summarization.

##### Workflow Summary Table Example

Below is an example of the `workflow_summary_table` for a workflow with 2 instances. All the runtime and latency are in units of seconds.

```json
{
  "daily_invocation_counts": { "2024-03-09+0000": 15 },
  "logs": [
    {
      "run_id": "eca39262530c4033a9f29343a39d71ca",
      "runtime": 8.746771,
      "start_time": "2024-03-09 18:26:24,469750+0000",
      "execution_latencies": {
        "small_sync_example-0_0_1-initial_function:entry_point:0": {
            "latency": 7.561505556106567,
            "provider_region": "aws:us-east-1",
        },
        "small_sync_example-0_0_1-syncFunction:sync:": {
            "latency": 1.798128366470337,
            "provider_region": "aws:us-east-1",
        },
        "small_sync_example-0_0_1-secondSyncFunction:sync:": {
            "latency": 1.1149189472198486,
            "provider_region": "aws:us-east-1",
        }
      },
      "transmission_data": [
        {
          "transmission_size": 4.629604518413544e-6,
          "transmission_latency": 2.962404,
          "from_instance": "small_sync_example-0_0_1-initial_function:entry_point:0",
          "to_instance": "small_sync_example-0_0_1-syncFunction:sync:",
          "from_region": { "provider": "aws", "region": "us-east-1" },
          "to_region": { "provider": "aws", "region": "us-east-1" }
        },
        {
          "transmission_size": 4.641711711883545e-6,
          "transmission_latency": 1.536726,
          "from_instance": "small_sync_example-0_0_1-initial_function:entry_point:0",
          "to_instance": "small_sync_example-0_0_1-secondSyncFunction:sync:",
          "from_region": { "provider": "aws", "region": "us-east-1" },
          "to_region": { "provider": "aws", "region": "us-east-1" }
        }
      ],
      "start_hop_latency": 0.769591,
      "start_hop_data_transfer_size": 6.146728992462158e-8,
      "start_hop_destination": { "provider": "aws", "region": "us-east-1" }
    },
    ...
  ],
  "workflow_runtime_samples": [
    8.746771, ...
  ],
  "last_sync_time": "2024-03-09 18:52:40,671496+0000"
}
```

#### Workflow Collector Output Table

The `workflow_instance_table` is responsible for summarizing and collecting information regarding past instance invocation at various regions:

- Key: `<workflow_unique_id>`
- Value (S):
  - Workflow runtime samples `workflow_runtime_samples`.
  - Total number of invocations of the workflow:
    - From date `start_time`
    - To date `end_time`
    - Total number of invocations.
  - Start hop destination:
    - Corresponding start hop data transfer sizes:
      - Corresponding Start hop latency measurements.
  - Instance data:
    - At Instance `<instance_unique_id>`
      - Number of invocations of this instance.
      - At Region `<provider_unique_id>:<region_name>`:
        - Execution Latency samples.
      - To Instance `<instance_unique_id>`
        - Probability of At Instance invoking To Instance
        - At Region `<provider_unique_id>:<region_name>`
          - To Region `<provider_unique_id>:<region_name>`
            - Data Transfer samples.
              - Corresponding Transmission Latency samples.

##### Workflow Instance Table Example

Below is an example of the `workflow_instance_table` output for a workflow with 2 instances. All the runtime and latency are in units of seconds.

```json
{
  "workflow_runtime_samples": [5.857085, 5.740116, 7.248474],
  "daily_invocation_counts": { "2024-03-12+0000": 3 },
  "start_hop_summary": {
    "aws:us-east-1": { "3.3527612686157227e-08": [0.52388, 0.514119, 0.519146] }
  },
  "instance_summary": {
    "image_processing_light-0_0_1-GetInput:entry_point:0": {
      "invocations": 3,
      "executions": {
        "aws:us-east-1": [
          1.140042781829834, 1.129507303237915, 1.0891644954681396
        ]
      },
      "to_instance": {
        "image_processing_light-0_0_1-Flip:image_processing_light-0_0_1-GetInput_0_0:1": {
          "invoked": 3,
          "regions_to_regions": {
            "aws:us-east-1": {
              "aws:us-east-1": {
                "2.9960647225379944e-06": [1.217899, 1.18531, 1.174224]
              }
            }
          },
          "non_executions": 0,
          "invocation_probability": 1.0
        }
      }
    },
    "image_processing_light-0_0_1-Flip:image_processing_light-0_0_1-GetInput_0_0:1": {
      "invocations": 3,
      "executions": {
        "aws:us-east-1": [
          4.638583183288574, 4.554178953170776, 6.073627948760986
        ]
      },
      "to_instance": {}
    }
  }
}
```

## Deployment Algorithm Inputs

Solver Inputs is a subcomponent of Solver responsible for providing input to the Solver.
It serves as an interface for the deployment algorithm instances to obtain execution and transmission data.
It accesses the necessary information from tables created and managed by the Data Collectors, including [`workflow_instance_table`](#workflow-collector-output-table), [`performance_region_table`](#performance-region-table), [`carbon_region_table`](#carbon-region-table), [`available_regions_table`](#available-regions-table), [`provider_region_table`](#provider-region-table), and [`provider_table`](#at-provider-table).

It consists of the `InputManager`, responsible for managing and setting up all appropriate loaders and calculators.
The Solver directly interacts with this component.
The data loaders include the `RegionViabilityLoader`, `DatacenterLoader`, `CarbonLoader`, `PerformanceLoader`, and `WorkflowLoader`.
Each is responsible for accessing one or more tables in a database, acquiring only the needed sets of data from each table to minimize database access cost.
The data calculators, including the "Runtime Calculator", "Cost Calculator", and "Carbon Calculator", are responsible for accessing data from the necessary data loaders and/or other calculators in order to calculate the execution and transmission cost/carbon/runtime.

Below is a diagram showing the overall access of data in the Solver Inputs:

![Solver Input Data Flow](./img/solver_input_architecture.png)

## Deployment Algorithms

The deployment algorithms are responsible for determining the optimal placement of the function instances across the available regions.
Every deployment algorithm must create valid and unique deployments.
A valid deployment is one that satisfies the hard constraints of the workflow in terms of resource requirements for the worst-case runtime (tail).
The provided constraints will then be valid as quality of service (QoS) requirements for the average-case runtime.
Prioritisation of the deployments is based on the average-case runtime.

### Coarse Grained

The Coarse Grained Solver is a simplified optimization algorithm designed to quickly identify viable deployment configurations across a limited set of permitted regions.
It does this by evaluating each permitted region for all instances in a topologically ordered manner, ensuring that the deployment satisfies hard resource constraints such as cost, runtime, and carbon footprint.
Unlike more complex deployment algorithms, the Coarse Grained Solver does not iterate over multiple configurations per instance but rather selects a single region that is permissible for all instances, thereby simplifying the decision-making process.

#### Coarse Grained Workflow

1. **Initialization**:
   - Identifies the set of regions permitted for all instances by intersecting the permissible regions of individual instances.
   - Initializes the deployment configuration with the selected region.

2. **Deployment Generation**:
   - For each permitted region, a deployment configuration is generated if it does not violate the specified hard resource constraints.
   - Ensures the uniqueness of the deployment configuration to avoid redundant solutions.

3. **Evaluation of Deployment**:
   - Checks if the generated deployment configuration meets the hard resource constraints specified and filter out the ones that do not meet these constraints.

4. **Result Compilation**:
   - Compiles a list of valid deployment configurations that satisfies the constraints.

### Stochastic Heuristic Descent

The Stochastic Heuristic deployment algorithm is a heuristic optimization algorithm that utilizes a stochastic approach to explore different deployment configurations.
It employs a heuristic method for quick and efficient problem-solving.
The deployment algorithm is not guaranteed to find the optimal solution, nor to be exhaustive in its search.
The deployment algorithm optimizes for multiple objectives including cost, runtime, and carbon footprint.
It ensures that solutions adhere to specified resource constraints.
Similar to the other deployment algorithms it uses worst-case estimates with regards to conditional calls (all conditional calls are assumed to be true) and the tail latency for the function runtimes and the network latencies to filter for hard constraints.
The deployment algorithm is implemented as a hill-climbing algorithm with a stochastic approach.

#### Stochastic Heuristic Descent Workflow

1. **Initialization**:
   - Sets up critical parameters like learning rate and maximum iterations.
   - Initializes the deployment configuration with initial region assignments.

2. **Iteration Loop**:
   - Iteratively updates a subset of instances based on the learning rate.
   - Randomly selects new region assignments for each instance and evaluates potential improvements.

3. **Evaluation of Deployment**:
   - Checks if the current deployment configuration meets the specified hard resource constraints.
   - Ensures the uniqueness of the deployment to avoid redundant solutions.

4. **Result Compilation**:
   - Upon completion of the iterations, compiles a list of valid and unique average case deployments.
   - These deployments represent the optimized configurations discovered by the deployment algorithm.

### Fine Grained

The Fine Grained Solver is a more complex optimization algorithm designed to identify the optimal deployment configurations across a wide range of permitted regions.
It does this by evaluating every possible configuration for each instance, ensuring that the deployment satisfies hard resource constraints such as cost, runtime, and carbon footprint.
The Fine Grained Solver iterates over multiple configurations per instance, selecting the best configuration based on the average-case runtime.
This search is exhaustive and guarantees the optimal solution, but it is computationally expensive and time-consuming.

#### Fine Grained Workflow

1. **Initialization**:
   - Identifies the set of regions permitted for all instances by intersecting the permissible regions of individual instances.
   - Initializes the deployment configuration with the selected region.
2. **Deployment Generation**:
    - For each permitted region and each instance, a deployment configuration is generated if it does not violate the specified hard resource constraints.
    - Ensures the uniqueness of the deployment configuration to avoid redundant solutions.
3. **Evaluation of Deployment**:
    - Checks if the generated deployment configuration meets the hard resource constraints specified and filter out the ones that do not meet these constraints.

##  References

[1]: Ben-Nun T, de Fine Licht J, Ziogas AN, Schneider T, Hoefler T. Stateful dataflow multigraphs: A data-centric model for performance portability on heterogeneous architectures. InProceedings of the International Conference for High Performance Computing, Networking, Storage and Analysis 2019 Nov 17 (pp. 1-14).
