#  Design Document

In this document we will discuss the design decisions that have been made for the project.

## Table of Contents

1. [Dataflow DAG Model](#dataflow-dag-model)
   1. [Physical Representation](#physical-representation)
   2. [Logical Representation](#logical-representation)
      1. [Logical Node Naming Scheme](#logical-node-naming-scheme)
      2. [Connection to Physical Representation](#connection-to-physical-representation)
   3. [Discussion](#discussion)
2. [Solver Dataflow Architecture](#solver-dataflow-architecture)
   1. [Data Collector](#data-collector)
   2. [Solver Input](#solver-input)
3. [Solver Methodologies](#solver-methodologies)
   1. [Simple Solver](#simple-solver)
   2. [Brute Force Solver](#brute-force-solver)
4. [Source Code Annotation](#source-code-annotation)
5. [Merge Node](#merge-node)
6. [References](#references)

##  Dataflow DAG Model

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

###  Logical Representation

Our logical representation is, similar to a dataflow DAG [1], the representation of the data flow between the physical instances of our functions.
The dataflow DAG in our case represents calls from functions to other functions with specific input data (thus the name dataflow).
Thus the nodes represent functions or _computations_ and the edges represent data movement between the functions.

The logical representation consists of a set of nodes and a set of edges, where the nodes are of three different types:

- Initial node: The initial node is the node that is called by the user.
It has no incoming edges.
- Intermediate node: An intermediate node is a node that is called by another node.
It has exactly one incoming edge.
- Merge node: A merge node is a node that is called by multiple other nodes.
It one or more incoming edges.

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
- Merge node: `<function_name>:merge:<index_in_dag>`

This naming scheme is used to uniquely identify a node in the logical representation.
The scheme has the implication that colons cannot be allowed in the function names.

#### Connection to Physical Representation

The logical representation is connected to the physical representation by the following rules:

- Each node in the logical representation is represented by a physical node, however, the same physical node might be present multiple times in the logical representation.

Thus we also speak of a _pyhsical node_ and its _logical instances_.

There are two exceptions with regards to physical nodes in the logical representation:

- The initial node is present only once in the logical representation.
- A merge node is present only once in the logical representation.

###  Discussion

These representations gave been chosen because they are simple and easy to understand.
The logical representation is a DAG, which is a well known data structure and easy to work with.
The representation of a DAG was chosen because it opens the avenues for graph optimizations in the solver.
We ended up choosing a graph instead of a multigraph because we do not need to represent multiple edges between two nodes.
The dataflow between the functions is furthermore directed and acyclic because we do not allow for loops in the workflow.

The physical representation is a set of nodes because we do not need to represent any edges between the nodes.
The physical representation is furthermore a set because we do not need to represent the same node multiple times.
This notation is only used for the deployment utilities and is hidden from the solver.

## Solver Dataflow Architecture

In order to determine the most optimal deployment path for each instance of a workflow, prioritizing cost, carbon, or runtime objectives, the solver must be able to gather a large amount of data from various sources and properly utilize those values for calculations. Data gathering steps should be decoupled from the solver directly, and some calculated values may be reused for different runs of a solver. Thus, these responsibilities are handled by the Data Collector and Solver Input.

### Data Collector

This application is responsible for directly interacting with and retrieving information from both external APIs and summarizing workflow invocation information. The data collector consists of many different parts, each responsible for different types of data and each invoked at different timming intervals.

#### Workflow Information Collector
This component is responsible for summarizing invocation information of a workflow and presenting it for use in the solver.

Below are the different pieces of information collected by this collector:
- `execution_time`: The average execution time of function invocation of an specific instance in units of seconds.
- `probability`: The probability that an instance node of being traversed in each invocation of a workflow.
- `data_transfer_size`: The average data transfer size that an instance node will supply to another instance after each invocation.

#### Datacenter Information Collector
This component is responsible for collecting and summarizing datacenter information used in the solver. Some fields are specific to certain providers and not applicable to others. Datacenter regions are also simply referred to as regions here.

Below are the different information that involved that are collected and sumarized by this collector:

- `data_transfer_ingress_cost`: Cost of ingress from one region to another in units of USD / GB.
- `data_transfer_egress_cost`: Cost of egress from one region to another in units of USD / GB.
- `transmission_times`: The transmission time in seconds that it takes to transmit data a range of data sizes in GB.
- `compute_cost`: The compute cost of execution in one region in units of USD / GBs.
- `pue`: The Power Usage Effectiveness of a datacenter region.
- `cfe`: Carbon Free Energy (in fraction), google specific concept.
- `average_kw_compute`: Average kw of power from unit of compute, in units of kw / Compute. 
- `memory_kw_mb`: Average kw of power from unit of Memory, in units of kw / MB. 
- `free_tier_invocations`: Number of free invocations remaining in an region.
- `free_tier_compute`: Amount of free compute available in an region, in units of GB-s.

#### Carbon Information Collecton
This component is responsible for collecting and summarizing information from external Carbon Intensity APIs and presenting it for use in the solver. Datacenter regions are also simply referred to as regions here.

Below are the different pieces of information involved that are collected and summarized by this collector:

- `grid_co2e`: The carbon intensity of the electric grid in a datacenter region, in units of gCO2eq / kWh.
- `data_transfer_co2e`: The carbon intensity of data movement in units of gCO2eq / GB.

### Solver Input
The solver input is responsible for loading the saved database from the [Data Collector](#data-collector), organizing all applicable data in a format usable for calculation, and presenting an easy-to-use interface for the solver.

Currently, the solver can access the information with the following simple function calls:
```python
get_execution_cost_carbon_runtime(instance_index: int, region_index: int)
```

```python
get_transmission_cost_carbon_runtime(from_instance_index: int, to_instance_index: int, from_region_index: int, to_region_index: int):
```

The solver can easily use these functions to quickly calculate the cost, carbon, and runtime of both execution and transmission at each step of a workflow.

## Solver Methodologies

In this project, we provide a variety of different solvers with different methododlogies, each with tradeoffs between speed, computational resource utilization, and thoroughness.

### Simple Solver

This solver deploys the entire workflow, along with all its functions and instances, in a single region. It can perform quickly with minimal computational resource utilization, but at the expense of having coarse-grained deployment options and low thoroughness.

### Brute Force Solver

In this solver, each instance of a workflow can be deployed in any of the user-defined permitted regions, and the solver will attempt to exhaustively search through all possibilities. Thus, this solver will have high thoroughness but at the cost of slow speed and very high computational resource utilization.

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
        "only_regions": [
          {
            "provider": "aws",
            "region": "us-east-1",
          }
        ],
        "forbidden_regions": None,
        "providers": [
            {
                "name": "aws",
                "config": {
                    "timeout": 60,
                    "memory": 128,
                },
            }
        ],
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

**TODO (#11): Implement conditional function invocation**

```python
workflow.invoke_serverless_function(second_function, payload, condition)
```

The condition is a boolean expression that is evaluated at runtime.
If the condition evaluates to true, the function is called, otherwise it is not called.

- Finally, there is the option of merging multiple predecessor calls at a merge node.
The responses from the predecessor calls are then passed to the merge node as a list of responses.
This is done with the following annotation:

```python
responses: list[Any] = workflow.get_predecessor_data()
```

## Merge Node

The merge nodes have a special semantic.
The definition of a merge node is that, compared to all other nodes, there are one or more predecessor nodes that call the merge node.
The merge node will receive the payload (responses) from all predecessor nodes and can then handle the responses according to the user defined logic.
This logic has an important implication for the logical representation of the DAG as it means that since the physical representation does not define on what specific predecessors we are waiting on the merge node waits for all predecessors and thus there can only be one logical instance of a merge node in the logical representation.
Hence also the slightly different naming scheme of the merge nodes (see also the section on [Logical Node Naming Scheme](#logical-node-naming-scheme)).

### Implementation

The logic of the merge node is implemented as follows:

1. When a predecessor calls a merge node, the predecessor will add its response to a list of responses in a distributed key-value store in the region of the merge node.
2. The predecessor will then atomically increment a counter in the distributed key-value store.
3. The new value of the counter is then checked against the number of predecessors of the merge node.
4. If the counter is equal to the number of predecessors, the merge node will be called. Otherwise, the predecessor will not call the merge node. This ensures that the merge node is only called when all predecessors have called the merge node.

##  References

[1]: Ben-Nun T, de Fine Licht J, Ziogas AN, Schneider T, Hoefler T. Stateful dataflow multigraphs: A data-centric model for performance portability on heterogeneous architectures. InProceedings of the International Conference for High Performance Computing, Networking, Storage and Analysis 2019 Nov 17 (pp. 1-14).
