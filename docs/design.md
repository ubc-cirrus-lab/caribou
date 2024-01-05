#  Design Document

In this document we will discuss the design decisions that have been made for the project.

##  Dataflow DAG Model

Each workflow has two representations in our model. We distinguish between the physical and the logical representation of a workflow. Following we will quickly go over these two representations and discuss why we chose this representation.

### Physical Representation

The physical representation is the actually deployed functions. Physical in this sense referes to actual source code being deployed at a cloud provider. This is the representation that the deployment utilities work with because the only thing which matters for them is which function source code is deployed in what region and at which provider. The physical representation consists of a set of nodes with no edges:

$P_f = \{n_1, n_2, ..., n_n\}$

This representation is quite simple in our case as the source code for each function is the same, the full workflow folder, however, the handler name (the function name within the package) is different.

###  Logical Representation

Our logical representation is, similar to a dataflow DAG [1], the representation of the data flow between the physical instances of our functions. The dataflow DAG in our case represents calls from functions to other functions with specific input data (thus the name dataflow). Thus the nodes represent functions or _computations_ and the edges represent data movement between the functions.

The logical representation consists of a set of nodes and a set of edges, where the nodes are of three different types:

- Initial node: The initial node is the node that is called by the user. It has no incoming edges.
- Intermediate node: An intermediate node is a node that is called by another node. It has exactly one incoming edge.
- Merge node: A merge node is a node that is called by multiple other nodes. It has multiple incoming edges.

The types of nodes are known at deployment time from static code analysis and are not changed during the execution of the workflow. Likweise, the edges are known at deployment time and are not changed during the execution of the workflow. A node is represented by a call from one function to another by the corresponding workflow annotation. See also the section on [Source Code Annotation](#source-code-annotation). The logical representation is a directed acyclic graph (DAG) with nodes and edges:

$L_f = (N, E)$

where $N$ is the set of nodes and $E$ is the set of edges.

#### Logical Node Naming Scheme

The naming scheme of the nodes is as follows:

- Initial node: `<function_name>:entry_point:0`
- Intermediate node: `<function_name>:<predecessor_function_name>_<predecessor_index>_<successor_of_predecessor_index>:<index_in_dag>`
  Where `<predecessor_function_name>` is the name of the predecessor function, `<predecessor_index>` is the index of the predecessor function in the dag, `<successor_of_predecessor_index>` is the index of the successor of the predecessor function (when a function calls multiple times the same function, this index is used to distinguish between the different calls), and `<index_in_dag>` is the index of the node in the dag in a topological order of dataflow.
- Merge node: `<function_name>:merge:<index_in_dag>`

#### Connection to Physical Representation

The logical representation is connected to the physical representation by the following rules:

- Each node in the logical representation is represented by a physical node, however, the same physical node might be present multiple times in the logical representation.

Thus we also speak of a _pyhsical node_ and its _logical instances_.

There are two exceptions with regards to physical nodes in the logical representation:

- The initial node is present only once in the logical representation.
- A merge node is present only once in the logical representation.

###  Discussion

These representations gave been chosen because they are simple and easy to understand. The logical representation is a DAG, which is a well known data structure and easy to work with. The representation of a DAG was chosen because it opens the avenues for graph optimizations in the solver. We ended up choosing a graph instead of a multigraph because we do not need to represent multiple edges between two nodes. the dataflow between the functions is furthermore directed and acyclic because we do not allow for loops in the workflow.

The physical representation is a set of nodes because we do not need to represent any edges between the nodes. The physical representation is furthermore a set because we do not need to represent the same node multiple times. This notation is only used for the deployment utilities and is hidden from the solver.

## Source Code Annotation

In an initial version of the project we used a JSON config based approach towards transmitting the workflow information to the deployment utilities. However, this approach decoupled the workflow from the source code and thus made it hard to reason about the workflow. Furthermore, it made the static code analysis more complicated as we had to parse the JSON config file and match the functions to the config file. Thus we decided to use source code annotations instead. The source code annotations are used to annotate the functions that are part of the workflow.

The source code annotations allow us to easily reason about the workflow and to extract both the physical and the logical representation of the workflow. There is still an additional configuration file that is used to transmit information such as environment variables and the name of the workflow to the deployment utilities. However, this information is decoupled from the source code and thus does not affect the static code analysis.

Currently, we support the following annotations:

- At the beginning of the workflow the user has to register the workflow with the following annotation:

```python
workflow = MultiXServerlessWorkflow("workflow_name")
```

- At the beginning of each function the user has to register the function with the following annotation:

```python
@workflow.serverless_function(
    name="function_name",
    entry_point=True,
    regions_and_providers={
        "only_regions": [["aws", "us-east-1"], ["aws", "us-east-2"], ["aws", "us-west-1"], ["aws", "us-west-2"]],
        "forbidden_regions": None,
    },
    providers=[
        {
            "name": "aws",
            "config": {
                "timeout": 60,
                "memory": 128,
            },
        }
    ],
)
```

The meaning of the different parameters is as follows:

- `name`: The name of the function. This is the name that is used directly in the physical representation of the workflow and is also used to identify the function in the logical representation of the workflow (see also the section on [Logical Node Naming Scheme](#logical-node-naming-scheme)).
- `entry_point`: A boolean flag that indicates whether the function is the entry point of the workflow. There can only be one entry point in a workflow.
- `regions_and_providers`: A dictionary that contains the regions and providers that the function can be deployed to. This can be used to override the global settings in the `config.yml`. If none or an empty dictionary is provided, the global config takes precedence. The dictionary has two keys:
  - `only_regions`: A list of regions that the function can be deployed to. If this list is empty, the function can be deployed to any region.
  - `forbidden_regions`: A list of regions that the function cannot be deployed to. If this list is empty, the function can be deployed to any region.
- `providers`: A list of providers that the function can be deployed to. This can be used to override the global settings in the `config.yml`. If a list of providers is specified at the function level this takes precedence over the global configurations. If none or an empty list is provided, the global config takes precedence. Each provider is a dictionary with two keys:
  - `name`: The name of the provider. This is the name that is used directly in the physical representation of the workflow.
  - `config`: A dictionary that contains the configuration for the specific provider.

- Within a function, a user can register a call to another function with the following annotation:

```python
workflow.invoke_serverless_function(second_function, payload)
```

The payload is a dictionary that contains the input data for the function. The payload is optional and can be omitted if the function does not require any input data.

- Additionally, there is the option of conditionally calling a function. This is done with the following annotation:

**TODO (#11): Implement conditional function invocation**

```python
workflow.invoke_serverless_function(second_function, payload, condition)
```

The condition is a boolean expression that is evaluated at runtime. If the condition evaluates to true, the function is called, otherwise it is not called.

- Finally, there is the option of merging multiple predecessor calls at a merge node. This is done with the following annotation:

**TODO (#10): Implement merge node**

```python
responses: list[dict[str, Any]] = workflow.get_predecessor_data()
```

##  References

[1]: Ben-Nun T, de Fine Licht J, Ziogas AN, Schneider T, Hoefler T. Stateful dataflow multigraphs: A data-centric model for performance portability on heterogeneous architectures. InProceedings of the International Conference for High Performance Computing, Networking, Storage and Analysis 2019 Nov 17 (pp. 1-14).
