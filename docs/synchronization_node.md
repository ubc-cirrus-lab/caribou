#  Synchronization Node

The synchronization nodes have a special semantic.
The definition of a synchronization node is that, compared to all other nodes, there are one or more predecessor nodes that call the synchronization node.
The synchronization node will receive the payload (responses) from all predecessor nodes and can then handle the responses according to the user defined logic.
This logic has an important implication for the logical representation of the DAG as it means that since the physical representation does not define on what specific predecessors we are waiting on the synchronization node waits for all predecessors and thus there can only be one logical instance of a synchronization node in the logical representation.

## Implementation

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
