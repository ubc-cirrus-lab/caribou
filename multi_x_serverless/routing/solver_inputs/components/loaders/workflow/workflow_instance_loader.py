from multi_x_serverless.routing.solver_inputs.components.loaders.instance_loader import InstanceLoader


class WorkflowInstanceLoader(InstanceLoader):
    def setup(self, workflow_id: str) -> bool:
        self._data = {}

        print(f"Loading data for workflow {workflow_id}")

        # TODO (#35): Load data from database, convert to proper format and store in self._data

        # Template of the loaded data - Also in docs
        # Key for this data is the instance name
        self._data = {
            "execution_time": {"i1": 5},  # Execution time value in seconds"
        }

        return False  # Not implemented
