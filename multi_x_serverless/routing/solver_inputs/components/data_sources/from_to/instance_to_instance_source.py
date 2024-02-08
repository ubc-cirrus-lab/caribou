from multi_x_serverless.routing.models.indexer import Indexer
from multi_x_serverless.routing.solver_inputs.components.data_sources.from_to.from_to_source import FromToSource


class InstanceToInstanceSource(FromToSource):
    def setup(self, loaded_data: dict, items_to_source: list[str], indexer: Indexer) -> None:
        self._data = {}

        # Known information
        for from_instance in items_to_source:
            from_instance_index = indexer.value_to_index(from_instance)
            for to_instance in items_to_source:
                to_instance_index = indexer.value_to_index(to_instance)

                if from_instance_index not in self._data:
                    self._data[from_instance_index] = {}

                self._data[from_instance_index][to_instance_index] = {
                    # Data Collector information
                    "probability": loaded_data.get("probability", {}).get((from_instance, to_instance), -1),
                    "data_transfer_size": loaded_data.get("data_transfer_size", {}).get(
                        (from_instance, to_instance), -1
                    ),
                }
