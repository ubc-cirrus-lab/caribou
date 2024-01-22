from typing import Any

from multi_x_serverless.data_collectors.data_collector import DataCollector


class RegionDatacenterDataCollector(DataCollector):
    def __init__(self) -> None:
        self._table_key = "region_datacenter_data"
        super().__init__()

    def collect_data(self) -> dict[str, Any]:
        # TODO (#50): Fill Data Collector Implementations

        # See multi_x_serverless/routing/solver_inputs/components/loaders/datacenter
        # for how to load data to the database to be retrieved there
        return {}
