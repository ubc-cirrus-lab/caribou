import numpy as np

from multi_x_serverless.routing.solver_inputs.components.loaders.loader import Loader


class DataCenterRegionToRegionLoader(Loader):
    def setup(self, regions: list[tuple[str, str]]) -> bool:
        self._data = {}

        # TODO (#35): Load data from database, convert to proper format and store in self._data

        # TODO (#35): When loading we need to pay attention that JSON keys are strings and not tuples
        # The tuples have to be converted from the strings in the JSON string loaded from the table

        # Template of the loaded data - Also in docs
        # Key for this data is the (from region, to region)
        self._data = {
            "data_transfer_ingress_cost": {(("p1", "r1"), ("p1", "r2")): 0.0},  # Cost in USD / GB
            "data_transfer_egress_cost": {(("p1", "r1"), ("p1", "r2")): 0.08},  # Cost in USD / GB
            "transmission_times": {
                (("p1", "r1"), ("p1", "r2")): [(5, 0.03), (10, 0.05)]
            },  # Transmission time in [(transmitted data in GB, transmission time in seconds)]
        }

        return False  # Not implemented
