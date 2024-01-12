import numpy as np

from multi_x_serverless.routing.solver_inputs.components.loaders.loader import Loader


class CarbonRegionLoader(Loader):
    def __init__(self) -> None:
        super().__init__()

    def setup(self, regions: list[(str, str)]) -> bool:
        self._data = {}

        # TODO (#35): Load data from database, convert to proper format and store in self._data

        # template of output data
        self._data = {
            "grid_co2e": {},  # "PLACEHOLDER: loaded dictionary ((region_provider, region_name): grid_co2e in gCO2eq/kWh)",
        }

        return False  # Not implemented
