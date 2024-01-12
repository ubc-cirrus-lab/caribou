import numpy as np

from multi_x_serverless.routing.solver_inputs.components.loaders.loader import Loader


class CarbonRegionFromToLoader(Loader):
    def __init__(self) -> None:
        super().__init__()

    def setup(self, regions: list[(str, str)]) -> bool:
        self._data = {}

        # TODO (#35): Load data from database, convert to proper format and store in self._data

        # template of output data
        self._data = {
            "data_transfer_co2e": {},  # "PLACEHOLDER: loaded dictionary (((from_region_provider, from_region_name), (to_region_provider, to_region_name)): gCO2eq/GB)",
        }

        return False  # Not implemented
