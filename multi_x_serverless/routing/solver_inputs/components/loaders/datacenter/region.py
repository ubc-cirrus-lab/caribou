import numpy as np

from multi_x_serverless.routing.solver_inputs.components.loaders.loader import Loader


class DataCenterRegionLoader(Loader):
    def __init__(self) -> None:
        super().__init__()

    def setup(self, regions: list[(str, str)]) -> bool:
        self._data = {}

        # TODO (#35): Load data from database, convert to proper format and store in self._data

        # template of output data
        self._data = {
            "compute_costs": {},  # "PLACEHOLDER: loaded dictionary ((region_provider, region_name): *****Compute cost list(this need to be treated differently as providers scale cost base on calls))", list[(float, int)]
            "pue": {},  # "PLACEHOLDER: loaded dictionary ((region_provider, region_name): PUE)",
            "cfe": {},  # "PLACEHOLDER: loaded dictionary ((region_provider, region_name): CFE)",
            "average_kw_compute": {},  # "PLACEHOLDER: loaded dictionary ((region_provider, region_name): compute kw/compute)",
            "memory_kw_mb": {},  # "PLACEHOLDER: loaded dictionary ((region_provider, region_name): memory kw/MB)",
            "Free_tier": {},  # "PLACEHOLDER: loaded dictionary ((region_provider, region_name): free tier informations (Implement at the same time as we implement for free tier issue #27))",
        }

        return False  # Not implemented
