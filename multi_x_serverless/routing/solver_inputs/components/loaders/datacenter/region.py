import numpy as np

from multi_x_serverless.routing.solver_inputs.components.loaders.loader import Loader


class DataCenterRegionLoader(Loader):
    def setup(self, regions: list[tuple[str, str]]) -> bool:
        self._data = {}

        # TODO (#35): Load data from database, convert to proper format and store in self._data

        # Template of the loaded data - Also in docs
        # Key for this data is the region
        self._data = {
            # For Compute cost, we simply assume that it will be taken care of by
            # Data Collector (the closest current cost). As calculating this dynamically
            # may be too costly. And in addition, cost tiers changes are in orders of Billions GBS for Amazon
            # So we assume that the workflow will not be large enough to cause a change in cost tier.
            "compute_cost": {("p1", "r1"): 0.0000166667},  # Compute cost in units of USD/GBs
            "pue": {("p1", "r1"): 1.2},  # Power Usage Effectiveness (in ratio)
            "cfe": {("p1", "r1"): 0.05},  # Carbon Free Energy (in fractions)
            "average_kw_compute": {
                ("p1", "r1"): 0.002485
            },  # Average compute kw/compute (compute kwh can be calcualted by: this * vcpu * runtime_in_hours)
            "memory_kw_mb": {
                ("p1", "r1"): 0.000000392
            },  # Memory kw/MB (memory kwh can be calculated by: this * memory * runtime_in_hours)
            "free_tier_invocations": {
                ("p1", "r1"): 1000000
            },  # Free tier remaining in a region in units of number of invocations
            "free_tier_compute": {("p1", "r1"): 400000},  # Free tier for compute in units of GB-s
        }

        return False  # Not implemented
