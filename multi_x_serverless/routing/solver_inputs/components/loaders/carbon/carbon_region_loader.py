from multi_x_serverless.routing.solver_inputs.components.loaders.region_loader import RegionLoader


class CarbonRegionLoader(RegionLoader):
    def setup(self, regions: list[tuple[str, str]]) -> bool:
        self._data = {}

        print("Loading CarbonRegionLoader for regions: ", regions)

        # TODO (#35): Load data from database, convert to proper format and store in self._data

        # Template of the loaded data - Also in docs
        # Key for this data is the region name
        self._data = {
            "grid_co2e": {("p1", "r1"): 79.0},  # grid_co2e in gCO2eq/kWh",
        }

        return False  # Not implemented
