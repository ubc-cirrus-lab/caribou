from multi_x_serverless.routing.solver_inputs.components.loaders.region_loader import RegionLoader


class CarbonRegionFromToLoader(RegionLoader):
    def setup(self, regions: list[tuple[str, str]]) -> bool:
        self._data = {}

        print("Loading CarbonRegionFromToLoader for regions: ", regions)

        # TODO (#35): Load data from database, convert to proper format and store in self._data

        # TODO (#35): When loading we need to pay attention that JSON keys are strings and not tuples
        # The tuples have to be converted from the strings in the JSON string loaded from the table

        # Template of the loaded data - Also in docs
        # Key for this data is the (from region name, to region name)
        self._data = {
            "data_transfer_co2e": {(("p1", "r1"), ("p1", "r2")): 3.05},  # gCO2eq/GB
        }

        return False  # Not implemented
