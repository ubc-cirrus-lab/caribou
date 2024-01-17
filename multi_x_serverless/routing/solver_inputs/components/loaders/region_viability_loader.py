import numpy as np

from multi_x_serverless.routing.solver_inputs.components.loaders.loader import Loader


class RegionViabilityLoader(Loader):
    def setup(self) -> bool:
        # TODO (#15): Implement this function
        # TODO: Load data from database, convert to proper format and store in self._data
        # template of output data
        self._data: dict = {
            "viable_regions": [
                {
                    "provider": "aws",
                    "region": "us-east-1",
                    # Can probably add some field here about architecture, supported languages, versions, etc
                },
                # Placeholder data
                {"provider": "p1", "region": "r1"},
                {"provider": "p1", "region": "r2"},
                {"provider": "p2", "region": "r3"},
                {"provider": "p3", "region": "r4"},
            ]
        }

        return False  # "Not yet implemented"
