from abc import abstractmethod

from multi_x_serverless.routing.solver_inputs.components.loaders.loader import Loader


class RegionLoader(Loader):
    @abstractmethod
    def setup(self, regions: list[tuple[str, str]]) -> bool:
        raise NotImplementedError
