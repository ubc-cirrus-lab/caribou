from abc import abstractmethod

from multi_x_serverless.routing.solver_inputs.components.loaders.loader import Loader


class InstanceLoader(Loader):
    @abstractmethod
    def setup(self, workflow_id: str) -> bool:
        raise NotImplementedError
