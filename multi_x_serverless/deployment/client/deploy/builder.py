from __future__ import annotations

from typing import Any

from multi_x_serverless.deployment.client.config import Config
from multi_x_serverless.deployment.client.deploy.models import (
    Resource,
)


def create_build_stage(config: Config) -> list[BuildStep]:
    pass


class BuildContext(object):
    def __init__(self) -> None:
        self._variables = {}

    def set_variable(self, name: str, value: Any) -> None:
        self._variables[name] = value

    def get_variable(self, name: str) -> Any:
        return self._variables[name]


class BuildStep(object):
    def __init__(self, name: str) -> None:
        self._name = name

    def execute(self, context: BuildContext, resources: list[Resource]) -> None:
        raise NotImplementedError()


class BuildStage(object):
    def __init__(self, steps: list[BuildStep]) -> None:
        self._steps = steps

    def execute(self, context: BuildContext, resources: list[Resource]) -> None:
        for step in self._steps:
            step.execute(context, resources)
