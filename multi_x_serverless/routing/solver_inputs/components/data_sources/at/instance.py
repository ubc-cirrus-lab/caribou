import typing

import numpy as np

# Indexers
from multi_x_serverless.routing.models.indexer import Indexer
from multi_x_serverless.routing.solver_inputs.components.data_sources.source import Source


class InstanceSource(Source):
    def __init__(self) -> None:
        super().__init__()

    def setup(
        self, loaded_data: dict, instance_configuration: list[dict], instances: list[str], instance_indexer: Indexer
    ) -> None:
        self._data = {}

        # Parse the instance configuration to usable format for next steps
        provider_configurations = self._parse_provider_configuration(instance_configuration)

        # Known information
        for instance in instances:
            instance_index = instance_indexer.value_to_index(instance)

            # Group data
            self._data[instance_index] = {
                # Data Collector information - Past invocation information (from historical data)
                "execution_time": loaded_data.get("execution_time", {}).get(instance, -1),
                # Other properties
                ## Need to consider different configurations of different providers defined at the instance configuration level
                "provider_configurations": provider_configurations[instance],
            }

    def _parse_provider_configuration(self, instance_configuration: list[dict]) -> dict:
        provider_configurations = {}
        for instance_information in instance_configuration:
            instance_name = instance_information["instance_name"]

            # Instance specific provider configuration
            instance_provider_information = {}
            for provider_name, provider_information in instance_information["regions_and_providers"][
                "providers"
            ].items():
                memory = provider_information.get("memory", -1)
                vcpu = provider_information.get("vcpu", -1)

                # Configure memory and vcpu configuration and or translation
                if provider_name == "aws":
                    # Vcpu ratio (assuming linear, intercept at 0 scaling) for aws lambda https://docs.aws.amazon.com/lambda/latest/dg/configuration-function-common.html
                    vcpu = memory / 1769

                instance_provider_information[provider_name] = {
                    "memory": memory,
                    "vcpu": vcpu,
                }

            provider_configurations[instance_name] = instance_provider_information

        return provider_configurations

    def get_value(self, data_name: str, instance_index: int) -> typing.Any:
        # Result type might not necessarily be float, such as provider_configurations
        # Which is a tuple containing memory and vcpu information of a instance.
        return self._data[instance_index][data_name]
