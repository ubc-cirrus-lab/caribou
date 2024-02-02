from typing import Any, Optional

from multi_x_serverless.common.provider import Provider
from multi_x_serverless.routing.models.indexer import Indexer
from multi_x_serverless.routing.solver_inputs.components.data_sources.at.at_source import AtSource


class InstanceSource(AtSource):
    def setup(
        self,
        loaded_data: dict,
        items_to_source: list,
        indexer: Indexer,
        configurations: Optional[list[dict]] = None,
    ) -> None:
        self._data: dict[int, Any] = {}

        if configurations is None:
            raise ValueError("No configurations provided for the instance source")

        # Parse the instance configuration to usable format for next steps
        provider_configurations = self._parse_provider_configuration(configurations)

        # Known information
        for instance in items_to_source:
            instance_index = indexer.value_to_index(instance)

            # Group data
            self._data[instance_index] = {
                # Data Collector information - Past invocation information (from historical data)
                "execution_time": loaded_data.get("execution_time", {}).get(instance, -1),
                # Other properties
                # Need to consider different configurations of different providers defined at
                # the instance configuration level
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
                if provider_name == Provider.AWS.value:
                    # Vcpu ratio (assuming linear, intercept at 0 scaling)
                    # for aws lambda https://docs.aws.amazon.com/lambda/latest/dg/configuration-function-common.html
                    vcpu = memory / 1769

                instance_provider_information[provider_name] = {
                    "memory": memory,
                    "vcpu": vcpu,
                }

            provider_configurations[instance_name] = instance_provider_information

        return provider_configurations
