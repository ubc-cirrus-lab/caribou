class Formatter:
    def __init__(
        self,
        home_deployment: list[int],
        home_deployment_metrics: dict[str, float],
    ) -> None:
        self._home_deployment = home_deployment
        self._home_deployment_metrics = home_deployment_metrics

    def format(
        self,
        results: tuple[list[int], dict[str, float]],
        index_to_instance_name: dict[int, str],
        index_to_region_provider_name: dict[int, str],
    ) -> dict:
        """
        The desired output format is explained in the `docs/design.md` file under `Workflow Placement Decision`.
        """
        return {
            index_to_instance_name[key]: {
                "provider_region": {
                    "provider": index_to_region_provider_name[value].split(":")[0],
                    "region": index_to_region_provider_name[value].split(":")[1],
                }
            }
            for key, value in enumerate(results[0])
        }
