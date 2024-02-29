class Formatter:
    def __init__(self, home_deployment: dict[int, int], home_deployment_metrics: dict[str, float]) -> None:
        self._home_deployment = home_deployment
        self._home_deployment_metrics = home_deployment_metrics

    def format(
        self,
        results: tuple[dict[int, int], dict[str, float]],
        index_to_instance_name: dict[int, str],
        index_to_region_provider_name: dict[int, str],
    ) -> dict:
        """
        The desired output format is explained in the `docs/design.md` file under `Workflow Placement Decision`.
        """
        # The results are already formatted, so just return them
        # TODO (#81): Preserve Home Region Workflow in Active Workflow Deployments
        # TODO (#152): Add expiry time to the selected deployment
        return {
            "current_deployment": {
                "workflow_placement": {
                    index_to_instance_name[key]: {
                        "provider_region": {
                            "provider": index_to_region_provider_name[value].split(":")[0],
                            "region": index_to_region_provider_name[value].split(":")[1],
                        }
                    }
                    for key, value in results[0].items()
                },
                "metrics": results[1],
            },
            "home_deployment": {
                "workflow_placement": {
                    index_to_instance_name[key]: {
                        "provider_region": {
                            "provider": index_to_region_provider_name[value].split(":")[0],
                            "region": index_to_region_provider_name[value].split(":")[1],
                        }
                    }
                    for key, value in self._home_deployment.items()
                },
                "metrics": self._home_deployment_metrics,
            }
        }
