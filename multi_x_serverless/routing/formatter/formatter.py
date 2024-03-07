from datetime import datetime, timedelta

from multi_x_serverless.common.constants import GLOBAL_TIME_ZONE, TIME_FORMAT


class Formatter:
    def __init__(
        self,
        home_deployment: list[int],
        home_deployment_metrics: dict[str, float],
        expiry_time_delta_seconds: int = 604800,
    ) -> None:
        self._home_deployment = home_deployment
        self._home_deployment_metrics = home_deployment_metrics
        self._expiry_time_delta_seconds = expiry_time_delta_seconds

    def format(
        self,
        results: tuple[list[int], dict[str, float]],
        index_to_instance_name: dict[int, str],
        index_to_region_provider_name: dict[int, str],
    ) -> dict:
        """
        The desired output format is explained in the `docs/design.md` file under `Workflow Placement Decision`.
        """
        expiry_date = datetime.now(GLOBAL_TIME_ZONE) + timedelta(seconds=self._expiry_time_delta_seconds)
        expiry_date_str = expiry_date.strftime(TIME_FORMAT)

        return {
            "workflow_placement": {
                "current_deployment": {
                    "instances": {
                        index_to_instance_name[key]: {
                            "provider_region": {
                                "provider": index_to_region_provider_name[value].split(":")[0],
                                "region": index_to_region_provider_name[value].split(":")[1],
                            }
                        }
                        for key, value in enumerate(results[0])
                    },
                    "metrics": results[1],
                    "expiry_time": expiry_date_str,
                },
                "home_deployment": {
                    "instances": {
                        index_to_instance_name[key]: {
                            "provider_region": {
                                "provider": index_to_region_provider_name[value].split(":")[0],
                                "region": index_to_region_provider_name[value].split(":")[1],
                            }
                        }
                        for key, value in enumerate(self._home_deployment)
                    },
                    "metrics": self._home_deployment_metrics,
                },
            }
        }
