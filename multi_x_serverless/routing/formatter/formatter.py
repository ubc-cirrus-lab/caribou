from typing import Any


class Formatter:
    def format(
        self,
        results: tuple[dict, float, float, float],
        index_to_instance_name: dict[int, Any],
        index_to_region_provider_name: dict[int, Any],
    ) -> dict:
        """
        The desired output format is explained in the `docs/design.md` file under `Workflow Placement Decision`.
        """
        # The results are already formatted, so just return them
        return {
            index_to_instance_name[key]: {
                "provider": index_to_region_provider_name[value][0],
                "region": index_to_region_provider_name[value][1],
            }
            for key, value in results[0].items()
        }
