class Formatter:
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
