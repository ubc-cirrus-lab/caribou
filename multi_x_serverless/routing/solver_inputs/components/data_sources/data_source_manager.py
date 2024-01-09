
class DataSourceManager:
    def __init__(self):
        super().__init__()
        self._data_sources = None

    def setup(self, loaded_data: dict) -> None:
        # Propagate loaded data to data sources
        None

        
        # # Using those information, we can now setup the data sources
        # self._workflow_instance_input.setup(workflow_instance_information, workflow_instance_from_to_information)
        # self._datacenter_region_input.setup(datacenter_region_information, datacenter_region_from_to_information)
        # self._carbon_region_input.setup(carbon_region_information, carbon_region_from_to_information)

        # # Now take the loaded data and send it to the data sources, which will be used in the component input managers
        # self._carbon_input.setup(regions, self._config, self._regions_indexer, self._instance_indexer)
        # self._cost_input.setup(regions, self._config, self._regions_indexer, self._instance_indexer)
        # self._runtime_input.setup(regions, self._config, self._regions_indexer, self._instance_indexer)
    


    def retrieve_data(self) -> dict:
        return self._data