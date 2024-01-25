# import json
# from abc import ABC, abstractmethod
# from typing import Any, Optional

# from multi_x_serverless.common.constants import DATA_COLLECTOR_DATA_TABLE
# from multi_x_serverless.common.models.endpoints import Endpoints
# from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient

# class DataRetriever(ABC):
#     _data_table: str

#     def __init__(self, client: RemoteClient) -> None:
#         self._data_collector_client = client

#     @abstractmethod
#     def collect_data(self) -> dict[str, Any]:
#         """
#         Collects data from the data source

#         Returns:
#             Dict[str, Any]: data collected from the data source
#         """
#         pass