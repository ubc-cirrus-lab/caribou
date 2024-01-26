from typing import Any

from multi_x_serverless.data_collector.components.data_retriever import DataRetriever


class ProviderRetriever(DataRetriever):
    def __init__(self) -> None:
        # TODO (#95): Fill Data Retriever Implementations
        pass


# Maybe use this to determine AWS region -
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-availability-zones
# Can use /aws/service/global-infrastructure/regions/region-code/longName to get region name

# To view Old legacy code:
# https://github.com/ubc-cirrus-lab/multi-x-serverless/pull/5 -> multi_x_serverless/routing/data_gatherers/cost/datacenter_information
