from benchmarks.experiments.wrapper_overhead.common.extended_aws_remote_client import ExtendedAWSRemoteClient

class WrapperOverheadCollectionUtility():
    def __init__(self, aws_region: str):
        self._client: ExtendedAWSRemoteClient = ExtendedAWSRemoteClient(aws_region)
