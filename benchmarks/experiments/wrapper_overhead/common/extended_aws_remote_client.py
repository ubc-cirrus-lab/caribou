from multi_x_serverless.common.models.remote_client.aws_remote_client import AWSRemoteClient

class ExtendedAWSRemoteClient(AWSRemoteClient): 
    def __init__(self, region: str) -> None:
        super().__init__(region)