from boto3.session import Session

from typing import Any

class AWSClient(object):
    def __init__(self, session: Session) -> None:
        self._session = session
        self._client_cache: dict[str, Any] = {}