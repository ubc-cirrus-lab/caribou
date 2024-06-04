from datetime import datetime, timedelta
from typing import Any, Optional

from caribou.common.constants import TIME_FORMAT


class ExecutionToSuccessorTime:  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        current_instance_name: str,
        successor_instance_name: str,
        current_instance_start_time: datetime,
        successor_instance_invocation_time: datetime,
        invoked_successor: bool,
    ) -> None:
        self.current_instance_name: str = current_instance_name
        self.successor_instance_name: str = successor_instance_name
        self.duration: timedelta = successor_instance_invocation_time - current_instance_start_time
        self.invoked_successor: bool = invoked_successor

    def to_dict(self) -> tuple[datetime, dict[str, Any]]:
        return (
            {
                "current_instance_name": self.current_instance_name,
                "successor_instance_name": self.successor_instance_name,
                "duration": self.duration.total_seconds(),
                "invoked_successor": self.invoked_successor,
            },
        )
