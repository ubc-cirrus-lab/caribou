from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class Instruction:
    name: str


@dataclass(frozen=True)
class RecordResourceVariable(Instruction):
    resource_type: str
    resource_name: str
    variable_name: str

    def __repr__(self) -> str:
        return f"RecordResourceVariable({self.name})"


@dataclass(frozen=True)
class RecordResourceValue(Instruction):
    resource_type: str
    resource_name: str
    arn: str
    value: Any

    def __repr__(self) -> str:
        return f"RecordResourceValue({self.name})"


@dataclass(frozen=True)
class APICall(Instruction):
    params: dict[str, Any]
    output_var: Optional[str] = None

    def __repr__(self) -> str:
        return f"APICall({self.name})"
