from dataclasses import dataclass
from typing import Optional


@dataclass
class FunctionInstance:
    name: str
    entry_point: bool
    regions_and_providers: Optional[dict]
    function_resource_name: str

    def to_json(self) -> dict:
        """
        Get the JSON representation of this function.
        """
        return {
            "instance_name": self.name,
            "function_name": self.function_resource_name,
            "regions_and_providers": self.regions_and_providers,
        }

    def __repr__(self) -> str:
        return f"FunctionInstance(name={self.name}, entry_point={self.entry_point}, regions_and_providers={self.regions_and_providers}, function_resource_name={self.function_resource_name})"  # pylint: disable=line-too-long
