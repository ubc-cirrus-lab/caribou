from dataclasses import dataclass


@dataclass
class FunctionInstance:
    name: str
    entry_point: bool
    timeout: int
    memory: int
    region_group: str
    function_resource_name: str

    def to_json(self) -> dict:
        """
        Get the JSON representation of this function.
        """
        return {
            "name": self.name,
            "entry_point": self.entry_point,
            "timeout": self.timeout,
            "memory": self.memory,
            "region_group": self.region_group,
        }

    def __repr__(self) -> str:
        return f"""FunctionInstance(
                name={self.name},
                entry_point={self.entry_point},
                timeout={self.timeout},
                memory={self.memory},
                region_group={self.region_group},
                function_resource_name={self.function_resource_name})
                """