import json
import re
from typing import Any

from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.data_collector.components.data_exporter import DataExporter


class SingleLineListEncoder(json.JSONEncoder):
    def encode(self, obj: Any) -> str:  # pylint: disable=arguments-renamed
        if isinstance(obj, list):
            return "[" + ", ".join(self.encode(el) for el in obj) + "]"
        return super().encode(obj)


def fix_nested_lists(s: str) -> str:
    # Fix nested lists
    pattern = re.compile(r"\[\s+([^][]+?)\s+\]")
    while True:
        s_new = pattern.sub(lambda m: "[" + " ".join(m.group(1).split()) + "]", s)
        if s_new == s:
            break
        s = s_new

    # Fix nested lists inside lists
    pattern_nested = re.compile(r"\[\s*\[([^][]+?)\]\s*\]")
    while True:
        s_new = pattern_nested.sub(lambda m: "[[" + " ".join(m.group(1).split()) + "]]", s)
        if s_new == s:
            break
        s = s_new

    # Fix deeply nested lists
    pattern_deep_nested = re.compile(r"\[\s*(\[[^][]+\](?:,\s*\[[^][]+\])*)\s*\]")
    while True:
        s_new = pattern_deep_nested.sub(lambda m: "[" + " ".join(m.group(1).split()) + "]", s)
        if s_new == s:
            break
        s = s_new

    return s


def pretty_print(data: dict[Any, Any]) -> None:
    json_str = json.dumps(data, cls=SingleLineListEncoder, indent=4)

    # TODO: Remove this print statement
    print(fix_nested_lists(json_str))


class WorkflowExporter(DataExporter):
    def __init__(self, client: RemoteClient, workflow_instance_table: str) -> None:
        super().__init__(client, "")
        self._workflow_summary_table: str = workflow_instance_table

    def export_all_data(self, workflow_summary_data: dict[str, Any]) -> None:
        self._export_workflow_summary(workflow_summary_data)
        pretty_print(workflow_summary_data)

    def _export_workflow_summary(self, workflow_summary_data: dict[str, Any]) -> None:
        self._export_data(self._workflow_summary_table, workflow_summary_data, False)
