from typing import Any

from multi_x_serverless.data_collector.components.data_exporter import DataExporter


class PerformanceExporter(DataExporter):
    def export_all_data(self, performance_region_data: dict[str, Any]) -> None:
        self._export_region_data(performance_region_data)
