import pandas as pd
from config.config import THRESHOLDS
from heuristics.base_heuristic import BaseHeuristic, Criticity

class DiskUsageHeuristic(BaseHeuristic):
    """Heuristic for evaluating disk usage by executors in a Spark application."""

    @staticmethod
    def evaluate(all_data):
        checks = []
        executor_data = all_data.get("executors", [])
        disk_usage_threshold = THRESHOLDS.get("disk_usage_bytes", 1e9)  # 1 GB

        if not executor_data:
            return pd.DataFrame([{
                "category": "Disk Usage",
                "expected": "Executors present",
                "current": "None",
                "description": "No executors found in data.",
                "criticity": Criticity.HIGH.value
            }])

        for executor in executor_data:
            executor_id = executor.get("id", "N/A")
            disk_used = executor.get("diskUsed", 0)

            criticity = Criticity.HIGH if disk_used > disk_usage_threshold else Criticity.NONE
            DiskUsageHeuristic.add_check(
                checks, "Disk Usage", f"<= {disk_usage_threshold / 1e6:.2f} MB",
                f"{disk_used / 1e6:.2f} MB", f"Executor {executor_id} is consuming excessive disk space.", criticity
            )

        return pd.DataFrame(checks)

# Example usage:
# df = DiskUsageHeuristic.evaluate(all_data)
# print(df)
