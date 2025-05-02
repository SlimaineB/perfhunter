import pandas as pd
from config.config import THRESHOLDS
from enum import Enum
from heuristics.base_heuristic import BaseHeuristic, Criticity

class DataSkewHeuristic(BaseHeuristic):
    """Heuristic for detecting data skew issues in Spark stages."""

    @staticmethod
    def evaluate(all_data):
        checks = []
        stage_data = all_data.get("stages", [])  # Retrieve stage data
        skew_ratio_threshold = THRESHOLDS["data_skew_ratio"]  # Skew threshold

        for stage in stage_data:
            stage_id = stage.get("stageId", "N/A")
            task_metrics = stage.get("taskMetrics", {})
            if task_metrics:
                max_records = task_metrics.get("maxRecordsRead", 0)
                min_records = task_metrics.get("minRecordsRead", 0)
                avg_records = task_metrics.get("avgRecordsRead", 0)

                # 1. Check for data skew
                criticity = Criticity.HIGH if min_records > 0 and max_records > skew_ratio_threshold * min_records else Criticity.NONE
                DataSkewHeuristic.add_check(
                    checks, "Data Skew", f"maxRecords <= {skew_ratio_threshold} * minRecords", 
                    f"{max_records} vs {min_records}", f"Stage {stage_id} has significant data skew.", criticity
                )

                # 2. Check for empty partitions
                criticity = Criticity.MEDIUM if min_records == 0 else Criticity.NONE
                DataSkewHeuristic.add_check(
                    checks, "Empty Partitions", "minRecords > 0", str(min_records), 
                    f"Stage {stage_id} contains empty partitions, which may indicate poor data distribution.", criticity
                )

                # 3. Check for oversized partitions
                criticity = Criticity.MEDIUM if avg_records > 0 and max_records > 2 * avg_records else Criticity.NONE
                DataSkewHeuristic.add_check(
                    checks, "Oversized Partitions", "maxRecords <= 2 * avgRecords", 
                    f"{max_records} vs {avg_records}", f"Stage {stage_id} has oversized partitions.", criticity
                )

        return pd.DataFrame(checks)

# Example usage:
# df = DataSkewHeuristic.evaluate(all_data)
# print(df)
