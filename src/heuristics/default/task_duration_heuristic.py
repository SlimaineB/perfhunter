import pandas as pd
from config.config import THRESHOLDS
from heuristics.base_heuristic import BaseHeuristic, Criticity

class TaskDurationHeuristic(BaseHeuristic):
    """Heuristic for evaluating task duration in Spark stages."""

    @staticmethod
    def evaluate(all_data):
        checks = []
        stage_data = all_data.get("stages", [])
        task_duration_threshold = THRESHOLDS.get("task_duration_ms", 10000)  # 10 seconds

        for stage in stage_data:
            stage_id = stage.get("stageId", "N/A")
            num_tasks = stage.get("numTasks", 1)  # Prevent division by zero
            total_duration = stage.get("totalDuration", 0)
            avg_task_duration = total_duration / num_tasks

            criticity = Criticity.HIGH if avg_task_duration > task_duration_threshold else Criticity.NONE
            TaskDurationHeuristic.add_check(
                checks, "Task Duration", f"<= {task_duration_threshold} ms", 
                f"{avg_task_duration:.2f} ms", f"Stage {stage_id} has a high average task duration.", criticity
            )

        return pd.DataFrame(checks)

# Example usage:
# df = TaskDurationHeuristic.evaluate(all_data)
# print(df)
