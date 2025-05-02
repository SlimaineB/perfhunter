import pandas as pd
from datetime import datetime
from config.config import THRESHOLDS
from heuristics.base_heuristic import BaseHeuristic, Criticity

class StagePerformanceHeuristic(BaseHeuristic):
    """Heuristic for evaluating stage performance based on execution time."""

    @staticmethod
    def evaluate(all_data):
        checks = []
        stage_data = all_data.get("stages", [])
        duration_threshold = THRESHOLDS["stage_duration_ms"]

        for stage in stage_data:
            stage_id = stage.get("stageId", "N/A")
            submission_time = stage.get("submissionTime")
            completion_time = stage.get("completionTime")

            if submission_time and completion_time:
                try:
                    # Convert timestamps to datetime objects
                    submission_dt = datetime.strptime(submission_time, "%Y-%m-%dT%H:%M:%S.%fGMT")
                    completion_dt = datetime.strptime(completion_time, "%Y-%m-%dT%H:%M:%S.%fGMT")

                    # Compute duration in milliseconds
                    duration = (completion_dt - submission_dt).total_seconds() * 1000

                    # Set criticity based on duration threshold
                    criticity = Criticity.HIGH if duration > duration_threshold else Criticity.NONE
                    StagePerformanceHeuristic.add_check(
                        checks, "Performance", f"Stage duration <= {duration_threshold} ms",
                        f"{int(duration)} ms", f"Stage {stage_id} took too long to execute.", criticity
                    )
                except ValueError as e:
                    StagePerformanceHeuristic.add_check(
                        checks, "Performance", "Valid timestamp format", "Invalid format",
                        f"Stage {stage_id} has incorrectly formatted timestamps.", Criticity.HIGH
                    )

        return pd.DataFrame(checks)

# Example usage:
# df = StagePerformanceHeuristic.evaluate(all_data)
# print(df)
