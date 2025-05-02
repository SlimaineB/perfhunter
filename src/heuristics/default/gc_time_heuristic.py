import pandas as pd
from config.config import THRESHOLDS
from heuristics.base_heuristic import BaseHeuristic, Criticity

class GCTimeHeuristic(BaseHeuristic):
    """Heuristic for evaluating garbage collection time in Spark executors."""

    @staticmethod
    def evaluate(all_data):
        checks = []
        executor_data = all_data.get("executors", [])
        gc_time_threshold = THRESHOLDS.get("gc_time_ms", 10000)  # Max GC time in ms

        for executor in executor_data:
            executor_id = executor.get("id", "N/A")
            gc_time = executor.get("totalGCTime", 0)

            criticity = Criticity.HIGH if gc_time > gc_time_threshold else Criticity.NONE
            GCTimeHeuristic.add_check(
                checks, "Garbage Collection", f"GC time <= {gc_time_threshold} ms", 
                f"{gc_time} ms", f"Executor {executor_id} spent significant time in garbage collection.", criticity
            )

        return pd.DataFrame(checks)

# Example usage:
# df = GCTimeHeuristic.evaluate(all_data)
# print(df)
