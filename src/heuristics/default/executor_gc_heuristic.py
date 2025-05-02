import pandas as pd
from config.config import THRESHOLDS
from heuristics.base_heuristic import BaseHeuristic, Criticity

class ExecutorGcHeuristic(BaseHeuristic):
    """
    Heuristic based on GC time and CPU run time.
    Calculates the ratio of the total time executors spend in GC to the total run time,
    and warns if too much or too little time is spent in GC.
    """

    @staticmethod
    def evaluate(all_data):
        checks = []
        executors = all_data.get("executors", [])
        if not executors:
            return pd.DataFrame([{
                "category": "Garbage Collection",
                "expected": "Executors present",
                "current": "None",
                "description": "No executors found in data.",
                "criticity": Criticity.HIGH.value
            }])

        # Thresholds (can be customized in config)
        gc_severity_a = THRESHOLDS.get("gc_severity_A_threshold", {
            "low": 0.08, "moderate": 0.10, "severe": 0.15, "critical": 0.20, "ascending": True
        })
        gc_severity_d = THRESHOLDS.get("gc_severity_D_threshold", {
            "low": 0.05, "moderate": 0.04, "severe": 0.03, "critical": 0.01, "ascending": False
        })

        # Helper to determine severity
        def get_criticity(ratio, thresholds):
            ascending = thresholds.get("ascending", True)
            if ascending:
                if ratio >= thresholds["critical"]:
                    return Criticity.CRITICAL
                elif ratio >= thresholds["severe"]:
                    return Criticity.SEVERE
                elif ratio >= thresholds["moderate"]:
                    return Criticity.MODERATE
                elif ratio >= thresholds["low"]:
                    return Criticity.LOW
                else:
                    return Criticity.NONE
            else:
                if ratio <= thresholds["critical"]:
                    return Criticity.CRITICAL
                elif ratio <= thresholds["severe"]:
                    return Criticity.SEVERE
                elif ratio <= thresholds["moderate"]:
                    return Criticity.MODERATE
                elif ratio <= thresholds["low"]:
                    return Criticity.LOW
                else:
                    return Criticity.NONE

        # Sum GC time and executor run time (excluding driver)
        jvm_gc_time_total = sum(ex.get("totalGCTime", 0) for ex in executors if ex.get("id") != "driver")
        executor_run_time_total = sum(ex.get("totalDuration", 0) for ex in executors if ex.get("id") != "driver")

        if executor_run_time_total == 0:
            return pd.DataFrame([{
                "category": "Garbage Collection",
                "expected": "Executor runtime > 0",
                "current": "0 ms",
                "description": "Total executor run time is zero, cannot compute GC ratio.",
                "criticity": Criticity.HIGH.value
            }])

        ratio = jvm_gc_time_total / executor_run_time_total

        severity_time_a = get_criticity(ratio, gc_severity_a)
        severity_time_d = get_criticity(ratio, gc_severity_d)

        ExecutorGcHeuristic.add_check(
            checks, "Garbage Collection", f"GC time ratio within range", 
            f"{ratio:.4f}", "GC time to executor runtime ratio.", severity_time_a if severity_time_a != Criticity.NONE else severity_time_d
        )

        ExecutorGcHeuristic.add_check(
            checks, "Garbage Collection", "Total GC time", 
            f"{jvm_gc_time_total} ms", "Total time spent in GC by executors.", Criticity.NONE
        )

        ExecutorGcHeuristic.add_check(
            checks, "Garbage Collection", "Total executor runtime", 
            f"{executor_run_time_total} ms", "Total runtime of Spark executors.", Criticity.NONE
        )

        return pd.DataFrame(checks)

# Example usage:
# df = ExecutorGcHeuristic.evaluate(all_data)
# print(df)
