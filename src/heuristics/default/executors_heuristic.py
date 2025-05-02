import pandas as pd
import numpy as np
from config.config import THRESHOLDS
from heuristics.base_heuristic import BaseHeuristic, Criticity

class ExecutorsHeuristic(BaseHeuristic):
    """
    Heuristic based on metrics for a Spark app's executors.
    Evaluates the distribution (min, 25p, median, 75p, max) of key executor metrics.
    The max-to-median ratio determines the severity of any particular metric.
    """

    @staticmethod
    def evaluate(all_data):
        executors = all_data.get("executors", [])
        checks = []

        if not executors:
            return pd.DataFrame([{
                "category": "Executors",
                "expected": "Executors present",
                "current": "None",
                "description": "No executors found in data.",
                "criticity": Criticity.HIGH.value
            }])

        # Helper to extract a list of values for a given key path
        def extract_metric(executors, *keys):
            vals = []
            for ex in executors:
                val = ex
                for k in keys:
                    val = val.get(k, 0)
                vals.append(val)
            return vals

        # Compute distribution stats
        def distribution(values):
            arr = np.array(values)
            return {
                "min": int(np.min(arr)),
                "p25": int(np.percentile(arr, 25)),
                "median": int(np.median(arr)),
                "p75": int(np.percentile(arr, 75)),
                "max": int(np.max(arr))
            }

        # Compute max/median ratio and severity
        def get_criticity(dist, ignore_max_less_than, thresholds):
            if dist["max"] < ignore_max_less_than:
                return Criticity.NONE
            if dist["median"] == 0:
                return Criticity.CRITICAL
            ratio = dist["max"] / dist["median"]
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

        # Thresholds (can be customized in config)
        ratio_thresholds = THRESHOLDS.get("executors_max_to_median_ratio", {
            "low": 1.334, "moderate": 1.778, "severe": 3.162, "critical": 10
        })
        ignore_max_bytes = THRESHOLDS.get("executors_ignore_max_bytes", 100 * 1024 * 1024)
        ignore_max_millis = THRESHOLDS.get("executors_ignore_max_millis", 5 * 60 * 1000)

        # Metrics to analyze: (category, label, extraction keys, ignore threshold)
        metrics = [
            ("Memory", "Storage Memory Used", ["memoryUsed"], ignore_max_bytes),
            ("Performance", "Task Time", ["totalDuration"], ignore_max_millis),
            ("IO", "Input Bytes", ["totalInputBytes"], ignore_max_bytes),
            ("IO", "Shuffle Read Bytes", ["totalShuffleRead"], ignore_max_bytes),
            ("IO", "Shuffle Write Bytes", ["totalShuffleWrite"], ignore_max_bytes),
        ]

        # Total memory allocated/used
        total_storage_memory_allocated = sum(ex.get("maxMemory", 0) for ex in executors)
        total_storage_memory_used = sum(ex.get("memoryUsed", 0) for ex in executors)
        storage_memory_utilization_rate = (
            total_storage_memory_used / total_storage_memory_allocated
            if total_storage_memory_allocated else 0
        )

        # Adding storage memory utilization check
        ExecutorsHeuristic.add_check(
            checks, "Memory", "Usage <= Allocated",
            f"{total_storage_memory_used} / {total_storage_memory_allocated} bytes",
            "Executor storage memory utilization rate.",
            Criticity.MODERATE if storage_memory_utilization_rate > 0.75 else Criticity.NONE
        )

        for category, label, keys, ignore_max in metrics:
            values = extract_metric(executors, *keys)
            dist = distribution(values)
            criticity = get_criticity(dist, ignore_max, ratio_thresholds)

            ExecutorsHeuristic.add_check(
                checks, category, f"max <= {ignore_max}", 
                f"min={dist['min']}, p25={dist['p25']}, median={dist['median']}, p75={dist['p75']}, max={dist['max']}",
                f"{label}: Max-to-median ratio exceeds thresholds.", criticity
            )

        return pd.DataFrame(checks)

# Example usage:
# df = ExecutorsHeuristic.evaluate(all_data)
# print(df)
