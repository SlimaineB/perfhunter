from config.config import THRESHOLDS
import numpy as np

class ExecutorsHeuristic:
    """
    Heuristic based on metrics for a Spark app's executors.
    Evaluates the distribution (min, 25p, median, 75p, max) of key executor metrics.
    The max-to-median ratio determines the severity of any particular metric.
    """

    @staticmethod
    def evaluate(all_data):
        executors = all_data.get("executors", [])
        if not executors:
            return ["No executors found in data."]

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
        def severity_of_distribution(dist, ignore_max_less_than, thresholds):
            if dist["max"] < ignore_max_less_than:
                return "NONE"
            if dist["median"] == 0:
                return "CRITICAL"
            ratio = dist["max"] / dist["median"]
            if ratio >= thresholds["critical"]:
                return "CRITICAL"
            elif ratio >= thresholds["severe"]:
                return "SEVERE"
            elif ratio >= thresholds["moderate"]:
                return "MODERATE"
            elif ratio >= thresholds["low"]:
                return "LOW"
            else:
                return "NONE"

        # Thresholds (can be customized in config)
        ratio_thresholds = THRESHOLDS.get("executors_max_to_median_ratio", {
            "low": 1.334, "moderate": 1.778, "severe": 3.162, "critical": 10
        })
        ignore_max_bytes = THRESHOLDS.get("executors_ignore_max_bytes", 100 * 1024 * 1024)
        ignore_max_millis = THRESHOLDS.get("executors_ignore_max_millis", 5 * 60 * 1000)

        # Metrics to analyze: (label, extraction keys, ignore threshold)
        metrics = [
            ("storage memory used", ["memoryUsed"], ignore_max_bytes),
            ("task time", ["totalDuration"], ignore_max_millis),
            ("input bytes", ["totalInputBytes"], ignore_max_bytes),
            ("shuffle read bytes", ["totalShuffleRead"], ignore_max_bytes),
            ("shuffle write bytes", ["totalShuffleWrite"], ignore_max_bytes),
        ]

        details = []
        severities = []

        # Total memory allocated/used
        total_storage_memory_allocated = sum(ex.get("maxMemory", 0) for ex in executors)
        total_storage_memory_used = sum(ex.get("memoryUsed", 0) for ex in executors)
        storage_memory_utilization_rate = (
            total_storage_memory_used / total_storage_memory_allocated
            if total_storage_memory_allocated else 0
        )

        details.append(f"Total executor storage memory allocated: {total_storage_memory_allocated} bytes")
        details.append(f"Total executor storage memory used: {total_storage_memory_used} bytes")
        details.append(f"Executor storage memory utilization rate: {storage_memory_utilization_rate:.3f}")

        for label, keys, ignore_max in metrics:
            values = extract_metric(executors, *keys)
            dist = distribution(values)
            sev = severity_of_distribution(dist, ignore_max, ratio_thresholds)
            severities.append(sev)
            details.append(
                f"{label.title()} distribution: min={dist['min']}, p25={dist['p25']}, median={dist['median']}, "
                f"p75={dist['p75']}, max={dist['max']} (max/median ratio={dist['max'] / dist['median'] if dist['median'] else 'inf'}, severity={sev})"
            )

        # Overall severity is the worst one
        severity_order = ["NONE", "LOW", "MODERATE", "SEVERE", "CRITICAL"]
        overall_severity = max(severities, key=lambda s: severity_order.index(s))

        summary = [f"ExecutorsHeuristic severity: {overall_severity}"] + details
        return summary

# Example usage:
# result = ExecutorsHeuristic.evaluate(all_data)