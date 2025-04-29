from config.config import THRESHOLDS

class ExecutorGcHeuristic:
    """
    Heuristic based on GC time and CPU run time.
    Calculates the ratio of the total time executors spend in GC to the total run time,
    and warns if too much or too little time is spent in GC.
    """

    @staticmethod
    def evaluate(all_data):
        executors = all_data.get("executors", [])
        if not executors:
            return ["No executors found in data."]

        # Thresholds (can be customized in config)
        gc_severity_a = THRESHOLDS.get("gc_severity_A_threshold", {
            "low": 0.08, "moderate": 0.10, "severe": 0.15, "critical": 0.20, "ascending": True
        })
        gc_severity_d = THRESHOLDS.get("gc_severity_D_threshold", {
            "low": 0.05, "moderate": 0.04, "severe": 0.03, "critical": 0.01, "ascending": False
        })

        # Helper to determine severity
        def severity_of(ratio, thresholds):
            if thresholds.get("ascending", True):
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
            else:
                if ratio <= thresholds["critical"]:
                    return "CRITICAL"
                elif ratio <= thresholds["severe"]:
                    return "SEVERE"
                elif ratio <= thresholds["moderate"]:
                    return "MODERATE"
                elif ratio <= thresholds["low"]:
                    return "LOW"
                else:
                    return "NONE"

        # Sum GC time and executor run time (excluding driver)
        jvm_gc_time_total = 0
        executor_run_time_total = 0
        for ex in executors:
            if ex.get("id") == "driver":
                continue
            jvm_gc_time_total += ex.get("totalGCTime", 0)
            executor_run_time_total += ex.get("totalDuration", 0)

        if executor_run_time_total == 0:
            return ["Total executor run time is zero, cannot compute GC ratio."]

        ratio = jvm_gc_time_total / executor_run_time_total

        severity_time_a = severity_of(ratio, gc_severity_a)
        severity_time_d = severity_of(ratio, gc_severity_d)

        details = [
            f"GC time to Executor Run time ratio: {ratio:.4f}",
            f"Total GC time: {jvm_gc_time_total} ms",
            f"Total Executor Runtime: {executor_run_time_total} ms"
        ]

        if severity_order(severity_time_a) > severity_order("LOW"):
            details.append("GC ratio high: The job is spending too much time on GC. We recommend increasing the executor memory.")

        if severity_order(severity_time_d) > severity_order("LOW"):
            details.append("GC ratio low: The job is spending too little time in GC. Please check if you have asked for more executor memory than required.")

        return details

def severity_order(severity):
    order = ["NONE", "LOW", "MODERATE", "SEVERE", "CRITICAL"]
    return order.index(severity)