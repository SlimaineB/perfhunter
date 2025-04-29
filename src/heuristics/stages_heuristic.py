from config.config import THRESHOLDS

class StagesHeuristic:
    """
    Heuristic based on metrics for a Spark app's stages.
    Reports stage failures, high task failure rates for each stage, and long average executor runtimes for each stage.
    """

    @staticmethod
    def evaluate(all_data):
        stages = all_data.get("stages", [])
        app_conf = all_data.get("config", {})
        executors = all_data.get("executors", [])
        if not stages:
            return ["No stages found in data."]

        # Thresholds (can be customized in config)
        stage_failure_rate_thresholds = THRESHOLDS.get("stage_failure_rate_severity_thresholds", {
            "low": 0.1, "moderate": 0.3, "severe": 0.5, "critical": 0.5, "ascending": True
        })
        task_failure_rate_thresholds = THRESHOLDS.get("stage_task_failure_rate_severity_thresholds", {
            "low": 0.1, "moderate": 0.3, "severe": 0.5, "critical": 0.5, "ascending": True
        })
        stage_runtime_millis_thresholds = THRESHOLDS.get("stage_runtime_millis_severity_thresholds", {
            "low": 15 * 60 * 1000, "moderate": 30 * 60 * 1000, "severe": 45 * 60 * 1000, "critical": 60 * 60 * 1000, "ascending": True
        })

        def severity_of(value, thresholds):
            if thresholds.get("ascending", True):
                if value >= thresholds["critical"]:
                    return "CRITICAL"
                elif value >= thresholds["severe"]:
                    return "SEVERE"
                elif value >= thresholds["moderate"]:
                    return "MODERATE"
                elif value >= thresholds["low"]:
                    return "LOW"
                else:
                    return "NONE"
            else:
                if value <= thresholds["critical"]:
                    return "CRITICAL"
                elif value <= thresholds["severe"]:
                    return "SEVERE"
                elif value <= thresholds["moderate"]:
                    return "MODERATE"
                elif value <= thresholds["low"]:
                    return "LOW"
                else:
                    return "NONE"

        # Count stages by status
        num_completed_stages = sum(1 for stage in stages if stage.get("status") == "COMPLETE")
        num_failed_stages = sum(1 for stage in stages if stage.get("status") == "FAILED")
        num_stages = num_completed_stages + num_failed_stages
        stage_failure_rate = (num_failed_stages / num_stages) if num_stages > 0 else 0.0
        stage_failure_rate_severity = severity_of(stage_failure_rate, stage_failure_rate_thresholds)

        # Task failure rates per stage
        stages_with_high_task_failure_rates = []
        task_failure_rate_severities = []
        for stage in stages:
            num_complete_tasks = stage.get("numCompleteTasks", 0)
            num_failed_tasks = stage.get("numFailedTasks", 0)
            num_tasks = num_complete_tasks + num_failed_tasks
            if num_tasks == 0:
                task_failure_rate = 0.0
            else:
                task_failure_rate = num_failed_tasks / num_tasks
            severity = severity_of(task_failure_rate, task_failure_rate_thresholds)
            task_failure_rate_severities.append(severity)
            if severity_order(severity) > severity_order("MODERATE"):
                stages_with_high_task_failure_rates.append((stage, task_failure_rate))

        # Long average executor runtimes per stage
        executor_instances = int(app_conf.get("spark.executor.instances", len(executors) if executors else 1))
        stages_with_long_avg_executor_runtimes = []
        runtime_severities = []
        for stage in stages:
            executor_run_time = stage.get("executorRunTime", 0)
            avg_executor_runtime = executor_run_time // executor_instances if executor_instances else 0
            severity = severity_of(avg_executor_runtime, stage_runtime_millis_thresholds)
            runtime_severities.append(severity)
            if severity_order(severity) > severity_order("MODERATE"):
                stages_with_long_avg_executor_runtimes.append((stage, avg_executor_runtime))

        # Overall severity
        severities = [stage_failure_rate_severity] + task_failure_rate_severities + runtime_severities
        severity_levels = ["NONE", "LOW", "MODERATE", "SEVERE", "CRITICAL"]
        overall_severity = max(severities, key=lambda s: severity_levels.index(s))

        # Formatting
        def format_stage_with_high_task_failure_rate(stage, rate):
            return f"stage {stage.get('stageId')}, attempt {stage.get('attemptId')} (task failure rate: {rate:.3f})"

        def format_stage_with_long_runtime(stage, runtime):
            return f"stage {stage.get('stageId')}, attempt {stage.get('attemptId')} (runtime: {readable_timespan(runtime)})"

        details = [
            f"Spark completed stages count: {num_completed_stages}",
            f"Spark failed stages count: {num_failed_stages}",
            f"Spark stage failure rate: {stage_failure_rate:.3f}",
            "Spark stages with high task failure rates:\n" +
                "\n".join(format_stage_with_high_task_failure_rate(s, r) for s, r in stages_with_high_task_failure_rates),
            "Spark stages with long average executor runtimes:\n" +
                "\n".join(format_stage_with_long_runtime(s, rt) for s, rt in stages_with_long_avg_executor_runtimes)
        ]

        return [f"StagesHeuristic severity: {overall_severity}"] + details

def severity_order(severity):
    order = ["NONE", "LOW", "MODERATE", "SEVERE", "CRITICAL"]
    return order.index(severity)

def readable_timespan(ms):
    # Converts milliseconds to a human-readable string
    seconds = ms // 1000
    minutes = seconds // 60
    hours = minutes // 60
    if hours > 0:
        return f"{hours}h {minutes % 60}m"
    elif minutes > 0:
        return f"{minutes}m {seconds % 60}s"
    else:
        return f"{seconds}s"