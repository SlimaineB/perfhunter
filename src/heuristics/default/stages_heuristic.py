import pandas as pd
from config.config import THRESHOLDS
from heuristics.base_heuristic import BaseHeuristic, Criticity

class StagesHeuristic(BaseHeuristic):
    """
    Heuristic based on metrics for a Spark app's stages.
    Reports stage failures, high task failure rates for each stage, and long average executor runtimes for each stage.
    """

    @staticmethod
    def evaluate(all_data):
        checks = []
        stages = all_data.get("stages", [])
        app_conf = all_data.get("config", {})
        executors = all_data.get("executors", [])
        
        if not stages:
            return pd.DataFrame([{
                "category": "Stage Analysis",
                "expected": "Stages present",
                "current": "None",
                "description": "No stages found in data.",
                "criticity": Criticity.HIGH.value
            }])

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

        def get_criticity(value, thresholds):
            ascending = thresholds.get("ascending", True)
            if ascending:
                return next((c for k, c in thresholds.items() if k != "ascending" and value >= thresholds[k]), Criticity.NONE)
            return next((c for k, c in thresholds.items() if k != "ascending" and value <= thresholds[k]), Criticity.NONE)

        # Stage failure rate analysis
        num_completed_stages = sum(1 for stage in stages if stage.get("status") == "COMPLETE")
        num_failed_stages = sum(1 for stage in stages if stage.get("status") == "FAILED")
        num_stages = num_completed_stages + num_failed_stages
        stage_failure_rate = (num_failed_stages / num_stages) if num_stages > 0 else 0.0
        stage_failure_rate_severity = get_criticity(stage_failure_rate, stage_failure_rate_thresholds)

        StagesHeuristic.add_check(
            checks, "Stage Failure Rate", f"<= {stage_failure_rate_thresholds['critical']}",
            f"{stage_failure_rate:.3f}", "Overall Spark stage failure rate.", stage_failure_rate_severity
        )

        # Task failure rates per stage
        for stage in stages:
            num_complete_tasks = stage.get("numCompleteTasks", 0)
            num_failed_tasks = stage.get("numFailedTasks", 0)
            num_tasks = num_complete_tasks + num_failed_tasks
            task_failure_rate = (num_failed_tasks / num_tasks) if num_tasks > 0 else 0.0
            severity = get_criticity(task_failure_rate, task_failure_rate_thresholds)

            StagesHeuristic.add_check(
                checks, "Task Analysis", f"Failure rate <= {task_failure_rate_thresholds['critical']}",
                f"{task_failure_rate:.3f}", f"Stage {stage.get('stageId')} task failure rate.", severity
            )

        # Long average executor runtimes per stage
        executor_instances = int(app_conf.get("spark.executor.instances", len(executors) if executors else 1))
        for stage in stages:
            executor_run_time = stage.get("executorRunTime", 0)
            avg_executor_runtime = executor_run_time // executor_instances if executor_instances else 0
            severity = get_criticity(avg_executor_runtime, stage_runtime_millis_thresholds)

            StagesHeuristic.add_check(
                checks, "Executor Runtime", f"<= {stage_runtime_millis_thresholds['critical']}",
                f"{avg_executor_runtime} ms", f"Stage {stage.get('stageId')} average executor runtime.", severity
            )

        return pd.DataFrame(checks)

# Example usage:
# df = StagesHeuristic.evaluate(all_data)
# print(df)
