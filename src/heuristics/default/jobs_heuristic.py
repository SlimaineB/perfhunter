import pandas as pd
from config.config import THRESHOLDS
from heuristics.base_heuristic import BaseHeuristic, Criticity

class JobsHeuristic(BaseHeuristic):
    """
    Heuristic based on metrics for a Spark app's jobs.
    Reports job failures and high task failure rates for each job.
    """

    @staticmethod
    def evaluate(all_data):
        checks = []
        jobs = all_data.get("jobs", [])
        if not jobs:
            return pd.DataFrame([{
                "category": "Job Analysis",
                "expected": "Jobs present",
                "current": "None",
                "description": "No jobs found in data.",
                "criticity": Criticity.HIGH.value
            }])

        # Thresholds (can be customized in config)
        job_failure_rate_thresholds = THRESHOLDS.get("job_failure_rate_severity_thresholds", {
            "low": 0.1, "moderate": 0.3, "severe": 0.5, "critical": 0.5, "ascending": True
        })
        task_failure_rate_thresholds = THRESHOLDS.get("job_task_failure_rate_severity_thresholds", {
            "low": 0.1, "moderate": 0.3, "severe": 0.5, "critical": 0.5, "ascending": True
        })

        def get_criticity(value, thresholds):
            ascending = thresholds.get("ascending", True)
            if ascending:
                if value >= thresholds["critical"]:
                    return Criticity.CRITICAL
                elif value >= thresholds["severe"]:
                    return Criticity.SEVERE
                elif value >= thresholds["moderate"]:
                    return Criticity.MODERATE
                elif value >= thresholds["low"]:
                    return Criticity.LOW
                else:
                    return Criticity.NONE
            else:
                if value <= thresholds["critical"]:
                    return Criticity.CRITICAL
                elif value <= thresholds["severe"]:
                    return Criticity.SEVERE
                elif value <= thresholds["moderate"]:
                    return Criticity.MODERATE
                elif value <= thresholds["low"]:
                    return Criticity.LOW
                else:
                    return Criticity.NONE

        # Count jobs by status
        num_completed_jobs = sum(1 for job in jobs if job.get("status") == "SUCCEEDED")
        num_failed_jobs = sum(1 for job in jobs if job.get("status") == "FAILED")
        num_jobs = num_completed_jobs + num_failed_jobs
        job_failure_rate = (num_failed_jobs / num_jobs) if num_jobs > 0 else 0.0
        job_failure_rate_severity = get_criticity(job_failure_rate, job_failure_rate_thresholds)

        JobsHeuristic.add_check(
            checks, "Job Analysis", f"Failure rate <= {job_failure_rate_thresholds['critical']}",
            f"{job_failure_rate:.3f}", "Overall Spark job failure rate.", job_failure_rate_severity
        )

        # Task failure rates per job
        for job in jobs:
            job_id = job.get("jobId", "N/A")
            num_completed_tasks = job.get("numCompletedTasks", 0)
            num_failed_tasks = job.get("numFailedTasks", 0)
            num_tasks = num_completed_tasks + num_failed_tasks
            task_failure_rate = (num_failed_tasks / num_tasks) if num_tasks > 0 else 0.0
            severity = get_criticity(task_failure_rate, task_failure_rate_thresholds)

            JobsHeuristic.add_check(
                checks, "Task Analysis", f"Failure rate <= {task_failure_rate_thresholds['critical']}",
                f"{task_failure_rate:.3f}", f"Job {job_id} task failure rate.", severity
            )

        return pd.DataFrame(checks)

# Example usage:
# df = JobsHeuristic.evaluate(all_data)
# print(df)
