from config.config import THRESHOLDS

class JobsHeuristic:
    """
    Heuristic based on metrics for a Spark app's jobs.
    Reports job failures and high task failure rates for each job.
    """

    @staticmethod
    def evaluate(all_data):
        jobs = all_data.get("jobs", [])
        if not jobs:
            return ["No jobs found in data."]

        # Thresholds (can be customized in config)
        job_failure_rate_thresholds = THRESHOLDS.get("job_failure_rate_severity_thresholds", {
            "low": 0.1, "moderate": 0.3, "severe": 0.5, "critical": 0.5, "ascending": True
        })
        task_failure_rate_thresholds = THRESHOLDS.get("job_task_failure_rate_severity_thresholds", {
            "low": 0.1, "moderate": 0.3, "severe": 0.5, "critical": 0.5, "ascending": True
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

        # Count jobs by status
        num_completed_jobs = sum(1 for job in jobs if job.get("status") == "SUCCEEDED")
        num_failed_jobs = sum(1 for job in jobs if job.get("status") == "FAILED")
        failed_jobs = [job for job in jobs if job.get("status") == "FAILED"]

        # Job failure rate
        num_jobs = num_completed_jobs + num_failed_jobs
        job_failure_rate = (num_failed_jobs / num_jobs) if num_jobs > 0 else 0.0
        job_failure_rate_severity = severity_of(job_failure_rate, job_failure_rate_thresholds)

        # Task failure rates per job
        jobs_with_high_task_failure_rates = []
        task_failure_rate_severities = []
        for job in jobs:
            num_completed_tasks = job.get("numCompletedTasks", 0)
            num_failed_tasks = job.get("numFailedTasks", 0)
            num_tasks = num_completed_tasks + num_failed_tasks
            if num_tasks == 0:
                task_failure_rate = 0.0
            else:
                task_failure_rate = num_failed_tasks / num_tasks
            severity = severity_of(task_failure_rate, task_failure_rate_thresholds)
            task_failure_rate_severities.append(severity)
            if severity_order(severity) > severity_order("MODERATE"):
                jobs_with_high_task_failure_rates.append((job, task_failure_rate))

        # Overall severity
        severities = [job_failure_rate_severity] + task_failure_rate_severities
        severity_levels = ["NONE", "LOW", "MODERATE", "SEVERE", "CRITICAL"]
        overall_severity = max(severities, key=lambda s: severity_levels.index(s))

        # Formatting
        def format_failed_job(job):
            return f"job {job.get('jobId')}, {job.get('name', '')}"

        def format_job_with_high_task_failure_rate(job, rate):
            return f"job {job.get('jobId')}, {job.get('name', '')} (task failure rate: {rate:.3f})"

        details = [
            f"Spark completed jobs count: {num_completed_jobs}",
            f"Spark failed jobs count: {num_failed_jobs}",
            "Spark failed jobs list:\n" + "\n".join(format_failed_job(j) for j in failed_jobs),
            f"Spark job failure rate: {job_failure_rate:.3f}",
            "Spark jobs with high task failure rates:\n" +
                "\n".join(format_job_with_high_task_failure_rate(j, r) for j, r in jobs_with_high_task_failure_rates)
        ]

        return [f"JobsHeuristic severity: {overall_severity}"] + details

def severity_order(severity):
    order = ["NONE", "LOW", "MODERATE", "SEVERE", "CRITICAL"]
    return order.index(severity)