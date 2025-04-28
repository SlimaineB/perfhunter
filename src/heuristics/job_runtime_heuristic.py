from config.config import THRESHOLDS
from datetime import datetime

class JobRuntimeHeuristic:
    @staticmethod
    def evaluate(all_data):
        recommendations = []
        job_data = all_data.get("jobs", [])
        duration_threshold = THRESHOLDS["job_duration_ms"]

        for job in job_data:
            job_id = job.get("jobId", "N/A")
            submission_time = job.get("submissionTime")
            completion_time = job.get("completionTime")

            if submission_time and completion_time:
                # Convertir les timestamps en objets datetime
                submission_dt = datetime.strptime(submission_time, "%Y-%m-%dT%H:%M:%S.%fGMT")
                completion_dt = datetime.strptime(completion_time, "%Y-%m-%dT%H:%M:%S.%fGMT")

                # Calculer la durée en millisecondes
                duration = (completion_dt - submission_dt).total_seconds() * 1000

                # Vérifier si la durée dépasse le seuil
                if duration > duration_threshold:
                    recommendations.append(
                        f"Job {job_id} est long : durée={int(duration)}ms (seuil : {duration_threshold}ms)."
                    )

            # Vérifier si le job a échoué
            if job.get("status") != "SUCCEEDED":
                recommendations.append(
                    f"Job {job_id} a échoué ou n'est pas terminé (statut : {job.get('status')})."
                )

        return recommendations if recommendations else "Les durées des jobs sont acceptables."