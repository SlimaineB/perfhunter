from config.config import THRESHOLDS
from datetime import datetime

class StagePerformanceHeuristic:
    @staticmethod
    def evaluate(all_data):
        recommendations = []
        stage_data = all_data.get("stages", [])
        duration_threshold = THRESHOLDS["stage_duration_ms"]

        for stage in stage_data:
            # Calculer la durée du stage à partir de submissionTime et completionTime
            submission_time = stage.get("submissionTime")
            completion_time = stage.get("completionTime")

            if submission_time and completion_time:
                # Convertir les timestamps en objets datetime
                submission_dt = datetime.strptime(submission_time, "%Y-%m-%dT%H:%M:%S.%fGMT")
                completion_dt = datetime.strptime(completion_time, "%Y-%m-%dT%H:%M:%S.%fGMT")

                # Calculer la durée en millisecondes
                duration = (completion_dt - submission_dt).total_seconds() * 1000

                # Vérifier si la durée dépasse le seuil
                if duration > duration_threshold:
                    recommendations.append(
                        f"Stage {stage['stageId']} est lent : durée={int(duration)}ms."
                    )

        return recommendations if recommendations else "Les performances des stages sont acceptables."