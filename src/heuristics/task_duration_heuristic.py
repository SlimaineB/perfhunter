from config.config import THRESHOLDS


class TaskDurationHeuristic:
    @staticmethod
    def evaluate(all_data):
        recommendations = []
        stage_data = all_data.get("stages", [])
        task_duration_threshold = THRESHOLDS.get("task_duration_ms", 10000)  # 10 secondes

        for stage in stage_data:
            stage_id = stage.get("stageId", "N/A")
            num_tasks = stage.get("numTasks", 1)  # Évite la division par zéro
            total_duration = stage.get("totalDuration", 0)
            avg_task_duration = total_duration / num_tasks

            if avg_task_duration > task_duration_threshold:
                recommendations.append(
                    f"Stage {stage_id} a une durée moyenne de tâche élevée : "
                    f"{avg_task_duration:.2f} ms (seuil : {task_duration_threshold} ms)."
                )

        return recommendations if recommendations else "Les durées moyennes des tâches sont acceptables."