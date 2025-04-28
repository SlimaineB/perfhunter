class FailedTasksHeuristic:
    @staticmethod
    def evaluate(all_data):
        recommendations = []
        job_data = all_data.get("jobs", [])
        stage_data = all_data.get("stages", [])

        # Vérifier les tâches échouées dans les jobs
        for job in job_data:
            job_id = job.get("jobId", "N/A")
            failed_tasks = job.get("numFailedTasks", 0)
            if failed_tasks > 0:
                recommendations.append(
                    f"Job {job_id} a {failed_tasks} tâches échouées. Vérifiez les journaux pour plus de détails."
                )

        # Vérifier les tâches échouées dans les stages
        for stage in stage_data:
            stage_id = stage.get("stageId", "N/A")
            failed_tasks = stage.get("numFailedTasks", 0)
            if failed_tasks > 0:
                recommendations.append(
                    f"Stage {stage_id} a {failed_tasks} tâches échouées. Cela peut indiquer un problème de données ou de configuration."
                )

        return recommendations if recommendations else "Aucune tâche échouée détectée."