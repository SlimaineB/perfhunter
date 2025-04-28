from config.config import THRESHOLDS

class DataSkewHeuristic:
    @staticmethod
    def evaluate(all_data):
        recommendations = []
        stage_data = all_data.get("stages", [])  # Récupère les données des stages
        skew_ratio_threshold = THRESHOLDS["data_skew_ratio"]  # Lire le seuil depuis la config

        for stage in stage_data:
            stage_id = stage.get("stageId", "N/A")
            task_metrics = stage.get("taskMetrics", {})
            if task_metrics:
                # 1. Vérifier le skew des données
                max_records = task_metrics.get("maxRecordsRead", 0)
                min_records = task_metrics.get("minRecordsRead", 0)
                if min_records > 0 and max_records > skew_ratio_threshold * min_records:
                    recommendations.append(
                        f"Stage {stage_id} a un skew important : "
                        f"maxRecordsRead={max_records}, minRecordsRead={min_records} "
                        f"(seuil : {skew_ratio_threshold})."
                    )

                # 2. Vérifier les partitions avec des tailles nulles
                if min_records == 0:
                    recommendations.append(
                        f"Stage {stage_id} contient des partitions vides, ce qui peut indiquer un problème de répartition des données."
                    )

                # 3. Vérifier les partitions surdimensionnées
                avg_records = task_metrics.get("avgRecordsRead", 0)
                if avg_records > 0 and max_records > 2 * avg_records:
                    recommendations.append(
                        f"Stage {stage_id} a des partitions surdimensionnées : "
                        f"maxRecordsRead={max_records}, avgRecordsRead={avg_records}."
                    )

        return recommendations if recommendations else "Aucun skew détecté."