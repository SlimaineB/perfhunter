from config.config import THRESHOLDS

class ShuffleReadHeuristic:
    @staticmethod
    def evaluate(all_data):
        recommendations = []
        stage_data = all_data.get("stages", [])
        shuffle_read_threshold = THRESHOLDS["shuffle_read_bytes"]
        shuffle_wait_time_threshold = THRESHOLDS.get("shuffle_wait_time_ms", 5000)  # Temps d'attente max en ms
        remote_vs_local_ratio_threshold = THRESHOLDS.get("remote_vs_local_ratio", 2)  # Ratio max entre remote/local

        for stage in stage_data:
            stage_id = stage.get("stageId", "N/A")

            # 1. Vérifier la taille des données lues en shuffle
            shuffle_read = stage.get("shuffleReadBytes", 0)
            if shuffle_read > shuffle_read_threshold:
                recommendations.append(
                    f"Stage {stage_id} lit beaucoup de données en shuffle : "
                    f"{shuffle_read / 1e6:.2f} Mo (seuil : {shuffle_read_threshold / 1e6:.2f} Mo)."
                )

            # 2. Vérifier le temps d'attente pour les lectures en shuffle
            shuffle_wait_time = stage.get("shuffleFetchWaitTime", 0)
            if shuffle_wait_time > shuffle_wait_time_threshold:
                recommendations.append(
                    f"Stage {stage_id} a un temps d'attente élevé pour les lectures en shuffle : "
                    f"{shuffle_wait_time} ms (seuil : {shuffle_wait_time_threshold} ms)."
                )

            # 3. Vérifier le ratio entre les lectures distantes et locales
            remote_bytes = stage.get("shuffleRemoteBytesRead")
            local_bytes = stage.get("shuffleLocalBytesRead")  # Évite la division par zéro
            if(local_bytes != 0):
                remote_vs_local_ratio = remote_bytes / local_bytes
                if remote_vs_local_ratio > remote_vs_local_ratio_threshold:
                    recommendations.append(
                        f"Stage {stage_id} a un ratio élevé de lectures distantes par rapport aux lectures locales : "
                        f"{remote_vs_local_ratio:.2f} (seuil : {remote_vs_local_ratio_threshold}). "
                        f"Considérez l'optimisation de la localisation des données."
                    )

            # 4. Vérifier le nombre de blocs distants lus
            remote_blocks = stage.get("shuffleRemoteBlocksFetched", 0)
            local_blocks = stage.get("shuffleLocalBlocksFetched", 0)
            if remote_blocks > local_blocks:
                recommendations.append(
                    f"Stage {stage_id} lit plus de blocs distants ({remote_blocks}) que locaux ({local_blocks}). "
                    f"Cela peut entraîner des ralentissements dus à la latence réseau."
                )

        return recommendations if recommendations else "Les métriques de shuffle sont acceptables."