from config.config import THRESHOLDS


class ShuffleWriteHeuristic:
    @staticmethod
    def evaluate(all_data):
        recommendations = []
        stage_data = all_data.get("stages", [])
        shuffle_write_threshold = THRESHOLDS.get("shuffle_write_bytes", 1e9)  # 1 Go

        for stage in stage_data:
            stage_id = stage.get("stageId", "N/A")
            shuffle_write = stage.get("shuffleWriteBytes", 0)
            if shuffle_write > shuffle_write_threshold:
                recommendations.append(
                    f"Stage {stage_id} écrit beaucoup de données en shuffle : "
                    f"{shuffle_write / 1e6:.2f} Mo (seuil : {shuffle_write_threshold / 1e6:.2f} Mo)."
                )

        return recommendations if recommendations else "Les écritures en shuffle sont acceptables."