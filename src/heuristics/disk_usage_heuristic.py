from config.config import THRESHOLDS


class DiskUsageHeuristic:
    @staticmethod
    def evaluate(all_data):
        recommendations = []
        executor_data = all_data.get("executors", [])
        disk_usage_threshold = THRESHOLDS.get("disk_usage_bytes", 1e9)  # 1 Go

        for executor in executor_data:
            executor_id = executor.get("id", "N/A")
            disk_used = executor.get("diskUsed", 0)
            if disk_used > disk_usage_threshold:
                recommendations.append(
                    f"Executor {executor_id} utilise beaucoup d'espace disque : "
                    f"{disk_used / 1e6:.2f} Mo (seuil : {disk_usage_threshold / 1e6:.2f} Mo)."
                )

        return recommendations if recommendations else "L'utilisation du disque est acceptable."