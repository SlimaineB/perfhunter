from config.config import THRESHOLDS


class GCTimeHeuristic:
    @staticmethod
    def evaluate(all_data):
        recommendations = []
        executor_data = all_data.get("executors", [])
        gc_time_threshold = THRESHOLDS.get("gc_time_ms", 10000)  # Temps max en ms

        for executor in executor_data:
            executor_id = executor.get("id", "N/A")
            gc_time = executor.get("totalGCTime", 0)
            if gc_time > gc_time_threshold:
                recommendations.append(
                    f"Executor {executor_id} a passé beaucoup de temps dans le garbage collection : "
                    f"{gc_time} ms (seuil : {gc_time_threshold} ms)."
                )

        return recommendations if recommendations else "Le temps passé dans le garbage collection est acceptable."