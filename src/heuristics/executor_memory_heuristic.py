from config.config import THRESHOLDS

class ExecutorMemoryHeuristic:
    @staticmethod
    def evaluate(all_data):
        recommendations = []
        executor_data = all_data.get("executors", [])
        memory_usage_threshold = THRESHOLDS["executor_memory_usage"]

        for executor in executor_data:
            executor_id = executor.get("id", "N/A")
            memory_used = executor.get("memoryMetrics", {}).get("usedOnHeapStorageMemory", 0)
            memory_total = executor.get("memoryMetrics", {}).get("totalOnHeapStorageMemory", 1)  # Évite la division par zéro

            # 1. Vérifier l'utilisation de la mémoire
            if memory_used / memory_total > memory_usage_threshold:
                recommendations.append(
                    f"Executor {executor_id} utilise plus de {memory_usage_threshold * 100}% de sa mémoire on-heap : "
                    f"{memory_used}/{memory_total}."
                )

            # 2. Vérifier les pics de mémoire JVM
            peak_heap_memory = executor.get("peakMemoryMetrics", {}).get("JVMHeapMemory", 0)
            peak_off_heap_memory = executor.get("peakMemoryMetrics", {}).get("JVMOffHeapMemory", 0)
            if peak_heap_memory > 0.8 * memory_total:
                recommendations.append(
                    f"Executor {executor_id} a atteint un pic élevé de mémoire JVM on-heap : "
                    f"{peak_heap_memory} (80% du total : {memory_total})."
                )
            if peak_off_heap_memory > 0.2 * memory_total:
                recommendations.append(
                    f"Executor {executor_id} a atteint un pic élevé de mémoire JVM off-heap : "
                    f"{peak_off_heap_memory}."
                )

            # 3. Vérifier les tâches échouées
            failed_tasks = executor.get("failedTasks", 0)
            if failed_tasks > 0:
                recommendations.append(
                    f"Executor {executor_id} a {failed_tasks} tâches échouées. "
                    f"Vérifiez les journaux pour plus de détails."
                )

        return recommendations if recommendations else "Utilisation de la mémoire des exécutors correcte."