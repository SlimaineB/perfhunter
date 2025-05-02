import pandas as pd
from config.config import THRESHOLDS
from heuristics.base_heuristic import BaseHeuristic, Criticity

class ExecutorMemoryHeuristic(BaseHeuristic):
    """Heuristic for evaluating executor memory usage in Spark."""

    @staticmethod
    def evaluate(all_data):
        checks = []
        executor_data = all_data.get("executors", [])
        memory_usage_threshold = THRESHOLDS["executor_memory_usage"]

        if not executor_data:
            return pd.DataFrame([{
                "category": "Memory Usage",
                "expected": "Executors present",
                "current": "None",
                "description": "No executors found in data.",
                "criticity": Criticity.HIGH.value
            }])

        for executor in executor_data:
            executor_id = executor.get("id", "N/A")
            memory_used = executor.get("memoryMetrics", {}).get("usedOnHeapStorageMemory", 0)
            memory_total = executor.get("memoryMetrics", {}).get("totalOnHeapStorageMemory", 1)  # Prevent division by zero

            # 1. Check memory utilization
            criticity = Criticity.HIGH if memory_used / memory_total > memory_usage_threshold else Criticity.NONE
            ExecutorMemoryHeuristic.add_check(
                checks, "Memory Usage", f"<= {memory_usage_threshold * 100}%", 
                f"{memory_used}/{memory_total}", f"Executor {executor_id} exceeds on-heap memory usage threshold.", criticity
            )

            # 2. Check JVM heap/off-heap memory peaks
            peak_heap_memory = executor.get("peakMemoryMetrics", {}).get("JVMHeapMemory", 0)
            peak_off_heap_memory = executor.get("peakMemoryMetrics", {}).get("JVMOffHeapMemory", 0)

            criticity = Criticity.SEVERE if peak_heap_memory > 0.8 * memory_total else Criticity.NONE
            ExecutorMemoryHeuristic.add_check(
                checks, "JVM Memory", "Heap peak <= 80% of total",
                f"{peak_heap_memory}", f"Executor {executor_id} reached high JVM on-heap memory peak.", criticity
            )

            criticity = Criticity.MODERATE if peak_off_heap_memory > 0.2 * memory_total else Criticity.NONE
            ExecutorMemoryHeuristic.add_check(
                checks, "JVM Memory", "Off-heap peak <= 20% of total",
                f"{peak_off_heap_memory}", f"Executor {executor_id} reached high JVM off-heap memory peak.", criticity
            )

            # 3. Check failed tasks
            failed_tasks = executor.get("failedTasks", 0)
            criticity = Criticity.HIGH if failed_tasks > 0 else Criticity.NONE
            ExecutorMemoryHeuristic.add_check(
                checks, "Task Stability", "Failed tasks = 0",
                f"{failed_tasks}", f"Executor {executor_id} has failed tasks. Check logs for details.", criticity
            )

        return pd.DataFrame(checks)

# Example usage:
# df = ExecutorMemoryHeuristic.evaluate(all_data)
# print(df)
