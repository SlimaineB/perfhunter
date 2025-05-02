import pandas as pd
from enum import Enum
from config.config import THRESHOLDS
from heuristics.base_heuristic import BaseHeuristic, Criticity

class ConfigurationHeuristic(BaseHeuristic):
    """
    Heuristic based on an app's known configuration.
    Returns a DataFrame with expected values, current settings, descriptions, criticity levels, and category.
    """

    @staticmethod
    def evaluate(all_data):
        config = all_data.get("config", {})
        checks = []

        thresholds = {
            "min_driver_memory": THRESHOLDS.get("min_driver_memory", "2g"),
            "min_executor_memory": THRESHOLDS.get("min_executor_memory", "2g"),
            "min_executor_cores": THRESHOLDS.get("min_executor_cores", 2),
            "min_driver_cores": THRESHOLDS.get("min_driver_cores", 2),
            "min_executor_instances": THRESHOLDS.get("min_executor_instances", 2),
            "max_executor_memory_overhead": THRESHOLDS.get("max_executor_memory_overhead", "8g"),
            "max_driver_memory_overhead": THRESHOLDS.get("max_driver_memory_overhead", "8g"),
            "serializer_recommendation": THRESHOLDS.get("serializer_if_non_null_recommendation", "org.apache.spark.serializer.KryoSerializer"),
            "threshold_min_executors": THRESHOLDS.get("dynamic_allocation_min_executors", 1),
            "threshold_max_executors": THRESHOLDS.get("dynamic_allocation_max_executors", 900)
        }

        # Driver memory
        driver_memory = config.get("spark.driver.memory")
        criticity = Criticity.HIGH if driver_memory and ConfigurationHeuristic._memory_to_mb(driver_memory) < ConfigurationHeuristic._memory_to_mb(thresholds["min_driver_memory"]) else None
        ConfigurationHeuristic.add_check(checks, "Memory", thresholds["min_driver_memory"], driver_memory, "Minimum recommended driver memory.", criticity)

        # Executor memory
        executor_memory = config.get("spark.executor.memory")
        criticity = Criticity.HIGH if executor_memory and ConfigurationHeuristic._memory_to_mb(executor_memory) < ConfigurationHeuristic._memory_to_mb(thresholds["min_executor_memory"]) else None
        ConfigurationHeuristic.add_check(checks, "Memory", thresholds["min_executor_memory"], executor_memory, "Minimum recommended executor memory.", criticity)

        # Executor instances
        executor_instances = config.get("spark.executor.instances")
        criticity = Criticity.HIGH if executor_instances and int(executor_instances) < thresholds["min_executor_instances"] else None
        ConfigurationHeuristic.add_check(checks, "Scaling", thresholds["min_executor_instances"], executor_instances, "Minimum recommended executor instances.", criticity)

        # Executor cores
        executor_cores = config.get("spark.executor.cores")
        criticity = Criticity.HIGH if executor_cores and int(executor_cores) < thresholds["min_executor_cores"] else None
        ConfigurationHeuristic.add_check(checks, "CPU", thresholds["min_executor_cores"], executor_cores, "Minimum recommended executor cores.", criticity)

        # Driver cores
        driver_cores = config.get("spark.driver.cores")
        criticity = Criticity.HIGH if driver_cores and int(driver_cores) < thresholds["min_driver_cores"] else None
        ConfigurationHeuristic.add_check(checks, "CPU", thresholds["min_driver_cores"], driver_cores, "Minimum recommended driver cores.", criticity)

        # Serializer
        serializer = config.get("spark.serializer")
        criticity = Criticity.HIGH if serializer != thresholds["serializer_recommendation"] else None
        ConfigurationHeuristic.add_check(checks, "Performance", thresholds["serializer_recommendation"], serializer, "Recommended serializer for better performance.", criticity)

        # Dynamic allocation min/max executors
        min_executors = config.get("spark.dynamicAllocation.minExecutors")
        criticity = Criticity.MEDIUM if min_executors and int(min_executors) > thresholds["threshold_min_executors"] else None
        ConfigurationHeuristic.add_check(checks, "Scaling", thresholds["threshold_min_executors"], min_executors, "Recommended minimum executors for dynamic allocation.", criticity)

        max_executors = config.get("spark.dynamicAllocation.maxExecutors")
        criticity = Criticity.MEDIUM if max_executors and int(max_executors) > thresholds["threshold_max_executors"] else None
        ConfigurationHeuristic.add_check(checks, "Scaling", thresholds["threshold_max_executors"], max_executors, "Recommended maximum executors for dynamic allocation.", criticity)

        # Overhead memory
        executor_overhead = config.get("spark.yarn.executor.memoryOverhead")
        criticity = Criticity.HIGH if executor_overhead and ConfigurationHeuristic._memory_to_mb(executor_overhead) > ConfigurationHeuristic._memory_to_mb(thresholds["max_executor_memory_overhead"]) else None
        ConfigurationHeuristic.add_check(checks, "Memory", thresholds["max_executor_memory_overhead"], executor_overhead, "Recommended maximum executor memory overhead.", criticity)

        driver_overhead = config.get("spark.yarn.driver.memoryOverhead")
        criticity = Criticity.HIGH if driver_overhead and ConfigurationHeuristic._memory_to_mb(driver_overhead) > ConfigurationHeuristic._memory_to_mb(thresholds["max_driver_memory_overhead"]) else None
        ConfigurationHeuristic.add_check(checks, "Memory", thresholds["max_driver_memory_overhead"], driver_overhead, "Recommended maximum driver memory overhead.", criticity)

        return pd.DataFrame(checks)

# Example usage:
# df = ConfigurationHeuristic.evaluate(all_data)
# print(df)
