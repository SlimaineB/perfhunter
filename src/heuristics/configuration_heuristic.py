from config.config import THRESHOLDS

class ConfigurationHeuristic:
    """
    Heuristic that checks key Spark configuration parameters and provides recommendations.
    Inspired by Dr. Elephant's ConfigurationHeuristic.
    """

    @staticmethod
    def evaluate(all_data):
        recommendations = []
        config = all_data.get("config", {})

        # Check driver memory
        driver_memory = config.get("spark.driver.memory")
        if driver_memory:
            min_driver_memory = THRESHOLDS.get("min_driver_memory", "2g")
            if _memory_to_mb(driver_memory) < _memory_to_mb(min_driver_memory):
                recommendations.append(
                    f"Driver memory ({driver_memory}) is below recommended minimum ({min_driver_memory})."
                )

        # Check executor memory
        executor_memory = config.get("spark.executor.memory")
        if executor_memory:
            min_executor_memory = THRESHOLDS.get("min_executor_memory", "2g")
            if _memory_to_mb(executor_memory) < _memory_to_mb(min_executor_memory):
                recommendations.append(
                    f"Executor memory ({executor_memory}) is below recommended minimum ({min_executor_memory})."
                )

        # Check executor cores
        executor_cores = config.get("spark.executor.cores")
        if executor_cores:
            min_executor_cores = THRESHOLDS.get("min_executor_cores", 2)
            if int(executor_cores) < min_executor_cores:
                recommendations.append(
                    f"Executor cores ({executor_cores}) is below recommended minimum ({min_executor_cores})."
                )

        # Check executor instances
        executor_instances = config.get("spark.executor.instances")
        if executor_instances:
            min_executor_instances = THRESHOLDS.get("min_executor_instances", 2)
            if int(executor_instances) < min_executor_instances:
                recommendations.append(
                    f"Executor instances ({executor_instances}) is below recommended minimum ({min_executor_instances})."
                )

        # Check serializer
        serializer = config.get("spark.serializer")
        if serializer and serializer != "org.apache.spark.serializer.KryoSerializer":
            recommendations.append(
                "It is recommended to use KryoSerializer for better performance (set spark.serializer=org.apache.spark.serializer.KryoSerializer)."
            )

        # Check dynamic allocation and shuffle service
        dynamic_allocation = config.get("spark.dynamicAllocation.enabled", "false").lower() == "true"
        shuffle_service = config.get("spark.shuffle.service.enabled", "false").lower() == "true"
        if dynamic_allocation and not shuffle_service:
            recommendations.append(
                "Dynamic allocation is enabled but shuffle service is disabled. Enable shuffle service for dynamic allocation to work properly."
            )

        # Check memory overhead (if present)
        executor_overhead = config.get("spark.yarn.executor.memoryOverhead")
        if executor_overhead:
            max_executor_overhead = THRESHOLDS.get("max_executor_memory_overhead", "8g")
            if _memory_to_mb(executor_overhead) > _memory_to_mb(max_executor_overhead):
                recommendations.append(
                    f"Executor memory overhead ({executor_overhead}) is above recommended maximum ({max_executor_overhead})."
                )

        driver_overhead = config.get("spark.yarn.driver.memoryOverhead")
        if driver_overhead:
            max_driver_overhead = THRESHOLDS.get("max_driver_memory_overhead", "8g")
            if _memory_to_mb(driver_overhead) > _memory_to_mb(max_driver_overhead):
                recommendations.append(
                    f"Driver memory overhead ({driver_overhead}) is above recommended maximum ({max_driver_overhead})."
                )

        # Check for wildcard in jars
        yarn_jars = config.get("spark.yarn.secondary.jars", "")
        if "*" in yarn_jars:
            recommendations.append(
                "Avoid using wildcard '*' in spark.yarn.secondary.jars for better reliability."
            )

        return recommendations if recommendations else ["Spark configuration parameters are within recommended thresholds."]

def _memory_to_mb(mem_str):
    """
    Convert Spark memory string (e.g., '2g', '512m') to MB.
    """
    mem_str = str(mem_str).strip().lower()
    if mem_str.endswith("g"):
        return int(float(mem_str[:-1]) * 1024)
    if mem_str.endswith("m"):
        return int(float(mem_str[:-1]))
    return int(mem_str)