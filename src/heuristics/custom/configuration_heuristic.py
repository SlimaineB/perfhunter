from config.config import THRESHOLDS

class ConfigurationHeuristic:
    """
    Heuristic based on an app's known configuration.
    Informs users about key app configuration settings and checks if values are within thresholds.
    """

    @staticmethod
    def evaluate(all_data):
        config = all_data.get("config", {})

        # Thresholds (can be customized in config)
        min_driver_memory = THRESHOLDS.get("min_driver_memory", "2g")
        min_executor_memory = THRESHOLDS.get("min_executor_memory", "2g")
        min_executor_cores = THRESHOLDS.get("min_executor_cores", 2)
        min_driver_cores = THRESHOLDS.get("min_driver_cores", 2)
        min_executor_instances = THRESHOLDS.get("min_executor_instances", 2)
        max_executor_memory_overhead = THRESHOLDS.get("max_executor_memory_overhead", "8g")
        max_driver_memory_overhead = THRESHOLDS.get("max_driver_memory_overhead", "8g")
        serializer_recommendation = THRESHOLDS.get("serializer_if_non_null_recommendation", "org.apache.spark.serializer.KryoSerializer")
        threshold_min_executors = THRESHOLDS.get("dynamic_allocation_min_executors", 1)
        threshold_max_executors = THRESHOLDS.get("dynamic_allocation_max_executors", 900)

        def _memory_to_mb(mem_str):
            mem_str = str(mem_str).strip().lower()
            if mem_str.endswith("g"):
                return int(float(mem_str[:-1]) * 1024)
            if mem_str.endswith("m"):
                return int(float(mem_str[:-1]))
            return int(mem_str)

        def format_property(val):
            return val if val else "Not presented. Using default."

        recommendations = []

        # Driver memory
        driver_memory = config.get("spark.driver.memory")
        if driver_memory and _memory_to_mb(driver_memory) < _memory_to_mb(min_driver_memory):
            recommendations.append(f"Driver memory ({driver_memory}) is below recommended minimum ({min_driver_memory}).")

        # Executor memory
        executor_memory = config.get("spark.executor.memory")
        if executor_memory and _memory_to_mb(executor_memory) < _memory_to_mb(min_executor_memory):
            recommendations.append(f"Executor memory ({executor_memory}) is below recommended minimum ({min_executor_memory}).")

        # Executor instances
        executor_instances = config.get("spark.executor.instances")
        if executor_instances and int(executor_instances) < min_executor_instances:
            recommendations.append(f"Executor instances ({executor_instances}) is below recommended minimum ({min_executor_instances}).")

        # Executor cores
        executor_cores = config.get("spark.executor.cores")
        if executor_cores and int(executor_cores) < min_executor_cores:
            recommendations.append(f"Executor cores ({executor_cores}) is below recommended minimum ({min_executor_cores}).")

        # Driver cores
        driver_cores = config.get("spark.driver.cores")
        if driver_cores and int(driver_cores) < min_driver_cores:
            recommendations.append(f"Driver cores ({driver_cores}) is below recommended minimum ({min_driver_cores}).")

        # Serializer
        serializer = config.get("spark.serializer")
        if serializer is None or serializer != serializer_recommendation:
            recommendations.append("KryoSerializer is Not Enabled. It is recommended to use KryoSerializer for better performance (set spark.serializer=org.apache.spark.serializer.KryoSerializer).")

        # Dynamic allocation min/max executors
        min_executors = config.get("spark.dynamicAllocation.minExecutors")
        if min_executors and int(min_executors) > threshold_min_executors:
            recommendations.append("The minimum executors for Dynamic Allocation should be <=1. Please change it in the spark.dynamicAllocation.minExecutors field.")

        max_executors = config.get("spark.dynamicAllocation.maxExecutors")
        if max_executors and int(max_executors) > threshold_max_executors:
            recommendations.append("The maximum executors for Dynamic Allocation should be <=900. Please change it in the spark.dynamicAllocation.maxExecutors field.")

        # Shuffle and dynamic allocation
        dynamic_allocation = config.get("spark.dynamicAllocation.enabled", "false").lower() == "true"
        shuffle_service = config.get("spark.shuffle.service.enabled", "false").lower() == "true"
        if dynamic_allocation and not shuffle_service:
            recommendations.append("Spark shuffle service is not enabled.")
        elif not dynamic_allocation and not shuffle_service:
            recommendations.append("Spark shuffle service is not enabled.")

        # Jars wildcard
        yarn_jars = config.get("spark.yarn.secondary.jars", "")
        if "*" in yarn_jars:
            recommendations.append("It is recommended to not use * notation while specifying jars in the field spark.yarn.secondary.jars.")

        # Overhead memory
        executor_overhead = config.get("spark.yarn.executor.memoryOverhead")
        if executor_overhead and _memory_to_mb(executor_overhead) > _memory_to_mb(max_executor_memory_overhead):
            recommendations.append("Please do not specify excessive amount of overhead memory for Executors. Change it in the field spark.yarn.executor.memoryOverhead.")

        driver_overhead = config.get("spark.yarn.driver.memoryOverhead")
        if driver_overhead and _memory_to_mb(driver_overhead) > _memory_to_mb(max_driver_memory_overhead):
            recommendations.append("Please do not specify excessive amount of overhead memory for Driver. Change it in the field spark.yarn.driver.memoryOverhead.")

        # Application duration (if available)
        app_duration = all_data.get("application_duration")
        if app_duration:
            recommendations.append(f"Application duration: {app_duration} seconds")

        # Output all config values for reference
        details = [
            f"spark.driver.memory: {format_property(driver_memory)}",
            f"spark.executor.memory: {format_property(executor_memory)}",
            f"spark.executor.instances: {format_property(executor_instances)}",
            f"spark.executor.cores: {format_property(executor_cores)}",
            f"spark.driver.cores: {format_property(driver_cores)}",
            f"spark.serializer: {format_property(serializer)}",
            f"spark.dynamicAllocation.enabled: {format_property(config.get('spark.dynamicAllocation.enabled'))}",
            f"spark.shuffle.service.enabled: {format_property(config.get('spark.shuffle.service.enabled'))}",
            f"spark.yarn.executor.memoryOverhead: {format_property(executor_overhead)}",
            f"spark.yarn.driver.memoryOverhead: {format_property(driver_overhead)}",
            f"spark.yarn.secondary.jars: {format_property(yarn_jars)}"
        ]

        return recommendations + details if recommendations else ["Spark configuration parameters are within recommended thresholds."] + details