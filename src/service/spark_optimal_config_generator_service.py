class SparkOptimalConfigGeneratorService:
    """
    Génère une configuration Spark optimale à partir des données du Spark History Server.
    """

    def __init__(self):
        pass

    def suggest_config(self, history_data):
        """
        history_data: dict contenant les clés 'jobs', 'stages', 'executors'
        Retourne un dict avec une suggestion de configuration Spark.
        """
        executors = history_data.get("executors", [])
        stages = history_data.get("stages", [])

        if not executors:
            raise ValueError("Aucune donnée d'executor fournie.")

        # Improved heuristic inspired by Dr. Elephant: use peak usage + safety margin.
        # We recommend executor memory as 20% above the peak memory used by any executor,
        # to provide a safety buffer for memory spikes and avoid OOM errors.
        used_memories = [ex.get("memoryUsed", 0) for ex in executors if ex.get("memoryUsed")]
        max_memory_used = max(used_memories) if used_memories else 0
        # Add 20% safety margin, convert to MB. Minimum 2048 MB to ensure stability.
        peak_memory_mb = int(max_memory_used * 1.2 / (1024 ** 2)) if max_memory_used else 2048

        # We recommend executor cores as the maximum observed, to avoid under-provisioning.
        used_cores = [ex.get("totalCores", 0) for ex in executors if ex.get("totalCores")]
        max_cores_used = max(used_cores) if used_cores else 2

        # We recommend the number of executors as the count of active executors that processed tasks,
        # with a minimum of 2 for parallelism and fault tolerance.
        active_executors = [ex for ex in executors if ex.get("isActive", True) and ex.get("totalTasks", 0) > 0]
        num_executors = max(len(active_executors), 2)

        config = {
            "spark.executor.memory": f"{max(peak_memory_mb, 2048)}m",
            "spark.executor.cores": max(max_cores_used, 2),
            "spark.executor.instances": num_executors,
            "spark.dynamicAllocation.enabled": "false"
        }

        # Driver memory is set to 20% above the largest stage input, with a minimum of 1024 MB,
        # to ensure the driver can handle the largest data shuffle or input.
        if stages:
            max_input = max((stage.get("inputBytes", 0) for stage in stages), default=0)
            driver_memory = int(max_input * 1.2 / (1024 ** 2))  # 20% margin
            config["spark.driver.memory"] = f"{max(driver_memory, 1024)}m"

        # Add a note if memory saturation is detected, to alert the user to possible OOM risks.
        if used_memories and max_memory_used > 0.9 * peak_memory_mb * (1024 ** 2):
            config["note"] = (
                "Warning: executor memory saturation detected. "
                "Consider increasing memory or optimizing your Spark job."
            )

        return config