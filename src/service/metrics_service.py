import requests
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.config import THRESHOLDS
from fetcher.history_server_rest_api_fetcher import HistoryServerRestApiFetcher

class MetricsService:
    def __init__(self, base_url):
        self.base_url = base_url
        self.history_server_fetcher = HistoryServerRestApiFetcher(base_url)

    def list_applications(self, status=None, min_date=None, max_date=None, min_end_date=None, max_end_date=None, limit=None):

        return self.history_server_fetcher.list_applications(
            status=status,
            min_date=min_date,
            max_date=max_date,
            min_end_date=min_end_date,
            max_end_date=max_end_date,
            limit=limit
        )
    
    def fetch_all_data(self, app_id, attempt_id=None):
        """
        Récupère les données des jobs, stages, exécutors et config, et les stocke dans un attribut.
        """
        self.history_data = self.history_server_fetcher.fetch_all_data(app_id, attempt_id)
        return self.history_data  # Renvoie les données pour confirmation

    def get_memory_usage(self):
        """ Récupère la quantité totale de mémoire utilisée par les exécutors. """
        if self.history_data:
            executor_data = self.history_data.get("executors", [])
            return sum(int(ex.get("memoryUsed", 0)) for ex in executor_data if ex.get("memoryUsed"))
        return None  # Retourne `None` si les données n'ont pas été chargées

    def get_total_cores(self):
        """ Récupère le nombre total de cœurs CPU utilisés par les exécutors. """
        if self.history_data:
            executor_data = self.history_data.get("executors", [])
            return sum(int(ex.get("totalCores", 0)) for ex in executor_data if ex.get("totalCores"))
        return None

    def get_total_memory(self):
        """ Récupère le nombre total de cœurs CPU utilisés par les exécutors. """
        if self.history_data:
            executor_data = self.history_data.get("executors", [])
            return sum(int(ex.get("totalCores", 0)) for ex in executor_data if ex.get("totalCores"))
        return None

    def get_number_of_executors(self):
        """ Récupère le nombre total d'exécuteurs actifs. """
        if self.history_data:
            executor_data = self.history_data.get("executors", [])
            return len(executor_data)
        return None

    def get_application_duration(self):
        """ Récupère la durée totale de l'application en secondes. """
        if self.history_data:
            app_data = self.history_data.get("app", {})
            return sum(
                int(attempt.get("duration", 0) / 1000) for attempt in app_data.get("attempts", [])
                if attempt.get("duration") and attempt.get("completed") is True
            )
        return None
    
    def get_successfull_attempts(self):
        """ Récupère l'attempt succesfull """
        if self.history_data:
            app_data = self.history_data.get("app", {})
            for attempt in app_data.get("attempts", []):
                if attempt.get("duration") and attempt.get("completed") is True:
                    return attempt.get("attemptId")
        return None    

    def get_critical_path_duration_in_sec(self):
        """ Récupère la durée totale de l'application en secondes pour le chemin critique. """
        max_taskduration_per_stage =   self.get_max_task_time_per_stage()
        return (THRESHOLDS.get("driver_time_ms") + sum (value for key,  value in max_taskduration_per_stage.items()))/1000 if max_taskduration_per_stage else None

    
    def get_max_task_time_per_stage(self):
        """ Parcourt les tâches de chaque stage et trouve le temps maximum. """
        if self.history_data:
            app_id = self.history_data.get("app", []).get("id")
            attempt_id = self.history_data.get("app", []).get("attemptId")
            print("App ID:", app_id)
            stages_data = self.history_data.get("stages", [])
            max_task_times = {}
            for stage in stages_data:
                if stage.get("status") == "COMPLETE":  # Filtrer les stages complets
                    stage_id = stage.get("stageId")
                    #print(stage_with_tasks[0])
                    tasks = stage.get("tasks", [])
                    print("Stage  :", stage_id)
                    max_time = max(
                        (task.get("duration", 0) for key, task in tasks.items() if task.get("duration") is not None),
                        default=0
                    )
                    max_task_times[stage_id] = max_time
            return max_task_times
        return None     

    def get_ratio_off_heap_memory(self):
        """ Récupère le ratio de la mémoire utilisée par rapport à la mémoire totale. """
        if self.history_data:
            executor_data = self.history_data.get("executors", [])
            total_memory = sum(int(ex.get("memoryMetrics", {}).get("totalOffHeapStorageMemory", 0)) for ex in executor_data  if ex.get("id") != "driver" and ex.get("memoryMetrics", {}))
            used_memory = sum(int(ex.get("peakMemoryMetrics", {}).get("JVMOffHeapMemory", 0)) for ex in executor_data  if ex.get("id") != "driver" and ex.get("peakMemoryMetrics", {}))
            print("Total Off Heap memory:", total_memory)
            print("Used Off Heap memory:", used_memory)
            return used_memory / total_memory if total_memory > 0 else None
        return None

    def get_ratio_on_heap_memory(self):
        """ Récupère le ratio de la mémoire utilisée par rapport à la mémoire totale. """
        if self.history_data:
            executor_data = self.history_data.get("executors", [])
            total_memory = sum(int(ex.get("memoryMetrics", {}).get("totalOnHeapStorageMemory", 0)) for ex in executor_data if ex.get("id") != "driver" and ex.get("memoryMetrics", {}))
            used_memory = sum(int(ex.get("peakMemoryMetrics", {}).get("JVMHeapMemory", 0)) for ex in executor_data if ex.get("id") != "driver" and ex.get("peakMemoryMetrics", {}))
            print("Total On Heap memory:", total_memory)
            print("Used On Heap memory:", used_memory)
            return used_memory / total_memory if total_memory > 0 else None
        return None

    def get_configured_heap_memory(self):
        """ Récupère la mémoire totale configurée pour l'application, en octets. """
        if self.history_data:
            config_data = self.history_data.get("config", {})
            heap_memory = config_data.get("spark.executor.memory", "1g")

            # Extraction du nombre et du suffixe
            size_units = {"k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}
            for unit, factor in size_units.items():
                if heap_memory.lower().endswith(unit):
                    numeric_value = heap_memory[:-1]  # Supprime le suffixe
                    if numeric_value.isdigit():  # Vérifie que c'est bien un nombre
                        return int(numeric_value) * factor  # Convertit en octets

        return 0  # Retourne 0 si aucune valeur valide trouvée

    def get_configured_user_memory(self):
        """ Récupère la mémoire totale configurée pour l'application, en octets. """
        configured_heap_memory = self.get_configured_heap_memory()
        if self.history_data:
            config_data = self.history_data.get("config", {})
            memory_fraction = config_data.get("spark.memory.fraction", 0.6)
            return configured_heap_memory - 300*1024 * (1 - memory_fraction)
        return None


    # Storage + Execution = (Configured Heap - 300Mb ) * spark.memory.fraction (default 0.6)
    def get_total_available_spark_memory(self):
        """ Récupère la mémoire totale  disponible : Storage +  Execution = (Configured Heap - 300Mb )* 0.6 """
        if self.history_data:
            executor_data = self.history_data.get("executors", [])
            total_memory = sum(int(ex.get("maxMemory",0)) for ex in executor_data  if ex.get("id") != "driver" and ex.get("maxMemory",0))
            print("Total Available Memory (Storage + Executor)  :", total_memory)
            return total_memory 
        return None

    def get_total_available_storage_memory(self):
        """ Récupère la mémoire totale  disponible : Storage +  Execution = (Configured Heap -300Mb )* 0.6 """
        config_data = self.history_data.get("config", {})
        storageFraction = config_data.get("spark.memory.storageFraction", 0.5) # TODO: Replace 0.5 with value from documentation of the current spark version
        total_memory = self.get_total_available_spark_memory()
        if total_memory:
            storage_memory = total_memory * storageFraction
            print("Total Available Storage Memory  :", storage_memory)
            return storage_memory
        return None

    def get_total_available_execution_memory(self):
        """ Récupère la mémoire totale  disponible : Storage +  Execution = (Configured Heap -300Mb )* 0.6 """
        config_data = self.history_data.get("config", {})
        storageFraction = config_data.get("spark.memory.storageFraction", 0.5) # TODO: Replace 0.5 with value from documentation of the current spark version
        total_memory = self.get_total_available_spark_memory()
        if total_memory:
            execution_memory = total_memory * (1 - storageFraction)
            print("Total Available Execution Memory  :", execution_memory)
            return execution_memory
        return None

    def get_num_of_executors(self):
        """ Récupère la mémoire totale  disponible : Storage +  Execution = (Configured Heap -300Mb )* 0.6 """
        executor_data = self.history_data.get("executors", [])
        if executor_data:
            num_executors = len(executor_data) - 1 # Exclure le driver
            print("Number of Executors  :", num_executors)
            return num_executors
        return None
    
if __name__ == "__main__":
    # Exemple d'utilisation
    base_url = "http://localhost:18080"
    metrics_service = MetricsService(base_url)
    
    # Liste des applications
    #applications = metrics_service.list_applications(limit=5)
    #print("Applications:", applications)
    
    # Récupération des données d'une application spécifique
    #app_id = "app-20250508211358-0001"
    app_id = "app-20250508204032-0000"
    attempt_id = None
    history_data = metrics_service.fetch_all_data(app_id, attempt_id)
    
    # Affichage de la mémoire utilisée
    print("Max task time per stage", metrics_service.get_max_task_time_per_stage())
    print("Ctritical path duration", metrics_service.get_critical_path_duration_in_sec())
    
    print("Ratio on heap memory:", metrics_service.get_ratio_on_heap_memory())
    print("Ratio off heap memory:", metrics_service.get_ratio_off_heap_memory())
    print("Configured heap memory per executor:", metrics_service.get_configured_heap_memory())
    print("Configured user memory per executor:", metrics_service.get_configured_user_memory())
    print( "Total available Spark memory (all executors):", metrics_service.get_total_available_spark_memory())
    print( "Total available Storage memory (all executors):", metrics_service.get_total_available_storage_memory())
    print( "Total available Execution memory (all executors):", metrics_service.get_total_available_execution_memory())
    print( "Total Num executors:", metrics_service.get_num_of_executors())