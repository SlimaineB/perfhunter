import requests
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
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

    def get_critical_path_duration(self):
        """ Récupère la durée totale de l'application en secondes pour le chemin critique. """
        if self.history_data:
            app_data = self.history_data.get("app", {})
            return sum(
                int(attempt.get("duration", 0) / 1000) for attempt in app_data.get("attempts", [])
                if attempt.get("duration") and attempt.get("completed") is True
            )
        return None
    
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
                    print("Tasks:", tasks)
                    max_time = max(
                        (task.get("duration", 0) for task in tasks if task.get("duration") is not None),
                        default=0
                    )
                    max_task_times[stage_id] = max_time
            return max_task_times
        return None     
    
if __name__ == "__main__":
    # Exemple d'utilisation
    base_url = "http://localhost:18080"
    metrics_service = MetricsService(base_url)
    
    # Liste des applications
    #applications = metrics_service.list_applications(limit=5)
    #print("Applications:", applications)
    
    # Récupération des données d'une application spécifique
    app_id = "app-20250508211358-0001"
    attempt_id = None
    history_data = metrics_service.fetch_all_data(app_id, attempt_id)
    
    # Affichage de la mémoire utilisée
    memory_used = metrics_service.get_max_task_time_per_stage()
    print("Mémoire utilisée:", memory_used)