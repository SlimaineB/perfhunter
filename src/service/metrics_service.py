import requests

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