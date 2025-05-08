import requests

class HistoryServerRestApiFetcher:
    def __init__(self, base_url):
        self.base_url = base_url


    def fetch_app_data(self, app_id, attempt_id=None):
        url = f"{self.base_url}/api/v1/applications/{app_id}"
        if attempt_id:
            url += f"/{attempt_id}"
        response = requests.get(url)
        return self.parse_response(response)



    def fetch_job_data(self, app_id, attempt_id=None):
        url = f"{self.base_url}/api/v1/applications/{app_id}"
        if attempt_id:
            url += f"/{attempt_id}"
        url += "/jobs"
        response = requests.get(url)
        return self.parse_response(response)

    def fetch_stage_data(self, app_id, attempt_id=None):
        url = f"{self.base_url}/api/v1/applications/{app_id}"
        if attempt_id:
            url += f"/{attempt_id}"
        url += "/stages"
        response = requests.get(url)
        return self.parse_response(response)

    def fetch_executor_data(self, app_id, attempt_id=None):
        url = f"{self.base_url}/api/v1/applications/{app_id}"
        if attempt_id:
            url += f"/{attempt_id}"
        url += "/executors"
        response = requests.get(url)
        return self.parse_response(response)

    def fetch_config_data(self, app_id, attempt_id=None):
        """
        Fetches the Spark configuration (environment) for the given application.
        """
        url = f"{self.base_url}/api/v1/applications/{app_id}"
        if attempt_id:
            url += f"/{attempt_id}"
        url += "/environment"
        response = requests.get(url)
        return self.parse_response(response)

    def fetch_all_data(self, app_id, attempt_id=None):
        """
        Récupère les données des jobs, stages, executors et config, et les combine dans un JSON.
        """
        try:
            app_data = self.fetch_app_data(app_id, attempt_id)
            job_data = self.fetch_job_data(app_id, attempt_id)
            stage_data = self.fetch_stage_data(app_id, attempt_id)
            executor_data = self.fetch_executor_data(app_id, attempt_id)
            config_env = self.fetch_config_data(app_id, attempt_id)
            # sparkProperties is a list of [key, value] pairs
            config_dict = dict(config_env.get("sparkProperties", [])) if config_env else {}

            combined_data = {
                "app": app_data,
                "jobs": job_data,
                "stages": stage_data,
                "executors": executor_data,
                "config": config_dict
            }

            return combined_data
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Erreur lors de la récupération des données : {e}")

    def parse_response(self, response):
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def list_applications(self, status=None, min_date=None, max_date=None, min_end_date=None, max_end_date=None, limit=None):
        """
        Récupère la liste de toutes les applications Spark connues par le Spark History Server, avec filtres optionnels.
        """
        params = {}
        if status:
            params["status"] = status
        if min_date:
            params["minDate"] = min_date
        if max_date:
            params["maxDate"] = max_date
        if min_end_date:
            params["minEndDate"] = min_end_date
        if max_end_date:
            params["maxEndDate"] = max_end_date
        if limit:
            params["limit"] = limit

        url = f"{self.base_url}/api/v1/applications"
        response = requests.get(url, params=params)
        return self.parse_response(response)

