import requests

class SparkAPI:
    def __init__(self, base_url):
        self.base_url = base_url

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

    def parse_response(self, response):
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()