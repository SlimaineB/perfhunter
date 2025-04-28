class SparkAPI:
    def __init__(self, base_url, auth_token):
        self.base_url = base_url
        self.auth_token = auth_token

    def fetch_job_data(self, job_id):
        url = f"{self.base_url}/api/v1/applications/{job_id}/jobs"
        headers = {
            "Authorization": f"Bearer {self.auth_token}"
        }
        response = requests.get(url, headers=headers)
        return self.parse_response(response)

    def parse_response(self, response):
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()