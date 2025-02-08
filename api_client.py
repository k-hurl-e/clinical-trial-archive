import requests
from typing import Optional, Dict
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential

class ClinicalTrialsClient:
    def __init__(self):
        self.base_url = "https://clinicaltrials.gov/api/v2/studies"
        self.headers = {
            "Accept": "application/json",
            "User-Agent": "ClinicalTrialsBackup/1.0"
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def get_studies(self,
                   page_token: Optional[str] = None,
                   min_update_date: Optional[datetime] = None) -> Dict:
        """
        Fetch studies from the API.

        Args:
            page_token: Token for pagination
            min_update_date: Only fetch studies updated after this date

        Returns:
            API response as dictionary
        """
        params = {
            'format': 'json',
            'pageSize': 100,
        }

        # Add fields we want to retrieve
        params['fields'] = ','.join([
            'NCTId',
            'BriefTitle',
            'Condition',
            'Intervention',
            'Phase',
            'OverallStatus',
            'LastUpdatePostDate',
            'HasResults'
        ])

        if page_token:
            params['pageToken'] = page_token

        # Add date filter if provided
        if min_update_date:
            date_str = min_update_date.strftime("%Y-%m-%d")
            params['lastUpdatePostDateFrom'] = date_str

        print(f"\nRequesting URL: {self.base_url}")
        print(f"Query parameters: {params}")

        try:
            response = requests.get(
                self.base_url,
                params=params,
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Error making request: {str(e)}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                print(f"Response content: {e.response.text}")
            raise