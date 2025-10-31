# scripts/utils/base_extractor.py
from scripts.utils.base_step import BaseStep
import requests, time


class BaseExtractor(BaseStep):
    """Unified base for TMDB and Discogs extractors."""


    def __init__(self, source_name: str, base_url: str, api_key: str = None):
        super().__init__(f"Extract_{source_name}")
        self.base_url = base_url
        self.api_key = api_key


    def _request(self, endpoint: str, params: dict = None, retries: int = 3):
        for i in range(retries):
            try:
                resp = requests.get(f"{self.base_url}{endpoint}", params=params, timeout=15)
                if resp.status_code == 200:
                    return resp.json()
            except Exception as e:
                self.logger.warning(f"Attempt {i+1} failed: {e}")
                time.sleep(2)
        self.logger.error(f"Failed after {retries} retries.")
        return None
