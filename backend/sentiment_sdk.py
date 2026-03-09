"""
Sentiment SDK — Drop-in client for any Python project
======================================================
Copy this file into your project and use:

    from sentiment_sdk import Sentiment

    client = Sentiment(
        url='https://your-app.com',
        key='sk-sent-...',
    )

    result = client.analyze('Bitcoin is pumping!', source='twitter')
    print(result['label'])  # "POSITIVE"
"""

import requests
from typing import Optional


class Sentiment:
    def __init__(self, url: str, key: str):
        self.url = url.rstrip('/')
        self.key = key

    def _request(self, method: str, path: str, body: dict = None):
        resp = requests.request(
            method,
            f"{self.url}/api/v1/sentiment{path}",
            json=body,
            headers={
                "Content-Type": "application/json",
                "X-API-Key": self.key,
            },
            timeout=10,
        )
        data = resp.json()
        if not data.get("ok"):
            raise Exception(data.get("message") or data.get("error") or "API error")
        return data["data"]

    def analyze(self, text: str, source: Optional[str] = None) -> dict:
        """Analyze a single text."""
        return self._request("POST", "/analyze", {"text": text, "source": source})

    def batch(self, items: list, source: Optional[str] = None) -> dict:
        """Analyze multiple texts at once (up to 100)."""
        return self._request("POST", "/batch", {"items": items, "source": source})

    def normalize(self, text: str) -> dict:
        """Normalize text: cleanup, tokenize, detect language."""
        return self._request("POST", "/normalize", {"text": text})

    def health(self) -> dict:
        """Check engine health."""
        return self._request("GET", "/health")
