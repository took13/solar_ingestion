import requests


class EnserveClient:
    def post_batch(self, endpoint_url: str, token: str, payload: dict, timeout_seconds: int = 60):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }

        return requests.post(
            endpoint_url,
            json=payload,
            headers=headers,
            timeout=timeout_seconds,
        )