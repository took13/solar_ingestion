import time
import requests


class EgressClient:
    def post_json(
        self,
        endpoint_url: str,
        auth_token: str,
        payload: dict,
        timeout_seconds: int = 60,
        max_attempts: int = 3,
        backoff_seconds: int = 60,
    ):
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
        }

        last_response = None
        last_exception = None

        for attempt in range(1, max_attempts + 1):
            try:
                response = requests.post(
                    endpoint_url,
                    json=payload,
                    headers=headers,
                    timeout=timeout_seconds,
                )
                last_response = response

                if response.status_code == 429 and attempt < max_attempts:
                    time.sleep(backoff_seconds)
                    continue

                return response

            except Exception as e:
                last_exception = e
                if attempt < max_attempts:
                    time.sleep(backoff_seconds)
                    continue
                raise

        if last_response is not None:
            return last_response

        raise last_exception