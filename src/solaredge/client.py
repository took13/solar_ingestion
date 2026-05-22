from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class SolarEdgeApiError(RuntimeError):
    pass


@dataclass
class SolarEdgeResponse:
    endpoint_name: str
    endpoint_path: str
    http_status: int
    response_json: dict[str, Any]
    elapsed_sec: float


class SolarEdgeClient:
    """
    SolarEdge Monitoring API client แบบ read-only

    Important:
    - API key ส่งเป็น query parameter api_key
    - ห้าม print full URL ที่มี api_key
    - Step นี้ยังไม่เขียน DB
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://monitoringapi.solaredge.com",
        timeout_sec: int = 60,
    ):
        if not api_key:
            raise ValueError("SolarEdge api_key is required")

        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec

    def get_site_power(
        self,
        *,
        site_id: str,
        start_time_local: str,
        end_time_local: str,
    ) -> SolarEdgeResponse:
        """
        GET /site/{siteId}/power

        start_time_local/end_time_local format:
        YYYY-MM-DD HH:MM:SS
        """
        endpoint_path = f"/site/{site_id}/power"

        params = {
            "startTime": start_time_local,
            "endTime": end_time_local,
            "api_key": self.api_key,
        }

        return self._get_json(
            endpoint_name="sitePower",
            endpoint_path=endpoint_path,
            params=params,
        )

    def get_energy_details(
        self,
        *,
        site_id: str,
        start_time_local: str,
        end_time_local: str,
        time_unit: str = "QUARTER_OF_AN_HOUR",
        meters: str = "Production,FeedIn,Purchased,SelfConsumption",
    ) -> SolarEdgeResponse:
        """
        GET /site/{siteId}/energyDetails

        meters examples:
        Production,Consumption,SelfConsumption,FeedIn,Purchased
        """
        endpoint_path = f"/site/{site_id}/energyDetails"

        params = {
            "startTime": start_time_local,
            "endTime": end_time_local,
            "timeUnit": time_unit,
            "meters": meters,
            "api_key": self.api_key,
        }

        return self._get_json(
            endpoint_name="energyDetails",
            endpoint_path=endpoint_path,
            params=params,
        )

    def _get_json(
        self,
        *,
        endpoint_name: str,
        endpoint_path: str,
        params: dict[str, Any],
    ) -> SolarEdgeResponse:
        url = f"{self.base_url}{endpoint_path}?{urlencode(params)}"

        request = Request(
            url=url,
            method="GET",
            headers={
                "Accept": "application/json",
                "User-Agent": "solar-ingestion/solaredge-pilot",
            },
        )

        started = time.perf_counter()

        try:
            with urlopen(request, timeout=self.timeout_sec) as response:
                body = response.read().decode("utf-8")
                elapsed = time.perf_counter() - started

                try:
                    payload = json.loads(body)
                except json.JSONDecodeError as exc:
                    raise SolarEdgeApiError(
                        f"{endpoint_name} returned non-JSON response"
                    ) from exc

                return SolarEdgeResponse(
                    endpoint_name=endpoint_name,
                    endpoint_path=endpoint_path,
                    http_status=response.status,
                    response_json=payload,
                    elapsed_sec=elapsed,
                )

        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            safe_message = self._safe_error_message(
                endpoint_name=endpoint_name,
                status_code=exc.code,
                body=body,
            )
            raise SolarEdgeApiError(safe_message) from exc

        except URLError as exc:
            raise SolarEdgeApiError(
                f"{endpoint_name} network error: {exc.reason}"
            ) from exc

    def _safe_error_message(
        self,
        *,
        endpoint_name: str,
        status_code: int,
        body: str,
    ) -> str:
        # ห้ามใส่ api_key ใน error message
        short_body = body[:1000] if body else ""
        return (
            f"{endpoint_name} failed with HTTP {status_code}. "
            f"Response body={short_body}"
        )