from __future__ import annotations

import json
from typing import Any

import requests

from src.api.exceptions import (
    HuaweiApiError,
    HuaweiLoginError,
    HuaweiRateLimitError,
    HuaweiUnauthorizedError,
)


class HuaweiLegacyClient:
    """
    Huawei FusionSolar / SmartPVMS legacy client for /thirdData/* endpoints.
    """

    def __init__(
        self,
        session_manager,
        base_url: str,
        timeout: int = 120,
        verify_ssl: bool = True,
    ):
        self.session_manager = session_manager
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.verify_ssl = verify_ssl

    def login(self) -> dict:
        try:
            token = self.session_manager.get_token(force_refresh=True)
            return {
                "success": True,
                "token": token,
            }
        except Exception as e:
            raise HuaweiLoginError(f"Login failed: {e}") from e

    def get_station_real_kpi(self, station_codes: list[str]) -> dict:
        if not station_codes:
            raise ValueError("station_codes must not be empty")

        payload = {
            "stationCodes": ",".join(station_codes),
        }
        return self._post_with_auth(
            endpoint="/thirdData/getStationRealKpi",
            json_body=payload,
            request_name="getStationRealKpi",
        )

    def get_dev_real_kpi(self, dev_type_id: int, dev_ids: list[int]) -> dict:
        if not dev_ids:
            raise ValueError("dev_ids must not be empty")

        payload = {
            "devTypeId": dev_type_id,
            "devIds": ",".join(str(x) for x in dev_ids),
        }
        return self._post_with_auth(
            endpoint="/thirdData/getDevRealKpi",
            json_body=payload,
            request_name="getDevRealKpi",
        )

    def get_dev_history_kpi(
        self,
        dev_type_id: int,
        dev_ids: list[int],
        start_time_ms: int,
        end_time_ms: int,
    ) -> dict:
        if not dev_ids:
            raise ValueError("dev_ids must not be empty")
        if end_time_ms <= start_time_ms:
            raise ValueError("end_time_ms must be greater than start_time_ms")

        payload = {
            "devTypeId": dev_type_id,
            "devIds": ",".join(str(x) for x in dev_ids),
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }
        return self._post_with_auth(
            endpoint="/thirdData/getDevHistoryKpi",
            json_body=payload,
            request_name="getDevHistoryKpi",
        )

    def _post_with_auth(
        self,
        endpoint: str,
        json_body: dict[str, Any],
        request_name: str,
    ) -> dict:
        token = self.session_manager.get_token(force_refresh=False)

        try:
            return self._post_once(
                endpoint=endpoint,
                json_body=json_body,
                token=token,
                request_name=request_name,
            )
        except HuaweiUnauthorizedError:
            self.session_manager.invalidate()
            refreshed_token = self.session_manager.get_token(force_refresh=True)
            return self._post_once(
                endpoint=endpoint,
                json_body=json_body,
                token=refreshed_token,
                request_name=request_name,
            )

    def _post_once(
        self,
        endpoint: str,
        json_body: dict[str, Any],
        token: str,
        request_name: str,
    ) -> dict:
        url = f"{self.base_url}{endpoint}"
        session = self.session_manager.get_session()
        session.headers.update({
            "XSRF-TOKEN": token,
            "Content-Type": "application/json",
            "Accept": "application/json, */*",
        })

        try:
            resp = session.post(
                url,
                json=json_body,
                timeout=self.timeout,
                verify=self.verify_ssl,
            )
        except requests.Timeout:
            raise
        except requests.ConnectionError:
            raise
        except Exception as e:
            raise HuaweiApiError(f"{request_name} request failed before response: {e}") from e

        return self._handle_response(
            resp=resp,
            request_name=request_name,
            request_payload=json_body,
        )

    def _handle_response(
        self,
        resp: requests.Response,
        request_name: str,
        request_payload: dict[str, Any],
    ) -> dict:
        body = self._safe_json(resp, request_name)

        http_status = resp.status_code
        success = bool(body.get("success", False)) if isinstance(body, dict) else False
        fail_code = body.get("failCode") if isinstance(body, dict) else None
        message = body.get("message") if isinstance(body, dict) else None

        if http_status == 401:
            self.session_manager.invalidate()
            raise HuaweiUnauthorizedError(
                f"{request_name} HTTP 401 Unauthorized | payload={request_payload}"
            )

        if http_status == 429:
            raise HuaweiRateLimitError(
                f"{request_name} HTTP 429 Too Many Requests | payload={request_payload}"
            )

        if http_status >= 500:
            raise HuaweiApiError(
                f"{request_name} HTTP {http_status} server error | message={message} | payload={request_payload}"
            )

        if http_status >= 400 and http_status not in (401, 429):
            raise HuaweiApiError(
                f"{request_name} HTTP {http_status} client error | failCode={fail_code} | message={message} | payload={request_payload}"
            )

        if not success:
            self._raise_body_error(
                request_name=request_name,
                fail_code=fail_code,
                message=message,
                payload=request_payload,
                body=body,
            )

        return {
            "success": success,
            "http_status": http_status,
            "fail_code": fail_code,
            "message": message,
            "data": body.get("data"),
            "params": body.get("params"),
            "raw_body": body,
        }

    def _raise_body_error(
        self,
        request_name: str,
        fail_code: Any,
        message: Any,
        payload: dict[str, Any],
        body: dict[str, Any],
    ) -> None:
        text = f"{fail_code} {message}".lower() if message is not None else str(fail_code).lower()

        if fail_code == 407 or "407" in text or "access_frequency_is_too_high" in text:
            raise HuaweiRateLimitError(
                f"{request_name} rate limited | failCode={fail_code} | message={message} | payload={payload}"
            )

        if fail_code == 429 or "too many requests" in text:
            raise HuaweiRateLimitError(
                f"{request_name} rate limited | failCode={fail_code} | message={message} | payload={payload}"
            )

        auth_signals = [
            "invalid_credential",
            "must_relogin",
            "relogin",
            "user.login",
            "token",
            "credential",
        ]
        if fail_code in (305, 401) or any(sig in text for sig in auth_signals):
            self.session_manager.invalidate()
            raise HuaweiUnauthorizedError(
                f"{request_name} auth invalid | failCode={fail_code} | message={message} | payload={payload}"
            )

        if request_name == "login":
            raise HuaweiLoginError(
                f"{request_name} failed | failCode={fail_code} | message={message} | payload={payload}"
            )

        raise HuaweiApiError(
            f"{request_name} failed | failCode={fail_code} | message={message} | payload={payload} | body={body}"
        )

    def _safe_json(self, resp: requests.Response, request_name: str) -> dict:
        try:
            data = resp.json()
        except json.JSONDecodeError as e:
            raise HuaweiApiError(
                f"{request_name} returned non-JSON response | HTTP {resp.status_code} | text={resp.text[:1000]}"
            ) from e

        if not isinstance(data, dict):
            raise HuaweiApiError(
                f"{request_name} returned unexpected JSON type: {type(data)}"
            )
        return data