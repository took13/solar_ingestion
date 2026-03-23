from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from src.api.exceptions import HuaweiRateLimitError, HuaweiUnauthorizedError
from src.api.session_manager import SessionManager


@dataclass
class ApiCallResult:
    http_status: int
    success: bool
    fail_code: Optional[int]
    message: Optional[str]
    body: Dict[str, Any]


class HuaweiLegacyClient:
    def __init__(self, session_manager: SessionManager, base_url: str, timeout: int = 120):
        self.session_manager = session_manager
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def get_dev_history_kpi(
        self,
        dev_type_id: int,
        dev_ids: List[int],
        start_time_ms: int,
        end_time_ms: int,
    ) -> ApiCallResult:
        url = f"{self.base_url}/thirdData/getDevHistoryKpi"
        payload = {
            "devTypeId": dev_type_id,
            "devIds": ",".join(str(x) for x in dev_ids),
            "startTime": start_time_ms,
            "endTime": end_time_ms
        }

        session = self.session_manager.get_session()
        resp = session.post(url, json=payload, timeout=self.timeout)

        try:
            body = resp.json()
        except Exception:
            body = {"success": False, "failCode": None, "message": resp.text, "data": []}

        fail_code = body.get("failCode")
        message = body.get("message")

        if resp.status_code == 401:
            self.session_manager.invalidate()
            raise HuaweiUnauthorizedError("401 Unauthorized")

        if fail_code == 407 or resp.status_code == 429:
            raise HuaweiRateLimitError(f"Rate limit triggered: http={resp.status_code}, failCode={fail_code}")

        return ApiCallResult(
            http_status=resp.status_code,
            success=bool(body.get("success", False)),
            fail_code=fail_code,
            message=message,
            body=body
        )