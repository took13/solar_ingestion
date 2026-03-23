from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
import requests

from src.api.exceptions import HuaweiLoginError


@dataclass
class TokenState:
    token: str
    acquired_at_utc: datetime
    expires_at_utc: datetime


class SessionManager:
    def __init__(self, base_url: str, username: str, system_code: str, timeout: int = 60):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.system_code = system_code
        self.timeout = timeout
        self.session = requests.Session()
        self._token_state: Optional[TokenState] = None

    def get_session(self) -> requests.Session:
        if self._token_state is None or self._is_expired():
            self.login()
        self.session.headers.update({
            "XSRF-TOKEN": self._token_state.token
        })
        return self.session

    def invalidate(self) -> None:
        self._token_state = None

    def login(self) -> TokenState:
        url = f"{self.base_url}/thirdData/login"
        payload = {
            "userName": self.username,
            "systemCode": self.system_code
        }

        response = self.session.post(url, json=payload, timeout=self.timeout)
        response.raise_for_status()
        body = response.json()

        if not body.get("success") or body.get("failCode") != 0:
            raise HuaweiLoginError(f"Login failed: failCode={body.get('failCode')}, message={body.get('message')}")

        token = response.headers.get("XSRF-TOKEN")
        if not token:
            raise HuaweiLoginError("Login succeeded but XSRF-TOKEN not found in response header.")

        now = datetime.now(timezone.utc)
        self._token_state = TokenState(
            token=token,
            acquired_at_utc=now,
            expires_at_utc=now + timedelta(minutes=30)
        )
        return self._token_state

    def _is_expired(self) -> bool:
        if self._token_state is None:
            return True
        # เผื่อ safety margin 2 นาที
        return datetime.now(timezone.utc) >= (self._token_state.expires_at_utc - timedelta(minutes=2))