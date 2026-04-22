from __future__ import annotations

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
    """
    Huawei session/token manager for /thirdData/login based auth.

    Behavior:
    - Reuse token until close to expiry
    - Allow forced refresh
    - Keep one requests.Session() so cookies are retained
    - Invalidate token when auth issues are detected
    """

    def __init__(self, base_url: str, username: str, system_code: str, timeout: int = 60):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.system_code = system_code
        self.timeout = timeout
        self.session = requests.Session()
        self._token_state: Optional[TokenState] = None

    def get_session(self) -> requests.Session:
        """
        Ensure session has a valid token header, then return requests.Session.
        """
        token = self.get_token(force_refresh=False)
        self.session.headers.update({
            "XSRF-TOKEN": token,
            "Content-Type": "application/json",
            "Accept": "application/json, */*",
        })
        return self.session

    def get_token(self, force_refresh: bool = False) -> str:
        """
        Main compatibility method expected by HuaweiLegacyClient.
        """
        if force_refresh or self._token_state is None or self._is_expired():
            self.login()
        return self._token_state.token

    def invalidate(self) -> None:
        self._token_state = None

    def login(self) -> TokenState:
        url = f"{self.base_url}/thirdData/login"
        payload = {
            "userName": self.username,
            "systemCode": self.system_code,
        }

        try:
            response = self.session.post(url, json=payload, timeout=self.timeout)
            response.raise_for_status()
        except requests.HTTPError as e:
            raise HuaweiLoginError(f"Login HTTP error: {e}") from e
        except requests.RequestException as e:
            raise HuaweiLoginError(f"Login request failed: {e}") from e

        try:
            body = response.json()
        except Exception as e:
            raise HuaweiLoginError(f"Login returned non-JSON response: {response.text[:1000]}") from e

        if not body.get("success") or body.get("failCode") != 0:
            raise HuaweiLoginError(
                f"Login failed: failCode={body.get('failCode')}, message={body.get('message')}"
            )

        token = response.headers.get("XSRF-TOKEN")
        if not token:
            raise HuaweiLoginError("Login succeeded but XSRF-TOKEN not found in response header.")

        now = datetime.now(timezone.utc)
        self._token_state = TokenState(
            token=token,
            acquired_at_utc=now,
            expires_at_utc=now + timedelta(minutes=30),
        )

        self.session.headers.update({
            "XSRF-TOKEN": token,
            "Content-Type": "application/json",
            "Accept": "application/json, */*",
        })

        return self._token_state

    def _is_expired(self) -> bool:
        if self._token_state is None:
            return True
        # safety margin 2 minutes
        return datetime.now(timezone.utc) >= (self._token_state.expires_at_utc - timedelta(minutes=2))