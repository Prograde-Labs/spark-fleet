"""Authentication and session primitives for the controller and agents.

Agents accept only requests carrying a shared bearer token; the controller
compares tokens and passwords in constant time. When UI login is enabled the
controller issues an HMAC-signed session cookie containing an expiry and a
per-session CSRF token, and mutations must echo that token in a header. Login is
off by default on the trusted LAN, so the session/CSRF dependencies pass through
when no session secret is configured.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass
from typing import Annotated

from fastapi import Cookie, Header, HTTPException

SESSION_COOKIE_NAME = "spark_fleet_session"
CSRF_HEADER_NAME = "X-CSRF-Token"
DEFAULT_SESSION_TTL_SECONDS = 12 * 3600
_MAX_CLOCK_SKEW_SECONDS = 300.0


class SecurityError(Exception):
    """Base class for authentication and session failures."""


class AuthError(SecurityError):
    """Raised when UI login is disabled or credentials are invalid."""


class SessionError(SecurityError):
    """Raised when a session token is malformed, forged, or expired."""


class CsrfError(SecurityError):
    """Raised when a mutation does not carry the session's CSRF token."""


@dataclass(frozen=True)
class Session:
    issued_at: float
    expires_at: float
    csrf: str


def _compare(left: str, right: str | None) -> bool:
    if not left or right is None or not right:
        return False
    try:
        return hmac.compare_digest(left.encode(), right.encode())
    except UnicodeEncodeError:
        return False


def verify_password(expected: str, provided: str | None) -> bool:
    """Compare a submitted password against the configured one."""

    return _compare(expected, provided)


def parse_bearer(header: str | None) -> str | None:
    """Extract the token from an ``Authorization: Bearer ...`` header."""

    if header is None:
        return None
    if header != header.strip():
        return None
    if any(character in header for character in "\t\r\n"):
        return None
    scheme, separator, remainder = header.partition(" ")
    if not separator or scheme.lower() != "bearer":
        return None
    token = remainder.strip()
    if not token or " " in token:
        return None
    return token


def verify_token(expected: str, provided: str | None) -> bool:
    """Compare a presented agent token against the configured token."""

    return _compare(expected, provided)


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64decode(value: str) -> bytes:
    try:
        return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (binascii.Error, ValueError) as exc:
        raise SessionError("invalid session token") from exc


def create_session(
    secret: str,
    *,
    ttl_seconds: float = DEFAULT_SESSION_TTL_SECONDS,
    now: float | None = None,
) -> str:
    """Create a signed session token carrying an expiry and CSRF token."""

    if not secret:
        raise ValueError("session secret must not be empty")
    if ttl_seconds <= 0:
        raise ValueError("session ttl must be positive")
    now = time.time() if now is None else now

    payload = {
        "issued_at": now,
        "expires_at": now + ttl_seconds,
        "csrf": secrets.token_urlsafe(32),
    }
    encoded = _b64encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode())
    signature = hmac.new(secret.encode(), encoded.encode(), hashlib.sha256).digest()
    return f"{encoded}.{_b64encode(signature)}"


def verify_session(secret: str, token: str, *, now: float | None = None) -> Session:
    """Validate a session token and return its payload."""

    if not secret:
        raise ValueError("session secret must not be empty")
    if not isinstance(token, str) or not token:
        raise SessionError("invalid session token")

    encoded, separator, signature = token.partition(".")
    if not separator or not encoded or not signature or "." in signature:
        raise SessionError("invalid session token")

    expected = hmac.new(secret.encode(), encoded.encode(), hashlib.sha256).digest()
    try:
        provided = signature.encode("ascii")
    except UnicodeEncodeError as exc:
        raise SessionError("invalid session token") from exc
    if not hmac.compare_digest(_b64encode(expected).encode(), provided):
        raise SessionError("invalid session signature")

    try:
        payload = json.loads(_b64decode(encoded))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise SessionError("invalid session token") from exc
    if not isinstance(payload, dict):
        raise SessionError("invalid session payload")

    issued_at = payload.get("issued_at")
    expires_at = payload.get("expires_at")
    csrf = payload.get("csrf")
    if isinstance(issued_at, bool) or not isinstance(issued_at, (int, float)):
        raise SessionError("invalid session payload")
    if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)):
        raise SessionError("invalid session payload")
    if not isinstance(csrf, str) or not csrf:
        raise SessionError("invalid session payload")
    if expires_at <= issued_at:
        raise SessionError("invalid session payload")

    now = time.time() if now is None else now
    if expires_at <= now:
        raise SessionError("session expired")
    if issued_at > now + _MAX_CLOCK_SKEW_SECONDS:
        raise SessionError("invalid session issued_at")

    return Session(issued_at=float(issued_at), expires_at=float(expires_at), csrf=csrf)


def verify_csrf(session: Session, provided: str | None) -> None:
    """Require the mutation header to match the session's CSRF token."""

    if provided is None:
        raise CsrfError("missing CSRF token")
    if not hmac.compare_digest(session.csrf.encode(), provided.encode()):
        raise CsrfError("invalid CSRF token")


def login_session(
    ui_password: str | None,
    session_secret: str | None,
    provided: str | None,
    *,
    ttl_seconds: float = DEFAULT_SESSION_TTL_SECONDS,
    now: float | None = None,
) -> str:
    """Verify a submitted UI password and issue a signed session token."""

    if ui_password is None or session_secret is None:
        raise AuthError("UI login is disabled")
    if not verify_password(ui_password, provided):
        raise AuthError("invalid credentials")
    return create_session(session_secret, ttl_seconds=ttl_seconds, now=now)


def session_cookie_settings(
    ttl_seconds: float = DEFAULT_SESSION_TTL_SECONDS,
) -> dict[str, object]:
    """Keyword arguments for ``Response.set_cookie`` carrying the session.

    ``secure`` is off because this release is LAN-only over plain HTTP; enable
    it once TLS terminates in front of the controller.
    """

    if ttl_seconds <= 0:
        raise ValueError("session ttl must be positive")
    return {
        "key": SESSION_COOKIE_NAME,
        "httponly": True,
        "samesite": "strict",
        "path": "/",
        "max_age": int(ttl_seconds),
        "secure": False,
    }


def agent_auth_dependency(expected_token: str):
    """Build a FastAPI dependency that enforces the shared agent bearer token."""

    if not expected_token:
        raise ValueError("agent token must not be empty")

    async def dependency(
        authorization: Annotated[str | None, Header()] = None,
    ) -> None:
        if not verify_token(expected_token, parse_bearer(authorization)):
            raise HTTPException(
                status_code=401,
                detail="missing or invalid agent token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    return dependency


def _require_session(session_secret: str, cookie: str | None) -> Session:
    if not cookie:
        raise HTTPException(status_code=401, detail="login required")
    try:
        return verify_session(session_secret, cookie)
    except SessionError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc


def ui_session_dependency(session_secret: str | None):
    """Build a FastAPI dependency returning the UI session (or ``None`` if off)."""

    if session_secret is not None and not session_secret:
        raise ValueError("session secret must not be empty")

    async def dependency(
        cookie: Annotated[str | None, Cookie(alias=SESSION_COOKIE_NAME)] = None,
    ) -> Session | None:
        if session_secret is None:
            return None
        return _require_session(session_secret, cookie)

    return dependency


def mutation_dependency(session_secret: str | None):
    """Build a FastAPI dependency that enforces login and CSRF on mutations."""

    if session_secret is not None and not session_secret:
        raise ValueError("session secret must not be empty")

    async def dependency(
        cookie: Annotated[str | None, Cookie(alias=SESSION_COOKIE_NAME)] = None,
        x_csrf_token: Annotated[str | None, Header(alias=CSRF_HEADER_NAME)] = None,
    ) -> Session | None:
        if session_secret is None:
            return None
        session = _require_session(session_secret, cookie)
        try:
            verify_csrf(session, x_csrf_token)
        except CsrfError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        return session

    return dependency
