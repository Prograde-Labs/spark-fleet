"""Tests for agent tokens, UI sessions, passwords, and CSRF protection."""

import base64
import hashlib
import hmac
import json

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from spark_fleet.shared.security import (
    CSRF_HEADER_NAME,
    SESSION_COOKIE_NAME,
    CsrfError,
    Session,
    SessionError,
    agent_auth_dependency,
    create_session,
    mutation_dependency,
    parse_bearer,
    ui_session_dependency,
    verify_csrf,
    verify_password,
    verify_session,
    verify_token,
)

SECRET = "s" * 40
TOKEN = "agent-token-0123456789"


def sign(payload: dict, secret: str = SECRET) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    encoded = base64.urlsafe_b64encode(raw).rstrip(b"=").decode()
    signature = hmac.new(secret.encode(), encoded.encode(), hashlib.sha256).digest()
    return f"{encoded}.{base64.urlsafe_b64encode(signature).rstrip(b'=').decode()}"


def test_password_match_and_mismatch():
    assert verify_password("correct horse", "correct horse") is True
    assert verify_password("correct horse", "wrong") is False
    assert verify_password("correct horse", "") is False
    assert verify_password("correct horse", None) is False


def test_password_compare_is_constant_time():
    import spark_fleet.shared.security as security

    calls = []

    def fake_compare(left, right):
        calls.append((left, right))
        return left == right

    original = security.hmac.compare_digest
    security.hmac.compare_digest = fake_compare
    try:
        assert verify_password("a", "a") is True
    finally:
        security.hmac.compare_digest = original

    assert calls == [(b"a", b"a")]


@pytest.mark.parametrize(
    "header",
    [
        "Bearer abc",
        "bearer abc",
        "BEARER abc",
        "Bearer   abc",
    ],
)
def test_parse_bearer_accepts_case_insensitive_scheme(header):
    assert parse_bearer(header) == "abc"


@pytest.mark.parametrize(
    "header",
    [
        None,
        "",
        "abc",
        "Basic abc",
        "Bearer",
        "Bearer ",
        "Bearer a b",
        " Bearer abc",
        "Bearer\tabc",
    ],
)
def test_parse_bearer_rejects_malformed_headers(header):
    assert parse_bearer(header) is None


def test_verify_token_uses_constant_time_compare():
    assert verify_token(TOKEN, TOKEN) is True
    assert verify_token(TOKEN, TOKEN + "x") is False
    assert verify_token(TOKEN, None) is False
    assert verify_token(TOKEN, "") is False


def test_session_round_trip():
    now = 1_000_000.0
    token = create_session(SECRET, ttl_seconds=3600, now=now)

    session = verify_session(SECRET, token, now=now + 1)

    assert isinstance(session, Session)
    assert session.issued_at == now
    assert session.expires_at == now + 3600
    assert session.csrf
    assert len(session.csrf) >= 32


def test_session_csrf_is_unique_per_login():
    first = verify_session(SECRET, create_session(SECRET))
    second = verify_session(SECRET, create_session(SECRET))

    assert first.csrf != second.csrf


def test_session_expires():
    token = create_session(SECRET, ttl_seconds=60, now=1000.0)

    with pytest.raises(SessionError, match="expired"):
        verify_session(SECRET, token, now=1060.0)


def test_session_rejects_tampered_payload():
    token = create_session(SECRET, now=1000.0)
    payload_b64, signature = token.split(".")
    payload = json.loads(base64.urlsafe_b64decode(payload_b64 + "=" * (-len(payload_b64) % 4)))
    payload["expires_at"] = payload["expires_at"] + 10_000
    tampered = (
        base64.urlsafe_b64encode(
            json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
        )
        .rstrip(b"=")
        .decode()
    )

    with pytest.raises(SessionError, match="signature"):
        verify_session(SECRET, f"{tampered}.{signature}", now=1000.0)


def test_session_rejects_tampered_signature():
    token = create_session(SECRET, now=1000.0)
    payload_b64, signature = token.split(".")
    flipped = signature[:-1] + ("A" if signature[-1] != "A" else "B")

    with pytest.raises(SessionError, match="signature"):
        verify_session(SECRET, f"{payload_b64}.{flipped}", now=1000.0)


def test_session_rejects_wrong_secret():
    token = create_session(SECRET, now=1000.0)

    with pytest.raises(SessionError, match="signature"):
        verify_session("x" * 40, token, now=1000.0)


@pytest.mark.parametrize(
    "token",
    [
        "",
        ".",
        "abc",
        "abc.",
        ".abc",
        "abc.def.ghi",
        "!!!.???",
        "eyJ4IjoxfQ.!!!",
    ],
)
def test_session_rejects_malformed_tokens(token):
    with pytest.raises(SessionError):
        verify_session(SECRET, token)


def test_session_rejects_invalid_payload_shape():
    for payload in (
        [],
        {"issued_at": 0, "expires_at": 100, "csrf": 3},
        {"issued_at": "0", "expires_at": 100, "csrf": "abc"},
        {"issued_at": 0, "expires_at": 0, "csrf": "abc"},
        {"issued_at": 50, "expires_at": 10, "csrf": "abc"},
        {"issued_at": 0, "expires_at": 100},
    ):
        with pytest.raises(SessionError):
            verify_session(SECRET, sign(payload), now=0.0)


def test_session_requires_non_empty_secret():
    with pytest.raises(ValueError, match="secret"):
        create_session("")


def test_session_rejects_non_positive_ttl():
    with pytest.raises(ValueError, match="ttl"):
        create_session(SECRET, ttl_seconds=0)


def test_verify_csrf_accepts_matching_token():
    session = verify_session(SECRET, create_session(SECRET))

    verify_csrf(session, session.csrf)


@pytest.mark.parametrize("provided", [None, "", "wrong"])
def test_verify_csrf_rejects_mismatch(provided):
    session = verify_session(SECRET, create_session(SECRET))

    with pytest.raises(CsrfError):
        verify_csrf(session, provided)


def test_agent_dependency_accepts_valid_token():
    app = FastAPI()
    app.get("/protected")(_agent_endpoint(agent_auth_dependency(TOKEN)))
    client = TestClient(app)

    response = client.get("/protected", headers={"Authorization": f"Bearer {TOKEN}"})

    assert response.status_code == 200
    assert response.json() == {"ok": True}


@pytest.mark.parametrize(
    "headers",
    [{}, {"Authorization": "Bearer wrong-token"}, {"Authorization": "Basic abc"}],
)
def test_agent_dependency_rejects_bad_tokens(headers):
    app = FastAPI()
    app.get("/protected")(_agent_endpoint(agent_auth_dependency(TOKEN)))
    client = TestClient(app)

    response = client.get("/protected", headers=headers)

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


def test_session_dependency_disabled_returns_none():
    app = FastAPI()
    app.get("/whoami")(_whoami_endpoint(ui_session_dependency(None)))
    client = TestClient(app)

    response = client.get("/whoami")

    assert response.status_code == 200
    assert response.json() == {"enabled": False}


def test_session_dependency_requires_cookie_when_enabled():
    app = FastAPI()
    app.get("/whoami")(_whoami_endpoint(ui_session_dependency(SECRET)))
    client = TestClient(app)

    assert client.get("/whoami").status_code == 401


def test_session_dependency_accepts_valid_cookie():
    app = FastAPI()
    app.get("/whoami")(_whoami_endpoint(ui_session_dependency(SECRET)))
    client = TestClient(app)

    token = create_session(SECRET)
    response = client.get("/whoami", cookies={SESSION_COOKIE_NAME: token})

    assert response.status_code == 200
    assert response.json()["enabled"] is True


def test_session_dependency_rejects_invalid_cookie():
    app = FastAPI()
    app.get("/whoami")(_whoami_endpoint(ui_session_dependency(SECRET)))
    client = TestClient(app)

    response = client.get("/whoami", cookies={SESSION_COOKIE_NAME: "bogus"})

    assert response.status_code == 401


def test_mutation_dependency_disabled_allows_request():
    app = FastAPI()
    app.post("/mutate")(_mutation_endpoint(mutation_dependency(None)))
    client = TestClient(app)

    assert client.post("/mutate").status_code == 200


def test_mutation_dependency_requires_session():
    app = FastAPI()
    app.post("/mutate")(_mutation_endpoint(mutation_dependency(SECRET)))
    client = TestClient(app)

    assert client.post("/mutate").status_code == 401


def test_mutation_dependency_requires_matching_csrf():
    app = FastAPI()
    app.post("/mutate")(_mutation_endpoint(mutation_dependency(SECRET)))
    client = TestClient(app)
    token = create_session(SECRET)
    session = verify_session(SECRET, token)

    missing = client.post("/mutate", cookies={SESSION_COOKIE_NAME: token})
    wrong = client.post(
        "/mutate",
        cookies={SESSION_COOKIE_NAME: token},
        headers={CSRF_HEADER_NAME: "wrong"},
    )
    valid = client.post(
        "/mutate",
        cookies={SESSION_COOKIE_NAME: token},
        headers={CSRF_HEADER_NAME: session.csrf},
    )

    assert missing.status_code == 403
    assert wrong.status_code == 403
    assert valid.status_code == 200


def _agent_endpoint(dependency):
    async def endpoint(_: None = Depends(dependency)):
        return {"ok": True}

    return endpoint


def _whoami_endpoint(dependency):
    async def endpoint(session: Session | None = Depends(dependency)):
        return {"enabled": session is not None}

    return endpoint


def _mutation_endpoint(dependency):
    async def endpoint(session: Session | None = Depends(dependency)):
        return {"session": session is not None}

    return endpoint
