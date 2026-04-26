from __future__ import annotations

import json
import urllib.request

from .config import settings


class Auth0VaultError(RuntimeError):
    pass


def get_tenant_secret(name: str) -> str:
    """Fetch a tenant-level secret from an Auth0-protected endpoint.

    The vault endpoint must accept `Authorization: Bearer <access_token>` and return
    JSON with the secret, e.g. `{ "SNOWFLAKE_TOKEN": "..." }`.
    """
    if not settings.AUTH0_SNOWFLAKE_TOKEN_URL:
        raise Auth0VaultError("AUTH0_SNOWFLAKE_TOKEN_URL is not configured")
    if not settings.AUTH0_DOMAIN or not settings.AUTH0_CLIENT_ID or not settings.AUTH0_CLIENT_SECRET:
        raise Auth0VaultError("Auth0 client credentials are not configured (AUTH0_DOMAIN/CLIENT_ID/CLIENT_SECRET)")

    token_url = f"https://{settings.AUTH0_DOMAIN}/oauth/token"
    payload = {
        "client_id": settings.AUTH0_CLIENT_ID,
        "client_secret": settings.AUTH0_CLIENT_SECRET,
        "audience": settings.AUTH0_AUDIENCE or settings.AUTH0_SNOWFLAKE_TOKEN_URL,
        "grant_type": "client_credentials",
    }
    req = urllib.request.Request(
        token_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:  # pragma: no cover - network/env specific
        raise Auth0VaultError(f"Failed to fetch Auth0 access token: {e}") from e

    access_token = data.get("access_token")
    if not access_token:
        raise Auth0VaultError("Auth0 response did not include access_token")

    req2 = urllib.request.Request(
        settings.AUTH0_SNOWFLAKE_TOKEN_URL,
        headers={"authorization": f"Bearer {access_token}"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req2, timeout=10) as resp2:
            body = json.loads(resp2.read().decode("utf-8"))
    except Exception as e:  # pragma: no cover - network/env specific
        raise Auth0VaultError(f"Failed to fetch tenant secret from vault endpoint: {e}") from e

    value = body.get(name) if isinstance(body, dict) else None
    if not value or not isinstance(value, str):
        raise Auth0VaultError(f"Vault endpoint did not return secret '{name}'")
    return value
