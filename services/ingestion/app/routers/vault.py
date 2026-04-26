from __future__ import annotations

import hmac
import os
import sqlite3
from typing import Literal

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel
import jwt
import urllib.request


router = APIRouter()

DB_PATH = os.environ.get("VAULT_DB_PATH", "/tmp/via_vault.sqlite")
ADMIN_TOKEN = os.environ.get("VAULT_ADMIN_TOKEN", "")
AUTH0_DOMAIN = os.environ.get("AUTH0_DOMAIN", "")
AUTH0_AUDIENCE = os.environ.get("AUTH0_AUDIENCE", "")
AUTH0_ISSUER = os.environ.get("AUTH0_ISSUER", "")  # optional override


def _require_admin(token: str | None) -> None:
    if not ADMIN_TOKEN:
        raise HTTPException(status_code=500, detail="Vault admin token not configured")
    if not token or not hmac.compare_digest(token, ADMIN_TOKEN):
        raise HTTPException(status_code=401, detail="Unauthorized")


def _require_auth0(request: Request) -> dict:
    if not AUTH0_DOMAIN:
        raise HTTPException(status_code=500, detail="AUTH0_DOMAIN not configured")
    auth = request.headers.get("authorization") or ""
    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = auth.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing bearer token")

    issuer = AUTH0_ISSUER or f"https://{AUTH0_DOMAIN}/"
    jwks_url = f"https://{AUTH0_DOMAIN}/.well-known/jwks.json"
    try:
        with urllib.request.urlopen(jwks_url, timeout=10) as resp:
            jwks = jwt.PyJWKSet.from_json(resp.read().decode("utf-8"))
    except Exception as e:  # pragma: no cover
        raise HTTPException(status_code=500, detail=f"Failed to load Auth0 JWKS: {e}")

    try:
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
        if not kid:
            raise HTTPException(status_code=401, detail="Invalid token header")
        jwk = next((k for k in jwks.keys if k.key_id == kid), None)
        if jwk is None:
            raise HTTPException(status_code=401, detail="Unknown signing key")
        decoded = jwt.decode(
            token,
            jwk.key,
            algorithms=["RS256"],
            audience=AUTH0_AUDIENCE or None,
            issuer=issuer,
            options={"verify_aud": bool(AUTH0_AUDIENCE)},
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")
    return decoded if isinstance(decoded, dict) else {}



def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS secrets (
          name TEXT PRIMARY KEY,
          value TEXT NOT NULL
        )
        """
    )
    return conn


class PutSecretRequest(BaseModel):
    name: str
    value: str


@router.put("")
def put_secret(
    req: PutSecretRequest,
    request: Request,
    x_admin_token: str | None = Header(default=None),
) -> dict:
    if x_admin_token:
        _require_admin(x_admin_token)
    else:
        _require_auth0(request)
    with _db() as conn:
        conn.execute("INSERT OR REPLACE INTO secrets(name,value) VALUES(?,?)", (req.name, req.value))
        conn.commit()
    return {"ok": True, "name": req.name}


@router.get("")
def get_secret(
    name: str,
    request: Request,
    x_admin_token: str | None = Header(default=None),
    format: Literal["json", "raw"] = "json",
) -> dict | str:
    if x_admin_token:
        _require_admin(x_admin_token)
    else:
        _require_auth0(request)
    with _db() as conn:
        row = conn.execute("SELECT value FROM secrets WHERE name = ?", (name,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Not found")
    value = str(row[0])
    if format == "raw":
        return value
    return {name: value}
