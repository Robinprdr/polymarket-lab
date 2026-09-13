"""Closed HTTP surface: fixed public GET routes, no auth, no redirects."""
from __future__ import annotations
import asyncio
from decimal import Decimal
import json
import logging
import re
import httpx

LOG = logging.getLogger(__name__)
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
WS_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class PublicAPI:
    def __init__(self, *, transport=None):
        self._client = httpx.AsyncClient(timeout=20, follow_redirects=False,
                                         trust_env=False, transport=transport)
        self.requests = 0
        self.retries = 0
        self._semaphore = asyncio.Semaphore(4)

    async def close(self):
        await self._client.aclose()

    async def _get(self, base, path, params):
        if (base, path) not in {(GAMMA, "/markets/keyset"), (CLOB, "/book")}:
            raise ValueError("Not an allowed public data route")
        allowed = {"closed", "limit", "after_cursor"} if base == GAMMA else {"token_id"}
        if set(params) - allowed:
            raise ValueError("Unexpected public query parameter")
        async with self._semaphore:
            for attempt in range(3):
                try:
                    self.requests += 1
                    response = await self._client.get(base + path, params=params)
                    if response.status_code == 429 or response.status_code >= 500:
                        if attempt < 2:
                            self.retries += 1
                            LOG.warning("Public GET retry status=%s path=%s", response.status_code, path)
                            await asyncio.sleep(2 ** attempt)
                            continue
                    response.raise_for_status()
                    return json.loads(response.content, parse_float=Decimal)
                except httpx.TransportError:
                    if attempt == 2:
                        raise
                    self.retries += 1
                    LOG.warning("Public GET transport failure path=%s attempt=%s", path, attempt + 1)
                    await asyncio.sleep(2 ** attempt)
        raise RuntimeError("GET exhausted")

    async def market_page(self, cursor=None, limit=100):
        if not 1 <= limit <= 100:
            raise ValueError("Page size must be between 1 and 100")
        params = {"closed": "false", "limit": limit}
        if cursor:
            params["after_cursor"] = cursor
        result = await self._get(GAMMA, "/markets/keyset", params)
        if not isinstance(result, dict) or not isinstance(result.get("markets"), list):
            raise ValueError("Unexpected Gamma keyset schema")
        cursor = result.get("next_cursor")
        if cursor is not None and not isinstance(cursor, str):
            raise ValueError("Unexpected Gamma cursor")
        return result["markets"], cursor

    async def book(self, token_id):
        if not isinstance(token_id, str) or not re.fullmatch(r"[0-9]+", token_id):
            raise ValueError("Invalid token ID")
        result = await self._get(CLOB, "/book", {"token_id": token_id})
        if not isinstance(result, dict):
            raise ValueError("Unexpected CLOB book schema")
        return result
