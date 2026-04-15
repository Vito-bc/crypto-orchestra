"""
Sentiment data tool.

Fetches two free, no-auth signals:
  1. Fear & Greed Index  — alternative.me/crypto/fear-and-greed-index/
  2. CryptoPanic headlines — cryptopanic.com (public feed, no key needed)

Both are used by the SentimentAgent.
"""

from __future__ import annotations

import json
from urllib.request import urlopen, Request
from urllib.error import URLError

_FEAR_GREED_URL  = "https://api.alternative.me/fng/?limit=1&format=json"
_CRYPTOPANIC_URL = "https://cryptopanic.com/api/free/v1/posts/?auth_token=&public=true&currencies={currencies}&kind=news"
_HEADERS = {"User-Agent": "CryptoOrchestra/1.0"}


def get_fear_and_greed() -> dict:
    """
    Returns:
        {
            "value": 45,           # 0=Extreme Fear, 100=Extreme Greed
            "label": "Fear",
            "raw": { ... }
        }
    """
    try:
        req = Request(_FEAR_GREED_URL, headers=_HEADERS)
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        entry = data["data"][0]
        return {
            "value": int(entry["value"]),
            "label": entry["value_classification"],
            "raw":   entry,
        }
    except (URLError, KeyError, json.JSONDecodeError) as exc:
        return {"value": 50, "label": "Unknown", "error": str(exc)}


def get_recent_headlines(asset: str, limit: int = 10) -> list[str]:
    """
    Returns a list of recent news headline strings for the asset.
    asset: e.g. "BTC/USDT" or "ETH-USD" — we extract the base currency.
    """
    # Extract base currency (BTC, ETH …)
    base = asset.upper().replace("-USD", "").replace("/USDT", "").replace("/USD", "")
    url  = _CRYPTOPANIC_URL.format(currencies=base)

    try:
        req = Request(url, headers=_HEADERS)
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        results = data.get("results", [])[:limit]
        return [r.get("title", "") for r in results if r.get("title")]
    except (URLError, KeyError, json.JSONDecodeError) as exc:
        return [f"[headlines unavailable: {exc}]"]
