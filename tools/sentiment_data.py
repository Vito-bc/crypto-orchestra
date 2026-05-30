"""
Sentiment data tool.

Fetches two free, no-auth signals:
  1. Fear & Greed Index  — alternative.me/crypto/fear-and-greed-index/
  2. Crypto news headlines — CoinDesk RSS (primary), Cointelegraph RSS (fallback)

Both are used by the SentimentAgent.
"""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from urllib.request import urlopen, Request

_FEAR_GREED_URL    = "https://api.alternative.me/fng/?limit=1&format=json"
_COINDESK_RSS      = "https://www.coindesk.com/arc/outboundfeeds/rss/"
_COINTELEGRAPH_RSS = "https://cointelegraph.com/rss"
_HEADERS = {"User-Agent": "Mozilla/5.0 CryptoOrchestra/1.0"}


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
    except Exception as exc:
        return {"value": 50, "label": "Unknown", "error": str(exc)}


def _parse_rss_titles(url: str, limit: int) -> list[str]:
    """Fetch an RSS feed and return up to `limit` <title> strings from <item> elements."""
    req = Request(url, headers=_HEADERS)
    with urlopen(req, timeout=12) as resp:
        root = ET.fromstring(resp.read())
    titles = []
    for item in root.iter("item"):
        t = item.find("title")
        if t is not None and t.text:
            titles.append(t.text.strip())
        if len(titles) >= limit:
            break
    return titles


def get_recent_headlines(asset: str, limit: int = 10) -> list[str]:
    """
    Returns a list of recent crypto news headlines.
    Uses CoinDesk RSS (primary) with Cointelegraph as fallback.
    Headlines are general crypto market news, not asset-specific.
    """
    for url in [_COINDESK_RSS, _COINTELEGRAPH_RSS]:
        try:
            return _parse_rss_titles(url, limit)
        except Exception:
            continue
    return ["[headlines unavailable: all RSS feeds failed]"]
