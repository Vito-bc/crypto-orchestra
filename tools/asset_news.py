"""
Asset-specific news tool.

Fetches news headlines relevant to a specific crypto asset (not just general market news).

Sources (all free, no required API keys):
  1. Google News RSS — real-time news search, no auth needed
  2. Reddit RSS      — community signals from asset-specific subreddits
  3. CryptoPanic     — optional; activated by CRYPTOPANIC_API_KEY in .env

IMPORTANT: All articles are filtered by publication date.
  - Only articles from the last 7 days are included in analysis
  - critical_alert is ONLY set if articles with critical keywords are < 3 days old
  This prevents old/historical news (e.g. 2019 Zcash counterfeiting bug) from
  triggering false vetoes.

Critical event detection:
  NEGATIVE flags: delisting, hack, exploit, ban, SEC action, exchange suspension
  POSITIVE catalysts: ETF, partnership, upgrade, adoption, institutional
"""

from __future__ import annotations

import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import quote
from urllib.request import urlopen, Request

_HEADERS = {"User-Agent": "Mozilla/5.0 CryptoOrchestra/1.0"}

_NEWS_MAX_AGE_DAYS      = 7   # articles older than this are ignored entirely
_CRITICAL_MAX_AGE_DAYS  = 3   # critical_alert only fires if article < 3 days old

_REDDIT_SUB = {
    "BTC-USD": "Bitcoin",
    "ETH-USD": "ethereum",
    "SOL-USD": "solana",
    "ZEC-USD": "zcash",
}

_COIN_NAME = {
    "BTC-USD": "Bitcoin BTC",
    "ETH-USD": "Ethereum ETH",
    "SOL-USD": "Solana SOL",
    "ZEC-USD": "Zcash ZEC",
}

_NEGATIVE_KEYWORDS = [
    "delist", "delisting", "banned", "ban", "hack", "hacked", "exploit",
    "sec", "lawsuit", "regulation", "suspended", "suspend",
    "probe", "fraud", "scam", "shutdown", "collapse", "insolvent",
    "sanctions", "privacy coin",
]

_POSITIVE_KEYWORDS = [
    "etf", "approval", "approved", "partnership", "upgrade", "adoption",
    "institutional", "halving", "bullish", "rally", "all-time high",
    "mainnet", "launch", "integration", "listing", "added",
]

# Only these trigger critical_alert — AND they must be in articles < 3 days old
_CRITICAL_NEGATIVE = [
    "delist", "delisting", "hacked", "hack confirmed", "exploit confirmed",
    "ban", "banned", "sec enforcement", "suspended trading", "shutdown",
]


def _fetch(url: str, timeout: int = 10) -> bytes | None:
    try:
        req = Request(url, headers=_HEADERS)
        with urlopen(req, timeout=timeout) as r:
            return r.read()
    except Exception:
        return None


def _parse_date(date_str: str | None) -> datetime | None:
    """Parse RSS pubDate string to UTC datetime. Returns None on failure."""
    if not date_str:
        return None
    try:
        dt = parsedate_to_datetime(date_str.strip())
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _parse_rss_with_dates(data: bytes, max_age_days: int) -> list[tuple[str, datetime | None]]:
    """
    Parse RSS feed and return (title, pub_date) pairs.
    Filters out items older than max_age_days.
    Items with unparseable dates are included (can't filter what we can't read).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    try:
        root = ET.fromstring(data)
        results = []
        for item in root.iter("item"):
            t = item.find("title")
            if t is None or not t.text:
                continue
            title = t.text.strip()

            # Try pubDate first, then dc:date
            pub_raw = None
            pd = item.find("pubDate")
            if pd is not None:
                pub_raw = pd.text
            if not pub_raw:
                # Try dc:date (Dublin Core)
                for child in item:
                    if child.tag.endswith("}date") or child.tag == "date":
                        pub_raw = child.text
                        break

            pub_dt = _parse_date(pub_raw)

            # Filter by age: skip if older than cutoff AND we could parse the date
            if pub_dt is not None and pub_dt < cutoff:
                continue

            results.append((title, pub_dt))
        return results
    except Exception:
        return []


def get_asset_headlines(asset: str, limit: int = 10) -> dict:
    """
    Fetch asset-specific headlines from Google News and Reddit.
    Only includes articles published in the last 7 days.

    Returns:
        {
            "headlines":         list[str],
            "headline_ages_days": list[float | None],  # age of each headline in days
            "negative_flags":    list[str],
            "positive_signals":  list[str],
            "critical_alert":    bool,   # ONLY for confirmed recent events (< 3 days)
            "oldest_critical_age_days": float | None,
            "sources":           list[str],
            "error":             str | None,
        }
    """
    result: dict = {
        "headlines":              [],
        "headline_ages_days":     [],
        "negative_flags":         [],
        "positive_signals":       [],
        "critical_alert":         False,
        "oldest_critical_age_days": None,
        "sources":                [],
        "error":                  None,
    }

    now = datetime.now(timezone.utc)
    coin_query = _COIN_NAME.get(asset.upper(), asset.replace("-USD", ""))
    items: list[tuple[str, datetime | None]] = []

    # 1. Google News RSS (with date filter)
    gn_url = (
        f"https://news.google.com/rss/search?q={quote(coin_query + ' crypto')}"
        "&hl=en-US&gl=US&ceid=US:en"
    )
    gn_data = _fetch(gn_url)
    if gn_data:
        gn_items = _parse_rss_with_dates(gn_data, _NEWS_MAX_AGE_DAYS)
        if gn_items:
            items.extend(gn_items[:limit])
            result["sources"].append("Google News")

    # 2. Reddit RSS for asset-specific subreddit (posts are always recent)
    sub = _REDDIT_SUB.get(asset.upper())
    if sub:
        reddit_url = f"https://www.reddit.com/r/{sub}/new.rss?limit=10"
        rd_data = _fetch(reddit_url, timeout=12)
        if rd_data:
            rd_items = _parse_rss_with_dates(rd_data, _NEWS_MAX_AGE_DAYS)
            if rd_items:
                items.extend(rd_items[:5])
                result["sources"].append(f"r/{sub}")

    # 3. CryptoPanic (if API key is set in .env) — has timestamps in JSON
    cp_key = os.getenv("CRYPTOPANIC_API_KEY")
    if cp_key:
        base_sym = asset.replace("-USD", "").replace("/USDT", "")
        cp_url = (
            f"https://cryptopanic.com/api/v1/posts/"
            f"?auth_token={cp_key}&currencies={base_sym}&filter=hot&public=true"
        )
        cp_data = _fetch(cp_url)
        if cp_data:
            try:
                cp_json = json.loads(cp_data)
                cutoff = now - timedelta(days=_NEWS_MAX_AGE_DAYS)
                for post in cp_json.get("results", [])[:5]:
                    title = post.get("title", "")
                    if not title:
                        continue
                    pub_dt = None
                    pub_str = post.get("published_at") or post.get("created_at")
                    if pub_str:
                        try:
                            pub_dt = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                        except Exception:
                            pass
                    if pub_dt and pub_dt < cutoff:
                        continue
                    items.append((title, pub_dt))
                result["sources"].append("CryptoPanic")
            except Exception:
                pass

    # Build output
    headlines = []
    ages_days = []
    for title, pub_dt in items[:limit]:
        headlines.append(title)
        if pub_dt:
            ages_days.append(round((now - pub_dt).total_seconds() / 86400, 1))
        else:
            ages_days.append(None)

    result["headlines"]          = headlines
    result["headline_ages_days"] = ages_days

    # Keyword analysis on recent headlines only
    all_text = " ".join(headlines).lower()
    result["negative_flags"]  = [kw for kw in _NEGATIVE_KEYWORDS if kw in all_text]
    result["positive_signals"] = [kw for kw in _POSITIVE_KEYWORDS if kw in all_text]

    # critical_alert ONLY fires if a critical-keyword article is < 3 days old
    critical_age_cutoff = now - timedelta(days=_CRITICAL_MAX_AGE_DAYS)
    for title, pub_dt in items[:limit]:
        title_lower = title.lower()
        if any(kw in title_lower for kw in _CRITICAL_NEGATIVE):
            # If we can't verify the date, assume recent (safer to flag)
            is_recent = pub_dt is None or pub_dt >= critical_age_cutoff
            if is_recent:
                result["critical_alert"] = True
                if pub_dt:
                    age = round((now - pub_dt).total_seconds() / 86400, 1)
                    result["oldest_critical_age_days"] = age
                break

    if not headlines:
        result["error"] = "No recent headlines (last 7 days) from any source"

    return result
