"""Manual review of all agent data sources, plus an import-safe smoke test."""

from tools.funding_data import get_funding_rate
from tools.market_positioning import get_long_short_ratio, get_open_interest
from tools.onchain_data import get_onchain_metrics
from tools.price_data import get_snapshot
from tools.sentiment_data import get_fear_and_greed, get_recent_headlines


ASSETS = ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]


def test_agent_review_dependencies_are_importable() -> None:
    """Pytest collection must stay offline; the live review runs only as a script."""
    assert all(callable(fn) for fn in (
        get_onchain_metrics,
        get_funding_rate,
        get_fear_and_greed,
        get_recent_headlines,
        get_snapshot,
        get_open_interest,
        get_long_short_ratio,
    ))


def main() -> None:
    print("=" * 60)
    print("1. CoinGecko metrics (onchain_data)")
    print("=" * 60)
    for asset in ASSETS:
        metrics = get_onchain_metrics(asset)
        ok = "OK  " if "price_change_24h" in metrics else "FAIL"
        change = metrics.get("price_change_24h", "ERR")
        note = metrics.get("exchange_note", metrics.get("error", "?"))
        print(f"  {asset}: [{ok}] 24h={change}%  {note[:50]}")

    print("\n" + "=" * 60)
    print("2. OKX Funding rates (funding_data)")
    print("=" * 60)
    for asset in ASSETS:
        funding = get_funding_rate(asset)
        ok = "OK  " if not funding.get("error") else "FAIL"
        rate = funding["current_rate_pct"]
        signal = funding["signal"]
        error = funding.get("error", "-")
        print(f"  {asset}: [{ok}] rate={rate:+.5f}%  signal={signal}  err={error}")

    print("\n" + "=" * 60)
    print("3. OKX Open Interest (market_positioning)")
    print("=" * 60)
    for asset in ASSETS:
        interest = get_open_interest(asset)
        ok = "OK  " if interest.get("source") else "FAIL"
        change = interest.get("oi_change_pct", 0)
        trend = interest.get("oi_trend", "?")
        signal = interest.get("signal", "?")
        print(f"  {asset}: [{ok}] OI chg={change:+.1f}%  trend={trend}  signal={signal}")

    print("\n" + "=" * 60)
    print("4. OKX Long/Short ratio (market_positioning)")
    print("=" * 60)
    for asset in ASSETS:
        ratio = get_long_short_ratio(asset)
        ok = "OK  " if ratio.get("source") else "FAIL"
        long_pct = ratio.get("long_pct", 50)
        short_pct = ratio.get("short_pct", 50)
        signal = ratio.get("signal", "?")
        print(f"  {asset}: [{ok}] long={long_pct}%  short={short_pct}%  signal={signal}")

    print("\n" + "=" * 60)
    print("5. Price snapshots (price_data / yfinance)")
    print("=" * 60)
    for asset in ASSETS:
        snapshot = get_snapshot(asset)
        if snapshot:
            print(
                f"  {asset}: [OK  ] close={snapshot['close']:.2f}  "
                f"rsi={snapshot['rsi_1h']:.1f}  "
                f"trend={snapshot.get('trend_4h', '?')}  signal={snapshot['signal']}"
            )
        else:
            print(f"  {asset}: [FAIL] snapshot returned None")

    print("\n" + "=" * 60)
    print("6. Fear & Greed Index")
    print("=" * 60)
    fear_greed = get_fear_and_greed()
    print(
        f"  Value={fear_greed['value']}  Label={fear_greed['label']}  "
        f"err={fear_greed.get('error', '-')}"
    )

    print("\n" + "=" * 60)
    print("7. CryptoPanic headlines")
    print("=" * 60)
    for asset in ["BTC-USD", "ETH-USD"]:
        headlines = get_recent_headlines(asset, limit=3)
        base = asset.split("-")[0]
        print(f"  {base}: {len(headlines)} headlines")
        for line in headlines:
            print(f"    - {line[:75]}")


if __name__ == "__main__":
    main()
