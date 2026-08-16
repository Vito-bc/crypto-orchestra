"""Manual market-positioning review, with import-safe pytest collection."""

from tools.market_positioning import (
    get_binance_funding_rate,
    get_long_short_ratio,
    get_open_interest,
)


def test_whale_review_dependencies_are_importable() -> None:
    assert all(callable(fn) for fn in (
        get_open_interest,
        get_long_short_ratio,
        get_binance_funding_rate,
    ))


def main() -> None:
    print("=== BTC-USD ===")
    interest = get_open_interest("BTC-USD")
    print(
        f"OI: ${interest['oi_usd']:,.0f}  change: {interest['oi_change_pct']:+.2f}%  "
        f"trend: {interest['oi_trend']}"
    )
    print(f"Signal: {interest['signal']}  conf: {interest['confidence']:.0%}")
    print(f"-> {interest['interpretation']}")

    ratio = get_long_short_ratio("BTC-USD")
    print(
        f"L/S: {ratio['long_pct']}% long / {ratio['short_pct']}% short "
        f"-> {ratio['signal']}"
    )
    print(f"-> {ratio['interpretation']}")

    funding = get_binance_funding_rate("BTC-USD")
    print(f"Binance funding: {funding['rate_pct']:+.5f}% -> {funding['signal']}")

    print("\n=== ZEC-USD (no Binance perp) ===")
    zec_interest = get_open_interest("ZEC-USD")
    print(f"OI: {zec_interest['interpretation']}")


if __name__ == "__main__":
    main()
