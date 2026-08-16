"""Manual ZEC news review, with import-safe pytest collection."""

from tools.asset_news import get_asset_headlines


def test_news_review_dependency_is_importable() -> None:
    assert callable(get_asset_headlines)


def main() -> None:
    result = get_asset_headlines("ZEC-USD", limit=10)
    print(f"Headlines found: {len(result['headlines'])}")
    print(f"Sources: {result['sources']}")
    print(f"Critical alert: {result['critical_alert']}")
    print(f"Negative flags: {result['negative_flags']}")
    print(f"Error: {result.get('error')}")
    print()
    for headline, age in zip(result["headlines"], result["headline_ages_days"]):
        age_str = f"{age}d ago" if age is not None else "age unknown"
        print(f"  [{age_str}] {headline[:90]}")


if __name__ == "__main__":
    main()
