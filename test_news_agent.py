"""Quick test: ZEC news with date filtering."""
from tools.asset_news import get_asset_headlines

r = get_asset_headlines("ZEC-USD", limit=10)
print(f"Headlines found: {len(r['headlines'])}")
print(f"Sources: {r['sources']}")
print(f"Critical alert: {r['critical_alert']}")
print(f"Negative flags: {r['negative_flags']}")
print(f"Error: {r.get('error')}")
print()
for h, a in zip(r["headlines"], r["headline_ages_days"]):
    age_str = f"{a}d ago" if a is not None else "age unknown"
    print(f"  [{age_str}] {h[:90]}")
