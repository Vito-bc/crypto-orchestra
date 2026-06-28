"""Test the three entry filters against current market conditions."""
import sys
sys.path.insert(0, ".")
from pipeline.runner import _check_entry_filters

print("=== Entry filter test (current market conditions) ===")
for asset in ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]:
    allowed, reason, size_mod = _check_entry_filters(asset)
    if not allowed:
        status = "BLOCK"
        msg = reason
    elif size_mod < 1.0:
        status = f"ALLOW ({size_mod:.0%} size)"
        msg = reason or "partial correlation veto — size reduced"
    else:
        status = "ALLOW"
        msg = "all filters passed"
    print(f"  {asset}: [{status}]  {msg}")
