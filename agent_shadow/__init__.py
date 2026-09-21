"""Log-only agent observation; intentionally outside the live trading pipeline.

Nothing in this package may import an order module or submit, open, or modify a
position. The structural guard in ``tests/test_agent_shadow.py`` enforces that
boundary over every Python file in this package.
"""

LOG_SCHEMA_VERSION = 1
