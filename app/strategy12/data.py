import time
from typing import List, Dict, Any

# Data access wrappers for Strategy 12 (reusing existing qx methods)

async def fetch_candles(qx, asset: str, timeframe_s: int, bars: int) -> List[Dict[str, Any]]:
    """Fetch candles with retry-light similar to app.main._get_candles_safe.
    timeframe_s: seconds per bar (e.g., 15, 30, 60)
    bars: how many bars desired
    """
    end_ts = time.time()
    try:
        candles = await qx.get_candles(asset, end_ts, timeframe_s * max(60, bars * 2), timeframe_s)
        return candles or []
    except Exception:
        return []

async def scan_assets(qx) -> List[str]:
    """Return all instrument symbols via qx.get_instruments()."""
    names: List[str] = []
    try:
        instruments = await qx.get_instruments()
        for i in instruments:
            try:
                names.append(i[1])
            except Exception:
                continue
    except Exception:
        pass
    return names

