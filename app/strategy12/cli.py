from __future__ import annotations
from typing import Optional, Dict, Any

from .pipeline import run_pipeline, S12Config


async def interactive_select_variant(qx, cfg: S12Config) -> Optional[Dict[str, Any]]:
    """Run pipeline, show ranked list (printed by pipeline), and ask user to pick.
    Returns a dict with keys: variant, params, accuracy, total_trades, wins, ist_hour_stats
    """
    results = await run_pipeline(qx, cfg)
    if not results:
        print("No variants produced results.")
        return None
    # Show indexed list
    for i, r in enumerate(results, start=1):
        print(f"[{i}] {r['variant']} acc={r['accuracy']:.3f} trades={r['total_trades']}")
    try:
        idx = int(input("Select a variant by number: ").strip())
    except Exception:
        idx = 1
    idx = max(1, min(len(results), idx))
    chosen = results[idx - 1]
    print(f"Selected variant: {chosen['variant']} with acc={chosen['accuracy']:.3f} on {chosen['total_trades']} trades")
    return chosen

