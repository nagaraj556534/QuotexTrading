from __future__ import annotations
from typing import List, Dict, Any


def rank_variants(results: List[dict]) -> List[dict]:
    """Sort by accuracy desc, then by total_trades desc, apply simple stability score on hours."""
    def stability_penalty(res: dict) -> float:
        hours = res.get("ist_hour_stats", {})
        # Penalize if accuracy varies too much across hours (simple proxy: min bucket trades)
        min_bucket = min((t for (w, t) in hours.values()), default=0)
        return 0.0 if min_bucket >= 5 else 0.05

    for r in results:
        r["score"] = r["accuracy"] - stability_penalty(r)
    return sorted(results, key=lambda x: (x["score"], x["total_trades"]), reverse=True)

