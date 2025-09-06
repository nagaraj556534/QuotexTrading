from __future__ import annotations
from typing import Dict, Any

DEFAULTS = {
    "min_conf": 0.90,
    "min_payout": 90.0,
    "atr_pctile_min": 0.2,
    "atr_pctile_max": 0.9,
    "liq_min": 0.5,
    "mtf_confluence": True,
}

def _mtf_confluence_ok(features: Dict[str, Any]) -> bool:
    trend = features.get("trend", {})
    agree = 0
    for tf in (60, 30, 15):
        t = trend.get(tf, {})
        if t.get("align_up") or t.get("align_down"):
            agree += 1
    return agree >= 2


def pass_quality_gates(features: Dict[str, Any], decision: Dict[str, Any], payout: float, cfg: Dict[str, Any] | None = None) -> bool:
    cfg = dict(DEFAULTS | (cfg or {}))

    # Confidence
    if float(decision.get("confidence", 0.0)) < float(cfg["min_conf"]):
        return False
    # Payout
    if float(payout) < float(cfg["min_payout"]):
        return False
    # Volatility regime
    atrp = float(features.get("volatility", {}).get(60, {}).get("atr_pctile", 0.5))
    if not (float(cfg["atr_pctile_min"]) <= atrp <= float(cfg["atr_pctile_max"])):
        return False
    # Liquidity
    if float(features.get("liquidity_score", 0.0)) < float(cfg["liq_min"]):
        return False
    # Market condition filter
    regime = features.get("regime", "")
    if regime == "volatile":
        return False
    # Multi-timeframe confluence
    if bool(cfg.get("mtf_confluence", True)) and not _mtf_confluence_ok(features):
        return False
    return True

