from __future__ import annotations
from typing import Dict, Any

class RuleBasedAI:
    """Fast, interpretable rule-based scorer.
    Returns a dict with tentative direction and confidence_score (0..1).
    """
    def predict(self, features: Dict[str, Any]) -> Dict[str, Any]:
        trend = features.get("trend", {})
        momentum = features.get("momentum", {})
        vol = features.get("volatility", {})
        patt = features.get("patterns", {})
        levels = features.get("levels", {})
        regime = features.get("regime", "unknown")

        # Multi-TF alignment (60s/30s/15s)
        def _align_bias() -> str:
            up = 0; down = 0
            for tf in (60, 30, 15):
                t = trend.get(tf, {})
                if t.get("align_up"):
                    up += 1
                if t.get("align_down"):
                    down += 1
            if up >= 2 and down == 0:
                return "call"
            if down >= 2 and up == 0:
                return "put"
            return "none"

        bias = _align_bias()
        conf = 0.0

        # Volatility gating: prefer mid regime
        atrp = float(vol.get(60, {}).get("atr_pctile", 0.5))
        if 0.2 <= atrp <= 0.9:
            conf += 0.15
        else:
            conf -= 0.10

        # RSI momentum confirmation
        rsi60 = float(momentum.get(60, {}).get("rsi", 50.0))
        rsi30 = float(momentum.get(30, {}).get("rsi", 50.0))
        if bias == "call" and (rsi60 >= 52 and rsi30 >= 52):
            conf += 0.25
        if bias == "put" and (rsi60 <= 48 and rsi30 <= 48):
            conf += 0.25

        # Pattern bonus
        if bias == "call" and (patt.get("bull_engulf") or patt.get("pin_bull")):
            conf += 0.20
        if bias == "put" and (patt.get("bear_engulf") or patt.get("pin_bear")):
            conf += 0.20

        # Distance to levels (avoid entry near immediate opposite)
        dist_res = float(levels.get("dist_res", 0.0))
        dist_sup = float(levels.get("dist_sup", 0.0))
        if bias == "call" and dist_res >= 0.0015:
            conf += 0.15
        if bias == "put" and dist_sup >= 0.0015:
            conf += 0.15

        # Regime adjustment
        if regime == "trending" and bias in ("call", "put"):
            conf += 0.15
        if regime == "ranging" and bias == "none":
            conf += 0.05

        # Liquidity
        liq = float(features.get("liquidity_score", 0.5))
        if liq >= 0.5:
            conf += 0.10
        else:
            conf -= 0.10

        direction = bias if bias != "none" else ("call" if rsi60 >= 50 else "put")
        return {"direction": direction, "confidence": max(0.0, min(1.0, conf))}

