from typing import Dict, Any, List, Tuple, Optional

from .features import ema, williams_r, body_ratio
# Try to import PSAR helper similar to main.py style
try:
    from strategy10_confluence import compute_psar  # when app/ is on sys.path
except Exception:
    try:
        from . import compute_psar  # unlikely path
    except Exception:
        compute_psar = None  # fallback: rule will behave as if psar not available

# Rule templates for S12 Phase 1: Combos of EMA/PSAR/Williams%R + candle body

class RuleVariant:
    def __init__(self, name: str, params: Dict[str, Any]):
        self.name = name
        self.params = params

    def evaluate(self, candles: List[Dict[str, Any]]) -> Tuple[bool, str]:
        """Return (has_signal, direction).
        Supports optional:
        - recent_window: consider last K bars (default 1)
        - min_confluence_score: weighted indicator confluence threshold (default 5.0)
        """
        if not candles or len(candles) < max(60, self.params.get("min_bars", 60)):
            return False, "call"
        opens = [float(x["open"]) for x in candles]
        highs = [float(x["high"]) for x in candles]
        lows = [float(x["low"]) for x in candles]
        closes = [float(x["close"]) for x in candles]

        ema_fast = ema(closes, self.params.get("ema_fast", 11))
        ema_slow = ema(closes, self.params.get("ema_slow", 55))
        psar = compute_psar(highs, lows, step=self.params.get("psar_step", 0.02),
                             max_step=self.params.get("psar_max", 0.3)) if compute_psar else [float('inf')] * len(closes)
        wr = williams_r(highs, lows, closes, period=self.params.get("wpr_period", 14))
        br = body_ratio(opens, closes, highs, lows)

        if len(ema_fast) < 2 or len(ema_slow) < 2 or len(psar) < 2 or len(wr) < 2 or len(br) < 1:
            return False, "call"

        # Parameters
        recent_window = int(self.params.get("recent_window", 1))
        recent_window = max(1, min(3, recent_window))
        min_conf = float(self.params.get("min_confluence_score", 0.0))

        # Legacy strict mode when no confluence score requested
        def _strict(i: int) -> Tuple[bool, str]:
            cross_up = ema_fast[i-1] <= ema_slow[i-1] and ema_fast[i] > ema_slow[i]
            cross_dn = ema_fast[i-1] >= ema_slow[i-1] and ema_fast[i] < ema_slow[i]
            psar_bull = closes[i] > psar[i]
            psar_bear = closes[i] < psar[i]
            wpr_up = wr[i-1] < self.params.get("wpr_upper_in", -20) and wr[i] > self.params.get("wpr_upper_out", -80)
            wpr_dn = wr[i-1] > self.params.get("wpr_lower_in", -80) and wr[i] < self.params.get("wpr_lower_out", -20)
            br_ok = br[i] >= self.params.get("min_body_ratio", 0.25)
            dist_ok = abs(ema_fast[i] - ema_slow[i]) >= self.params.get("min_ema_dist", 0.0)
            if cross_up and psar_bull and wpr_up and br_ok and dist_ok:
                return True, "call"
            if cross_dn and psar_bear and wpr_dn and br_ok and dist_ok:
                return True, "put"
            return False, "call"

        if min_conf <= 0.0:
            # Evaluate strictly over recent window (default 1)
            for j in range(1, recent_window + 1):
                i = -j
                ok, dirn = _strict(i)
                if ok:
                    return True, dirn
            return False, "call"

        # Confluence scoring mode
        def _score_call(i: int) -> float:
            score = 0.0
            ema_rel = ema_fast[i] > ema_slow[i]
            cross_up = ema_fast[i-1] <= ema_slow[i-1] and ema_fast[i] > ema_slow[i]
            psar_bull = closes[i] > psar[i]
            wpr_up = wr[i-1] <= self.params.get("wpr_upper_in", -20) and wr[i] > self.params.get("wpr_upper_out", -80)
            br_ok = br[i] >= self.params.get("min_body_ratio", 0.25)
            dist_ok = abs(ema_fast[i] - ema_slow[i]) >= self.params.get("min_ema_dist", 0.0)
            if ema_rel:
                score += 1.5
            if cross_up:
                score += 0.5
            if psar_bull:
                score += 2.0
            if wpr_up:
                score += 1.5
            if br_ok:
                score += 1.0
            if dist_ok:
                score += 0.5
            return score

        def _score_put(i: int) -> float:
            score = 0.0
            ema_rel = ema_fast[i] < ema_slow[i]
            cross_dn = ema_fast[i-1] >= ema_slow[i-1] and ema_fast[i] < ema_slow[i]
            psar_bear = closes[i] < psar[i]
            wpr_dn = wr[i-1] >= self.params.get("wpr_lower_in", -80) and wr[i] < self.params.get("wpr_lower_out", -20)
            br_ok = br[i] >= self.params.get("min_body_ratio", 0.25)
            dist_ok = abs(ema_fast[i] - ema_slow[i]) >= self.params.get("min_ema_dist", 0.0)
            if ema_rel:
                score += 1.5
            if cross_dn:
                score += 0.5
            if psar_bear:
                score += 2.0
            if wpr_dn:
                score += 1.5
            if br_ok:
                score += 1.0
            if dist_ok:
                score += 0.5
            return score

        best = (0.0, "call")
        for j in range(1, recent_window + 1):
            i = -j
            sc = _score_call(i)
            sp = _score_put(i)
            if sc >= min_conf and sc >= sp and sc > best[0]:
                best = (sc, "call")
            elif sp >= min_conf and sp > best[0]:
                best = (sp, "put")
        if best[0] >= min_conf:
            return True, best[1]
        return False, "call"


def default_rule_space() -> List[RuleVariant]:
    """Return a small set of rule variants (12.1, 12.2, ...) to explore without external deps."""
    base = [
        {"ema_fast": 11, "ema_slow": 55, "psar_step": 0.02, "psar_max": 0.3, "wpr_period": 14,
         "wpr_upper_in": -20, "wpr_upper_out": -80, "wpr_lower_in": -80, "wpr_lower_out": -20,
         "min_body_ratio": 0.25, "min_ema_dist": 0.0, "min_bars": 120},
        {"ema_fast": 9, "ema_slow": 34, "psar_step": 0.02, "psar_max": 0.2, "wpr_period": 14,
         "wpr_upper_in": -15, "wpr_upper_out": -85, "wpr_lower_in": -85, "wpr_lower_out": -15,
         "min_body_ratio": 0.30, "min_ema_dist": 0.0, "min_bars": 120},
        {"ema_fast": 13, "ema_slow": 55, "psar_step": 0.02, "psar_max": 0.3, "wpr_period": 10,
         "wpr_upper_in": -25, "wpr_upper_out": -75, "wpr_lower_in": -75, "wpr_lower_out": -25,
         "min_body_ratio": 0.20, "min_ema_dist": 0.0, "min_bars": 120},
    ]
    out: List[RuleVariant] = []
    for idx, params in enumerate(base, start=1):
        out.append(RuleVariant(name=f"12.{idx}", params=params))
    return out


async def latest_signal_for_asset(qx, asset: str, timeframe_s: int, candles: Optional[List[Dict[str, Any]]], rule: RuleVariant) -> Tuple[bool, str]:
    """Helper: ensure candles, evaluate rule, return signal."""
    if candles is None:
        try:
            candles = await qx.get_candles(asset, __import__('time').time(), timeframe_s * 1800, timeframe_s)
        except Exception:
            candles = []
    return rule.evaluate(candles)

