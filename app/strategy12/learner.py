from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple

from .rules import RuleVariant


@dataclass
class SynthesizedVariant:
    variant: RuleVariant
    source_predicates: Tuple[str, ...]
    direction: str


def synthesize_rule_variants_from_patterns(
    patterns: List[dict] | List[object], base_name: str = "12m"
) -> List[SynthesizedVariant]:
    """
    Convert discovered patterns to RuleVariant params.
    Heuristic mapping from predicate names to rule parameters.
    Accepts either dataclass Pattern instances or dict-like objects.

    Enhancement: emit multi-profile variants per pattern (conservative/balanced/aggressive)
    to cover accuracy-frequency trade-offs for 60s timeframe.
    """
    out: List[SynthesizedVariant] = []
    seq = 1
    for p in patterns:
        # Support both dataclass Pattern instances and dicts without evaluating defaults eagerly
        if hasattr(p, 'predicates'):
            preds: Tuple[str, ...] = tuple(getattr(p, 'predicates'))  # type: ignore
        else:
            preds = tuple(p.get('predicates', ()))  # type: ignore
        if hasattr(p, 'direction'):
            direction: str = str(getattr(p, 'direction'))  # type: ignore
        else:
            direction = str(p.get('direction', 'call'))  # type: ignore

        def _base_params() -> Dict[str, Any]:
            return {
                "ema_fast": 11,
                "ema_slow": 55,
                "psar_step": 0.02,
                "psar_max": 0.3,
                "wpr_period": 14,
                "wpr_upper_in": -20,
                "wpr_upper_out": -80,
                "wpr_lower_in": -80,
                "wpr_lower_out": -20,
                "min_body_ratio": 0.25,
                "min_ema_dist": 0.0,
                "min_bars": 120,
            }

        def _apply_mined(preds: Tuple[str, ...], params: Dict[str, Any]) -> None:
            # Direction bias: choose cross + psar polarity consistent (implicit at eval)
            # EMA distance thresholds
            for t in ("0p02", "0p01", "0p00"):
                key = f"ema_dist_ge_{t}"
                if key in preds:
                    params["min_ema_dist"] = {"0p02": 0.02, "0p01": 0.01, "0p00": 0.0}[t]
                    break
            # Body thresholds
            for t in ("0p40", "0p35", "0p30", "0p20"):
                key = f"body_ge_{t}"
                if key in preds:
                    params["min_body_ratio"] = {"0p40": 0.40, "0p35": 0.35, "0p30": 0.30, "0p20": 0.20}[t]
                    break
            # W%R crossings → set in/out consistent with mined predicates
            if "wpr_cross_up_from_oversold" in preds and direction == "call":
                params["wpr_upper_in"] = -20
                params["wpr_upper_out"] = -80
            if "wpr_cross_dn_from_overbought" in preds and direction == "put":
                params["wpr_lower_in"] = -80
                params["wpr_lower_out"] = -20

        # Balanced profile (as-is mined mapping) + recent window + confluence baseline
        p_bal = _base_params()
        _apply_mined(preds, p_bal)
        p_bal["recent_window"] = 2  # last 2 bars eligible for signal
        p_bal["min_confluence_score"] = 5.0
        name_bal = f"{base_name}.{seq}-bal"
        out.append(SynthesizedVariant(variant=RuleVariant(name=name_bal, params=p_bal), source_predicates=preds, direction=direction))

        # Conservative profile (tighter thresholds, higher confluence)
        p_cons = dict(p_bal)
        p_cons["min_body_ratio"] = min(0.80, round(p_cons.get("min_body_ratio", 0.25) + 0.10, 2))
        p_cons["min_ema_dist"] = min(0.20, round(p_cons.get("min_ema_dist", 0.0) + 0.02, 3))
        if direction == "call":
            p_cons["wpr_upper_in"] = -15
            p_cons["wpr_upper_out"] = -85
        else:
            p_cons["wpr_lower_in"] = -85
            p_cons["wpr_lower_out"] = -15
        p_cons["min_confluence_score"] = 6.0
        name_cons = f"{base_name}.{seq}-cons"
        out.append(SynthesizedVariant(variant=RuleVariant(name=name_cons, params=p_cons), source_predicates=preds, direction=direction))

        # Aggressive profile (looser thresholds, lower confluence)
        p_agg = dict(p_bal)
        p_agg["min_body_ratio"] = max(0.05, round(p_agg.get("min_body_ratio", 0.25) - 0.10, 2))
        p_agg["min_ema_dist"] = max(0.0, round(p_agg.get("min_ema_dist", 0.0) - 0.02, 3))
        if direction == "call":
            p_agg["wpr_upper_in"] = -30
            p_agg["wpr_upper_out"] = -70
        else:
            p_agg["wpr_lower_in"] = -70
            p_agg["wpr_lower_out"] = -30
        p_agg["min_confluence_score"] = 4.0
        name_agg = f"{base_name}.{seq}-agg"
        out.append(SynthesizedVariant(variant=RuleVariant(name=name_agg, params=p_agg), source_predicates=preds, direction=direction))

        # Increment sequence per mined pattern (keeps profiles grouped)
        seq += 1
    return out


def analyze_failures(trade_log: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Given trade-level diagnostics (per bar which predicates active, win/lose),
    compute simple counts of co-occurring conditions in losers to adjust params.
    Expected trade_log rows: {"predicates": set[str], "direction": str, "won": bool}
    """
    losers = [r for r in trade_log if not r.get("won", False)]
    freq: Dict[str, int] = {}
    for r in losers:
        for p in r.get("predicates", []):
            freq[p] = freq.get(p, 0) + 1
    return {"loser_predicate_freq": freq}


def refine_variant_by_analysis(base: RuleVariant, analysis: Dict[str, Any], round_idx: int = 0) -> RuleVariant:
    """
    Adjust parameters based on common loser predicates.
    Tighten body_ratio and EMA distance; optionally tighten W%R in/out.
    """
    p = dict(base.params)
    loser_freq: Dict[str, int] = analysis.get("loser_predicate_freq", {})
    # If many losers had small body → increase min_body_ratio
    if any(k.startswith("body_ge_0p2") for k in loser_freq.keys()):
        p["min_body_ratio"] = min(0.6, round(p.get("min_body_ratio", 0.25) + 0.05 + 0.05 * round_idx, 2))
    # If losers often had ema_dist_ge_0p00 → require more distance
    if any(k.startswith("ema_dist_ge_0p00") for k in loser_freq.keys()):
        p["min_ema_dist"] = min(0.2, round(p.get("min_ema_dist", 0.0) + 0.01 + 0.01 * round_idx, 3))
    # If losers often had no W%R cross, we cannot see it here; but we can tighten zones a bit conservatively
    p["wpr_upper_in"] = -min(40, abs(int(p.get("wpr_upper_in", -20))) + 5)
    p["wpr_upper_out"] = -min(95, abs(int(p.get("wpr_upper_out", -80))) + 5)
    p["wpr_lower_in"] = -min(95, abs(int(p.get("wpr_lower_in", -80))) + 5)
    p["wpr_lower_out"] = -min(40, abs(int(p.get("wpr_lower_out", -20))) + 5)
    return RuleVariant(name=f"{base.name}-r{round_idx+1}", params=p)

