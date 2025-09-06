from __future__ import annotations
import random
from typing import List, Dict, Any, Tuple, Optional

from .rules import RuleVariant, default_rule_space


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _grid(values: List[Any]) -> List[Any]:
    return list(values)


def _strict_wr_zones(base: Dict[str, Any], tighten: bool = True) -> Dict[str, Any]:
    p = dict(base)
    if tighten:
        p["wpr_upper_in"] = -int(_clip(abs(p.get("wpr_upper_in", -20)) + 5, 10, 35))
        p["wpr_upper_out"] = -int(_clip(abs(p.get("wpr_upper_out", -80)) + 5, 70, 95))
        p["wpr_lower_in"] = -int(_clip(abs(p.get("wpr_lower_in", -80)) + 5, 70, 95))
        p["wpr_lower_out"] = -int(_clip(abs(p.get("wpr_lower_out", -20)) + 5, 10, 35))
    return p


def _smart_templates(timeframes: List[int], strict_bias: bool) -> List[Dict[str, Any]]:
    """Curated base templates per timeframe with sane defaults for high precision."""
    bases: List[Dict[str, Any]] = []
    want_30 = 30 in timeframes
    want_15 = 15 in timeframes
    want_60 = 60 in timeframes or not timeframes
    if want_60:
        for ef, es in [(9, 34), (11, 55), (12, 55), (13, 55)]:
            p = {
                "ema_fast": ef, "ema_slow": es, "psar_step": 0.02, "psar_max": 0.3,
                "wpr_period": 12, "wpr_upper_in": -20, "wpr_upper_out": -80,
                "wpr_lower_in": -80, "wpr_lower_out": -20,
                "min_body_ratio": 0.35 if not strict_bias else 0.42,
                "min_ema_dist": 0.02 if strict_bias else 0.0,
                "min_bars": 120,
            }
            bases.append(_strict_wr_zones(p, tighten=strict_bias))
    if want_15:
        for ef, es in [(7, 34), (9, 34), (11, 55)]:
            p = {
                "ema_fast": ef, "ema_slow": es, "psar_step": 0.02, "psar_max": 0.3,
                "wpr_period": 12, "wpr_upper_in": -20, "wpr_upper_out": -80,
                "wpr_lower_in": -80, "wpr_lower_out": -20,
                "min_body_ratio": 0.30 if not strict_bias else 0.40,
                "min_ema_dist": 0.02 if strict_bias else 0.0,
                "min_bars": 120,
            }
            bases.append(_strict_wr_zones(p, tighten=strict_bias))
    if want_30:
        for ef, es in [(5, 20), (8, 21), (9, 26)]:
            p = {
                "ema_fast": ef, "ema_slow": es, "psar_step": 0.02, "psar_max": 0.2,
                "wpr_period": 12, "wpr_upper_in": -15, "wpr_upper_out": -85,
                "wpr_lower_in": -85, "wpr_lower_out": -15,
                "min_body_ratio": 0.15 if not strict_bias else 0.22,
                "min_ema_dist": 0.0,
                "min_bars": 20,
            }
            bases.append(_strict_wr_zones(p, tighten=strict_bias))
    # Add trend parameters defaults for each template
    for p in bases:
        p.setdefault("trend_lookback", 30)
        p.setdefault("trend_strength_min", 0.35)
        p.setdefault("require_align30", True)
    return bases


def generate_rule_variants_smart(max_variants: int = 8, timeframes: Optional[List[int]] = None, strict_bias: bool = False) -> List[RuleVariant]:
    """Generate curated, high-precision-biased variants.
    - timeframes: guide templates (15/30/60)
    - strict_bias: bias toward tighter bodies/EMA distance/%R zones
    """
    tf = timeframes or [15, 30, 60]
    base = _smart_templates(tf, strict_bias)
    # Diversify with small, controlled perturbations around curated bases
    rnd = random.Random()
    more: List[RuleVariant] = []
    for params in base:
        for _ in range(3 if strict_bias else 2):
            p = dict(params)
            # EMA jitter
            p["ema_fast"] = int(_clip(p["ema_fast"] + rnd.choice([-1, 0, 1]), 3, 30))
            p["ema_slow"] = int(_clip(p["ema_slow"] + rnd.choice([-5, 0, 5]), 20, 120))
            # W%R period +/-
            p["wpr_period"] = int(_clip(p.get("wpr_period", 12) + rnd.choice([-2, -1, 0, 1, 2]), 8, 20))
            # Body ratio tighten/loosen slightly
            step_br = 0.03 if strict_bias else 0.02
            p["min_body_ratio"] = round(_clip(p.get("min_body_ratio", 0.30) + rnd.choice([-step_br, 0.0, step_br]), 0.10, 0.55), 2)
            # EMA distance increment (for 60s/15s)
            if p.get("min_bars", 60) >= 120:
                p["min_ema_dist"] = round(_clip(p.get("min_ema_dist", 0.0) + rnd.choice([0.0, 0.01, 0.02, 0.03]), 0.0, 0.12), 3)
            # Occasionally tighten %R zones further under strict bias
            if strict_bias and rnd.random() < 0.6:
                p = _strict_wr_zones(p, tighten=True)
            more.append(RuleVariant(name="12x", params=p))
        # Trend/TA defaults for new gates
        p.setdefault("adx_min_60", 20)
        p.setdefault("adx_min_30", 18)
        p.setdefault("st_atr_period", 10)
        p.setdefault("st_mult", 3.0)
        p.setdefault("bbw_min", 0.06)
        p.setdefault("kc_mult", 1.5)
        p.setdefault("pullback_min_atr", 0.2)
        p.setdefault("pullback_max_atr", 0.8)
        p.setdefault("ha_confirm", True)
        p.setdefault("trend_lookback", 30)
        p.setdefault("trend_strength_min", 0.35)
        p.setdefault("require_align30", True)

    variants = [RuleVariant(name=f"12b.{i}", params=p) for i, p in enumerate(base, start=1)] + more
    # Cap
    if len(variants) > max_variants:
        rnd.shuffle(variants)
        variants = variants[:max_variants]
    # Rename sequentially 12.1...
    out: List[RuleVariant] = []
    for i, v in enumerate(variants, start=1):
        out.append(RuleVariant(name=f"12.{i}", params=v.params))
    return out


def generate_rule_variants(max_variants: int = 8, widen: bool = False) -> List[RuleVariant]:
    base = default_rule_space()
    if widen:
        extra = [
            {"ema_fast": 5, "ema_slow": 20, "psar_step": 0.02, "psar_max": 0.2, "wpr_period": 14,
             "wpr_upper_in": -15, "wpr_upper_out": -85, "wpr_lower_in": -85, "wpr_lower_out": -15,
             "min_body_ratio": 0.30, "min_ema_dist": 0.0, "min_bars": 120},
            {"ema_fast": 8, "ema_slow": 21, "psar_step": 0.02, "psar_max": 0.2, "wpr_period": 12,
             "wpr_upper_in": -20, "wpr_upper_out": -80, "wpr_lower_in": -80, "wpr_lower_out": -20,
             "min_body_ratio": 0.35, "min_ema_dist": 0.0, "min_bars": 120},
            {"ema_fast": 12, "ema_slow": 26, "psar_step": 0.02, "psar_max": 0.25, "wpr_period": 10,
             "wpr_upper_in": -10, "wpr_upper_out": -90, "wpr_lower_in": -90, "wpr_lower_out": -10,
             "min_body_ratio": 0.30, "min_ema_dist": 0.0, "min_bars": 120},
        ]
        for i, p in enumerate(extra, start=1):
            base.append(RuleVariant(name=f"12x.{i}", params=p))
    more: List[RuleVariant] = []
    for b in base:
        params = dict(b.params)
        for _ in range(2 if not widen else 4):
            p = dict(params)
            p["ema_fast"] = int(_clip(p["ema_fast"] + random.choice([-3, -2, -1, 0, 1, 2, 3]), 3, 25))
            p["ema_slow"] = int(_clip(p["ema_slow"] + random.choice([-8, -5, 0, 5, 8]), 20, 100))
            p["wpr_period"] = int(_clip(p.get("wpr_period", 14) + random.choice([-3, -2, -1, 0, 1, 2, 3]), 8, 28))
            p["min_body_ratio"] = round(_clip(p.get("min_body_ratio", 0.25) + random.choice([-0.08, -0.05, 0.0, 0.05, 0.08]), 0.10, 0.55), 2)
            p["min_ema_dist"] = round(_clip(p.get("min_ema_dist", 0.0) + random.choice([0.0, 0.0, 0.02, 0.03, 0.05]), 0.0, 0.2), 3)
            if random.random() < 0.5:
                p["wpr_upper_in"] = -int(_clip(abs(p.get("wpr_upper_in", -20)) + random.choice([0, 5, 10]), 5, 35))
                p["wpr_upper_out"] = -int(_clip(abs(p.get("wpr_upper_out", -80)) + random.choice([0, 5, 10]), 60, 95))
                p["wpr_lower_in"] = -int(_clip(abs(p.get("wpr_lower_in", -80)) + random.choice([0, 5, 10]), 60, 95))
                p["wpr_lower_out"] = -int(_clip(abs(p.get("wpr_lower_out", -20)) + random.choice([0, 5, 10]), 5, 35))
            more.append(RuleVariant(name=f"{b.name}-r{len(more)+1}", params=p))
    variants = base + more
    if len(variants) > max_variants:
        random.shuffle(variants)
        variants = variants[:max_variants]
    out: List[RuleVariant] = []
    for i, v in enumerate(variants, start=1):
        out.append(RuleVariant(name=f"12.{i}", params=v.params))
    return out


def refine_variant(var: RuleVariant, intensity: float = 1.0, seed: int | None = None) -> RuleVariant:
    """Return a modified RuleVariant with stricter filters to aim for higher precision.
    intensity scales how aggressive the tightening is.
    """
    rnd = random.Random(seed)
    p = dict(var.params)
    p["min_body_ratio"] = round(_clip(p.get("min_body_ratio", 0.25) + 0.05 * intensity, 0.15, 0.6), 2)
    p["min_ema_dist"] = round(_clip(p.get("min_ema_dist", 0.0) + 0.02 * intensity, 0.0, 0.3), 3)
    p["wpr_upper_in"] = -int(_clip(abs(p.get("wpr_upper_in", -20)) + 5 * intensity, 10, 40))
    p["wpr_upper_out"] = -int(_clip(abs(p.get("wpr_upper_out", -80)) + 5 * intensity, 70, 95))
    p["wpr_lower_in"] = -int(_clip(abs(p.get("wpr_lower_in", -80)) + 5 * intensity, 70, 95))
    p["wpr_lower_out"] = -int(_clip(abs(p.get("wpr_lower_out", -20)) + 5 * intensity, 10, 40))
    p["ema_fast"] = int(_clip(p["ema_fast"] + rnd.choice([-1, 0, 1]), 3, 25))
    p["ema_slow"] = int(_clip(p["ema_slow"] + rnd.choice([-5, 0, 5]), 20, 100))
    return RuleVariant(name=f"{var.name}-opt", params=p)


def generate_broader_variants(count: int = 12) -> List[RuleVariant]:
    return generate_rule_variants(max_variants=count, widen=True)
