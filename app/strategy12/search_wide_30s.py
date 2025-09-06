from __future__ import annotations
import random
from typing import List

from .rules import RuleVariant


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def generate_rule_variants_wide_30s(max_variants: int = 24) -> List[RuleVariant]:
    """30s-optimized rule generator for quick signals in limited historical data.
    Uses faster EMAs, looser thresholds, and min_bars=20 for 30s compatibility.
    """
    # 30s-tuned seed templates (faster EMAs, looser constraints)
    seeds = [
        {"ema_fast": 3,  "ema_slow": 13, "psar_step": 0.02, "psar_max": 0.2,
         "wpr_period": 10, "wpr_upper_in": -5, "wpr_upper_out": -95,
         "wpr_lower_in": -95, "wpr_lower_out": -5, "min_body_ratio": 0.15,
         "min_ema_dist": 0.0, "min_bars": 20},
        {"ema_fast": 5,  "ema_slow": 18, "psar_step": 0.02, "psar_max": 0.25,
         "wpr_period": 12, "wpr_upper_in": -10, "wpr_upper_out": -90,
         "wpr_lower_in": -90, "wpr_lower_out": -10, "min_body_ratio": 0.12,
         "min_ema_dist": 0.0, "min_bars": 20},
        {"ema_fast": 6,  "ema_slow": 21, "psar_step": 0.03, "psar_max": 0.3,
         "wpr_period": 8, "wpr_upper_in": -15, "wpr_upper_out": -85,
         "wpr_lower_in": -85, "wpr_lower_out": -15, "min_body_ratio": 0.18,
         "min_ema_dist": 0.0, "min_bars": 20},
        {"ema_fast": 4,  "ema_slow": 16, "psar_step": 0.025, "psar_max": 0.25,
         "wpr_period": 14, "wpr_upper_in": -8, "wpr_upper_out": -92,
         "wpr_lower_in": -92, "wpr_lower_out": -8, "min_body_ratio": 0.10,
         "min_ema_dist": 0.0, "min_bars": 20},
    ]

    variants: List[RuleVariant] = []
    rnd = random.Random(42)  # Fixed seed for reproducibility

    # Expand seeds with 30s-friendly perturbations
    for s in seeds:
        for _ in range(5):  # 4*5 = 20 + seeds = 24
            p = dict(s)
            # Keep EMAs fast for 30s responsiveness
            p["ema_fast"] = int(_clip(p["ema_fast"] + rnd.choice([-1, 0, 1, 2]), 3, 8))
            p["ema_slow"] = int(_clip(p["ema_slow"] + rnd.choice([-3, -1, 0, 2, 4]), 10, 30))
            p["wpr_period"] = int(_clip(p.get("wpr_period", 12) + rnd.choice([-2, -1, 0, 1, 2]), 6, 20))
            
            # Very loose body ratio for quick signals
            base_mbr = p.get("min_body_ratio", 0.15)
            p["min_body_ratio"] = round(_clip(base_mbr + rnd.choice([-0.05, -0.03, 0.0, 0.02, 0.04]), 0.08, 0.35), 2)
            
            # Extremely loose %R zones for 30s
            if rnd.random() < 0.9:
                p["wpr_upper_in"] = -int(_clip(abs(p.get("wpr_upper_in", -10)) + rnd.choice([0, 2, 5, 8]), 3, 25))
                p["wpr_upper_out"] = -int(_clip(abs(p.get("wpr_upper_out", -90)) + rnd.choice([0, 2, 5]), 75, 98))
                p["wpr_lower_in"] = -int(_clip(abs(p.get("wpr_lower_in", -90)) + rnd.choice([0, 2, 5, 8]), 75, 98))
                p["wpr_lower_out"] = -int(_clip(abs(p.get("wpr_lower_out", -10)) + rnd.choice([0, 2, 5]), 3, 25))
            
            # No EMA distance constraint for maximum signals
            p["min_ema_dist"] = 0.0
            p["min_bars"] = 20  # Always 20 for 30s
            
            variants.append(RuleVariant(name=f"12w30.{len(variants)+1}", params=p))

    # Include seeds themselves
    for s in seeds:
        variants.append(RuleVariant(name=f"12w30.{len(variants)+1}", params=s))

    # Shuffle and cap
    rnd.shuffle(variants)
    if len(variants) > max_variants:
        variants = variants[:max_variants]

    # Rename to 12.1, 12.2... for consistency
    out: List[RuleVariant] = []
    for i, v in enumerate(variants, start=1):
        out.append(RuleVariant(name=f"12.{i}", params=v.params))
    return out
