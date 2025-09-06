from __future__ import annotations
import random
from typing import List

from .rules import RuleVariant


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def generate_rule_variants_wide_60s(max_variants: int = 32) -> List[RuleVariant]:
    """More permissive rule generator optimized for 60s to boost trade count.
    Looser bodies, wider %R zones, smaller EMA distance thresholds.
    """
    # Seed templates (looser than default)
    seeds = [
        {"ema_fast": 5,  "ema_slow": 20, "psar_step": 0.02, "psar_max": 0.2,
         "wpr_period": 14, "wpr_upper_in": -10, "wpr_upper_out": -90,
         "wpr_lower_in": -90, "wpr_lower_out": -10, "min_body_ratio": 0.20,
         "min_ema_dist": 0.0, "min_bars": 120},
        {"ema_fast": 8,  "ema_slow": 21, "psar_step": 0.02, "psar_max": 0.2,
         "wpr_period": 12, "wpr_upper_in": -15, "wpr_upper_out": -85,
         "wpr_lower_in": -85, "wpr_lower_out": -15, "min_body_ratio": 0.18,
         "min_ema_dist": 0.0, "min_bars": 120},
        {"ema_fast": 12, "ema_slow": 26, "psar_step": 0.02, "psar_max": 0.25,
         "wpr_period": 10, "wpr_upper_in": -20, "wpr_upper_out": -80,
         "wpr_lower_in": -80, "wpr_lower_out": -20, "min_body_ratio": 0.16,
         "min_ema_dist": 0.0, "min_bars": 120},
    ]

    variants: List[RuleVariant] = []
    rnd = random.Random()

    # Expand seeds with wide perturbations aimed at 60s
    for s in seeds:
        for _ in range(8):  # 3*8 = 24 + seeds themselves ≈ 27
            p = dict(s)
            p["ema_fast"] = int(_clip(p["ema_fast"] + rnd.choice([-3, -2, -1, 0, 1, 2, 3]), 3, 25))
            p["ema_slow"] = int(_clip(p["ema_slow"] + rnd.choice([-8, -5, 0, 5, 8]), 20, 100))
            p["wpr_period"] = int(_clip(p.get("wpr_period", 14) + rnd.choice([-3, -2, -1, 0, 1, 2, 3]), 8, 28))
            # Looser min_body_ratio range 0.15–0.40
            base_mbr = p.get("min_body_ratio", 0.20)
            p["min_body_ratio"] = round(_clip(base_mbr + rnd.choice([-0.07, -0.05, -0.02, 0.0, 0.02, 0.05, 0.07]), 0.15, 0.40), 2)
            # Looser %R zones more frequently
            if rnd.random() < 0.8:
                p["wpr_upper_in"] = -int(_clip(abs(p.get("wpr_upper_in", -15)) + rnd.choice([0, 5, 10, 15]), 5, 40))
                p["wpr_upper_out"] = -int(_clip(abs(p.get("wpr_upper_out", -85)) + rnd.choice([0, 5, 10]), 60, 95))
                p["wpr_lower_in"] = -int(_clip(abs(p.get("wpr_lower_in", -85)) + rnd.choice([0, 5, 10, 15]), 60, 95))
                p["wpr_lower_out"] = -int(_clip(abs(p.get("wpr_lower_out", -15)) + rnd.choice([0, 5, 10]), 5, 40))
            # Reduce min_ema_dist constraint
            p["min_ema_dist"] = round(_clip(p.get("min_ema_dist", 0.0) + rnd.choice([0.0, 0.0, 0.01, 0.02]), 0.0, 0.15), 3)
            variants.append(RuleVariant(name=f"12w.{len(variants)+1}", params=p))

    # Include seeds themselves
    for s in seeds:
        variants.append(RuleVariant(name=f"12w.{len(variants)+1}", params=s))

    # Cap/Shuffle
    rnd.shuffle(variants)
    if len(variants) > max_variants:
        variants = variants[:max_variants]

    # Rename to 12.1, 12.2... while keeping params
    out: List[RuleVariant] = []
    for i, v in enumerate(variants, start=1):
        out.append(RuleVariant(name=f"12.{i}", params=v.params))
    return out

