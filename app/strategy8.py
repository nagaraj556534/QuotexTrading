import json
import os
from typing import Dict, Any, Tuple, List

# Strategy 8: Data-Discovered from optimizer output
# Loads research/strategy8_config.json and applies simple threshold rules

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "research", "strategy8_config.json")


def load_config() -> Dict[str, Any]:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def s8_has_signal(features: Dict[str, float], cfg: Dict[str, Any]) -> Tuple[bool, str]:
    params = cfg.get("params", {})
    body_min = float(params.get("body_min", 0.3))
    adx_min = float(params.get("adx_min", 18))
    oslv = float(params.get("stoch_os", 20))
    obuv = float(params.get("stoch_ob", 80))
    dist_frac = float(params.get("dist_frac_atr", 0.05))

    body_ok = features.get("body_ratio", 0.0) >= body_min
    adx_ok = features.get("adx14", 0.0) >= adx_min
    atr_val = features.get("atr14", 0.0)
    dist_ok = abs(features.get("dist_ema21", 0.0)) >= dist_frac * atr_val if atr_val else False

    k = features.get("stoch_k", 50.0)
    d = features.get("stoch_d", 50.0)
    stoch_buy = (k > d) and min(k, d) <= oslv
    stoch_sell = (k < d) and max(k, d) >= obuv

    if body_ok and adx_ok and dist_ok:
        if stoch_buy:
            return True, "call"
        if stoch_sell:
            return True, "put"
    return False, "call"

