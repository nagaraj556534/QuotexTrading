from __future__ import annotations
from typing import Dict, Any, Tuple

from .feature_engine import collect_live_features
from .models.rule_based import RuleBasedAI
from .models.ml_model import MLPredictor

class EnsembleAnalyzer:
    def __init__(self) -> None:
        self.rule = RuleBasedAI()
        self.ml = MLPredictor()

    async def analyze(self, qx, asset: str) -> Dict[str, Any]:
        features = await collect_live_features(qx, asset)
        r = self.rule.predict(features)
        m = self.ml.predict(features)
        # Simple ensemble: average confidence if directions agree, else pick higher conf
        if r["direction"] == m["direction"]:
            direction = r["direction"]
            confidence = min(1.0, 0.5 * (r["confidence"] + m["confidence"]))
        else:
            # Prefer model with higher confidence
            pair = sorted([(r["confidence"], r["direction"]), (m["confidence"], m["direction"])], reverse=True)
            confidence, direction = pair[0]
        return {"features": features, "direction": direction, "confidence": float(confidence)}

