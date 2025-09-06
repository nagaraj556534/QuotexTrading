from __future__ import annotations
from typing import Tuple, Optional, Dict, Any, List
import asyncio
from zoneinfo import ZoneInfo
from datetime import datetime

from .live_analyzer import EnsembleAnalyzer
from .quality_gates import pass_quality_gates

async def _get_assets(qx) -> List[str]:
    try:
        inst = await qx.get_instruments()
        return [i[1] for i in inst] if inst else []
    except Exception:
        return []

async def find_first_signal_ai(
    qx,
    min_confidence: float = 0.90,
    min_payout: float = 90.0,
    quality_cfg: Optional[Dict[str, Any]] = None,
    allowed_hours: Optional[set[int]] = None,
    debug: bool = False,
    live_hour_override: bool = False,
    current_ist_hour_force: int | None = None,
) -> Tuple[Optional[str], Optional[str]]:
    assets = await _get_assets(qx)
    if not assets:
        return None, None

    # IST hour filter (bypass if override enabled)
    if not live_hour_override and allowed_hours is not None and len(allowed_hours) > 0:
        try:
            cur_hour_ist = int(datetime.now(ZoneInfo("Asia/Kolkata")).hour)
            if cur_hour_ist not in allowed_hours:
                if debug:
                    print(f"[S14] Hour {cur_hour_ist} not in allowed IST hours; skipping scan.")
                return None, None
        except Exception:
            pass

    analyzer = EnsembleAnalyzer()

    for idx, asset in enumerate(assets, start=1):
        try:
            # Quick payout gate first
            try:
                payout = qx.get_payout_by_asset(asset, timeframe="1")
                payout = float(payout or 0.0)
            except Exception:
                payout = 0.0
            if payout < min_payout:
                continue

            # Analyze
            res = await asyncio.wait_for(analyzer.analyze(qx, asset), timeout=10.0)
            decision = {"direction": res["direction"], "confidence": res["confidence"]}
            features = res["features"]

            qc = dict(quality_cfg or {})
            if "min_conf" not in qc:
                qc["min_conf"] = float(min_confidence)
            if "min_payout" not in qc:
                qc["min_payout"] = float(min_payout)

            if pass_quality_gates(features, decision, payout, qc):
                if debug:
                    print(f"[S14] Signal {decision['direction']} on {asset} conf={decision['confidence']:.2f} payout={payout:.0f}% regime={features.get('regime')}")
                return asset, decision["direction"]
        except asyncio.TimeoutError:
            if debug:
                print(f"[S14] Timeout analyzing {asset}")
        except Exception as e:
            if debug:
                print(f"[S14] Error on {asset}: {e}")
            continue

    if debug:
        print("[S14] No signals this pass.")
    return None, None

