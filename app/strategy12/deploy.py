from __future__ import annotations
from typing import List, Dict, Any, Tuple, Optional
import time
from zoneinfo import ZoneInfo
from datetime import datetime

from .rules import RuleVariant
from .features import ema as _ema, williams_r as _wr, body_ratio as _br, atr as _atr
try:
    from strategy10_confluence import compute_psar as _psar  # type: ignore
except Exception:
    try:
        from .strategy10_confluence import compute_psar as _psar  # type: ignore
    except Exception:
        _psar = None  # type: ignore


def _ist_hour(ts: float) -> int:
    try:
        return int(datetime.fromtimestamp(ts, ZoneInfo("Asia/Kolkata")).hour)
    except Exception:
        return int(datetime.utcfromtimestamp(ts).hour)

# Lightweight availability cache (asset -> {ok, at, tradable, payout, reason})
AVAIL_CACHE: dict[str, dict[str, Any]] = {}
AVAIL_TTL_SEC = 45.0


def _avail_cache_get(asset: str):
    try:
        ent = AVAIL_CACHE.get(asset)
        if not ent:
            return None
        if (time.time() - float(ent.get("at", 0))) > AVAIL_TTL_SEC:
            return None
        return ent
    except Exception:
        return None


def _avail_cache_put(asset: str, ok: bool, tradable: bool, payout: float,
                      reason: str):
    try:
        AVAIL_CACHE[asset] = {
            "ok": bool(ok),
            "at": float(time.time()),
            "tradable": bool(tradable),
            "payout": float(payout),
            "reason": str(reason),
        }
    except Exception:
        pass


def _candle_time(c):
    for k in ("from", "time", "timestamp"):
        if k in c:
            return float(c[k])
    return None

# Helpers mirrored from pipeline for gating

def _pct_rank(vals: List[float], v: float) -> float:
    try:
        if not vals:
            return 50.0
        s = sorted([float(x) for x in vals])
        cnt = 0
        for x in s:
            if x <= v:
                cnt += 1
            else:
                break
        return 100.0 * cnt / max(1, len(s))
    except Exception:
        return 50.0

def _ts_safe(c: Dict[str, Any]) -> float:
    t = _candle_time(c)
    return float(t or 0.0)

def _find_idx_leq(candles: List[Dict[str, Any]], ts: float) -> int:
    if not candles:
        return -1
    lo, hi = 0, len(candles) - 1
    ans = -1
    while lo <= hi:
        mid = (lo + hi) // 2
        cts = _ts_safe(candles[mid])
        if cts <= ts:
            ans = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return ans


def _make_practice_eval(candles: List[Dict[str, Any]], params: Dict[str, Any], tf: int):
    """Lightweight evaluator for PRACTICE scanning; mirrors pipeline's fast evaluator exactly."""
    min_bars = 20 if tf == 30 else max(60, int(params.get("min_bars", 60)))
    if not candles or len(candles) < min_bars:
        return lambda prefix: (False, "call")
    opens = [float(x.get("open", 0)) for x in candles]
    highs = [float(x.get("high", 0)) for x in candles]
    lows = [float(x.get("low", 0)) for x in candles]
    closes = [float(x.get("close", 0)) for x in candles]
    e_fast = _ema(closes, int(params.get("ema_fast", 11)))
    e_slow = _ema(closes, int(params.get("ema_slow", 55)))
    wr = _wr(highs, lows, closes, period=int(params.get("wpr_period", 14)))
    br = _br(opens, closes, highs, lows)
    if _psar:
        psar = _psar(highs, lows, step=float(params.get("psar_step", 0.02)), max_step=float(params.get("psar_max", 0.3)))
    else:
        psar = [float('inf')] * len(closes)

    def _eval(prefix: List[Dict[str, Any]]):
        i = len(prefix) - 1
        if len(closes) < min_bars or i < 1 or i >= len(closes):
            return False, "call"
        if i >= len(e_fast) or i >= len(e_slow) or i >= len(psar) or i >= len(wr) or i >= len(br):
            return False, "call"
        cross_up = e_fast[i-1] <= e_slow[i-1] and e_fast[i] > e_slow[i]
        cross_dn = e_fast[i-1] >= e_slow[i-1] and e_fast[i] < e_slow[i]
        psar_bull = closes[i] > psar[i]
        psar_bear = closes[i] < psar[i]
        # Match pipeline inequalities precisely
        wpr_up = wr[i-1] < float(params.get("wpr_upper_in", -20)) and wr[i] > float(params.get("wpr_upper_out", -80))
        wpr_dn = wr[i-1] > float(params.get("wpr_lower_in", -80)) and wr[i] < float(params.get("wpr_lower_out", -20))
        br_thresh = float(params.get("min_body_ratio", 0.10 if tf == 30 else 0.25))
        br_ok = br[i] >= br_thresh
        if tf == 30:
            ema_above = e_fast[i] > e_slow[i]
            ema_below = e_fast[i] < e_slow[i]
            if ((ema_above or cross_up) and (wpr_up or psar_bull) and br_ok):
                return True, "call"
            if ((ema_below or cross_dn) and (wpr_dn or psar_bear) and br_ok):
                return True, "put"
        else:
            dist_ok = abs(e_fast[i] - e_slow[i]) >= float(params.get("min_ema_dist", 0.0))
            if cross_up and psar_bull and wpr_up and br_ok and dist_ok:
                return True, "call"
            if cross_dn and psar_bear and wpr_dn and br_ok and dist_ok:
                return True, "put"
        return False, "call"

    return _eval

async def _candles(qx, asset: str, tf: int, bars: int = 300, raise_on_error: bool = False) -> List[Dict[str, Any]]:
    try:
        return await qx.get_candles(asset, time.time(), tf * max(120, bars), tf)
    except Exception as e:
        if raise_on_error:
            raise
        return []


async def find_first_signal_s12(
    qx,
    rule: RuleVariant,
    min_payout: float,
    timeframes_s: List[int],
    allowed_hours: Optional[set[int]] = None,
    debug: bool = False,
    blacklist: Optional[set[str]] = None,
    cooldowns: Optional[dict[str, float]] = None,
    # strict95 / parity controls
    strict95_mode: bool = False,
    confluence_mode: str = "off",  # "off", "2of3", "3of3", "adaptive"
    gate_body_min: float = 0.33,
    gate_atr_lo: float = 35.0,
    gate_atr_hi: float = 80.0,
    min_conf_override: Optional[float] = None,
    strict_micro: bool = False,
) -> Tuple[Optional[str], Optional[str]]:
    """Scan all assets; for each preferred timeframe, check rule signal and IST hour filter; return first match.
    'cooldowns' may contain asset -> unix_ts_until to temporarily skip re-selection after a failure.
    """
    # Assets
    try:
        instruments = await qx.get_instruments()
    except Exception:
        instruments = []
    assets = [i[1] for i in instruments] if instruments else []

    # Filter by payout and availability; drop blacklisted
    eligible: List[str] = []
    bl = set(blacklist or [])
    for a in assets:
        if a in bl:
            continue
        try:
            p = qx.get_payout_by_asset(a, timeframe="1")
            tradable = True
            try:
                tradable = await qx.is_tradable(a) if hasattr(qx, 'is_tradable') else True
            except Exception:
                tradable = True
            # Raise min payout threshold default to 85%
            if p and float(p) >= float(min_payout) and tradable:
                eligible.append(a)
        except Exception:
            continue
    if debug:
        print(f"[S12] Eligible assets by payout: {len(eligible)}")

    # Force 60s primary, then 30s, then 15s if present
    pref = [60, 30, 15]
    tfs = [t for t in pref if t in timeframes_s] + [t for t in timeframes_s if t not in pref]

    # Limit per-pass scan size and randomize order for responsiveness
    import random as _rnd
    _rnd.shuffle(eligible)
    scan_cap = min(40, len(eligible))  # scan up to 40 assets per pass for speed
    assets_pass = eligible[:scan_cap]
    # Broker-level availability prefilter with cache
    avail_prefilter: List[str] = []
    for a in assets_pass:
        try:
            # Cached check
            ent = _avail_cache_get(a)
            if ent is not None:
                if ent.get("ok"):
                    avail_prefilter.append(a)
                if debug:
                    print(f"[S12] AvailCheck(cache) {a}: ok={ent.get('ok')} reason={ent.get('reason')} payout={ent.get('payout'):.0f}")
                continue
            # Live check
            tradable_now = await qx.is_tradable(a) if hasattr(qx, 'is_tradable') else True
            payout = 0.0
            try:
                p = qx.get_payout_by_asset(a, timeframe="1")
                payout = float(p) if p is not None else 0.0
            except Exception:
                payout = 0.0
            ok = bool(tradable_now and payout >= float(min_payout))
            reason = "ok" if ok else ("not_tradable" if not tradable_now else "below_min_payout")
            _avail_cache_put(a, ok, tradable_now, payout, reason)
            if ok:
                avail_prefilter.append(a)
            if debug:
                print(f"[S12] AvailCheck {a}: ok={ok} reason={reason} payout={payout:.0f}")
        except Exception:
            if debug:
                print(f"[S12] AvailCheck {a}: error during availability check")

    # Use prefiltered list for scanning
    assets_pass = avail_prefilter

    # Resolve hours: prefer variant-optimized hours if present in rule.params
    hours_filter = None
    try:
        rp = getattr(rule, 'params', {}) or {}
        if rp.get('allowed_hours'):
            hours_filter = set(int(h) for h in rp.get('allowed_hours') if isinstance(h, (int, float)))
    except Exception:
        hours_filter = None
    if hours_filter is None:
        hours_filter = allowed_hours

    # Scan assets/timeframes with timeout protection
    import asyncio, time as _t

    # Live hour override support for PRACTICE
    live_hour_override = bool(getattr(rule, 'params', {}).get('live_hour_override', False)) or bool(getattr(qx, 'cfg', None) and getattr(getattr(qx, 'cfg'), 'live_hour_override', False))
    current_ist_hour_force = getattr(rule, 'params', {}).get('current_ist_hour_force', None)

    async def _scan_one_asset(idx: int, a: str, tf: int):
        try:
            if debug:
                print(f"[S12] Scanning {idx}/{scan_cap}: {a} @ {tf}s ...")

            # Cooldown check: skip temporarily cooled assets
            if cooldowns and a in cooldowns and cooldowns[a] > _t.time():
                if debug:
                    print(f"[S12] Skip {a}: cooldown active for {round(cooldowns[a]-_t.time(),1)}s")
                return None, None

            # Optional asset tradability pre-check to save time
            try:
                tradable_now = await qx.is_tradable(a) if hasattr(qx, 'is_tradable') else True
            except Exception:
                tradable_now = True
            if not tradable_now:
                if debug:
                    print(f"[S12] Skip {a}: not tradable now")
                return None, None

            # Fetch candles with higher timeout and retry/backoff for 30s
            max_timeout = 12.0 if tf == 30 else 8.0
            attempts = 3 if tf == 30 else 2
            delay = 0.5
            candles = []
            for k in range(attempts):
                try:
                    candles = await asyncio.wait_for(_candles(qx, a, tf, bars=150), timeout=max_timeout)
                    break
                except asyncio.TimeoutError:
                    if debug:
                        print(f"[S12] Candle fetch timeout {k+1}/{attempts} for {a}@{tf}s; retrying...")
                except Exception as e:
                    if debug:
                        print(f"[S12] Candle fetch error on {a}@{tf}s: {e}")
                await asyncio.sleep(delay)
                delay = min(3.0, delay * 2)
            min_req = 14 if tf == 30 else (60 if tf == 15 else 120)
            if not candles or len(candles) < min_req:
                if debug:
                    n = len(candles) if isinstance(candles, list) else 0
                    print(f"[S12] Skip {a}@{tf}s: insufficient candles ({n})")
                return None, None

            # Use the same fast evaluator across all TFs as pipeline
            fast_eval = _make_practice_eval(candles, getattr(rule, 'params', {}), tf)
            has_signal, direction = await asyncio.wait_for(
                asyncio.to_thread(fast_eval, candles), timeout=3.0
            )
            if not has_signal:
                if debug:
                    print(f"[S12] No signal on {a} @ {tf}s")
                return None, None

            # Optional signal confluence (strict95)
            if strict95_mode and confluence_mode != "off":
                # Get other TF signals where possible
                c_map: Dict[int, List[Dict[str, Any]]] = {tf: candles}
                for tf2 in [15, 30, 60]:
                    if tf2 == tf:
                        continue
                    try:
                        c2 = await _candles(qx, a, tf2, bars=(120 if tf2 != 30 else 40))
                    except Exception:
                        c2 = []
                    c_map[tf2] = c2
                sigs: Dict[int, Tuple[bool, str]] = {}
                for tf2, c2 in c_map.items():
                    need = (60 if tf2 == 15 else (14 if tf2 == 30 else 120))
                    if not isinstance(c2, list) or len(c2) < need:
                        continue
                    ev = _make_practice_eval(c2, getattr(rule, 'params', {}), tf2)
                    ok, dirn = ev(c2)
                    sigs[tf2] = (ok, dirn)
                if confluence_mode == "3of3" and len(sigs) >= 3:
                    from .confluence import three_of_three_confluence as _cc
                elif confluence_mode == "adaptive" and all(t in sigs for t in (15, 30, 60)):
                    from .confluence import three_of_three_confluence as _cc
                else:
                    from .confluence import two_of_three_confluence as _cc
                has_conf, dir_conf = _cc(sigs)
                if not has_conf or dir_conf != direction:
                    if debug:
                        print(f"[S12] Confluence reject {a}@{tf}s")
                    return None, None

            # Hour filter (pretrade) with live override support
            if hours_filter:
                ts = _candle_time(candles[-1])
                hr = _ist_hour(ts) if ts else None
                # If live override ON, bypass the filter; else apply filter normally
                if not live_hour_override and hr is not None and hr not in hours_filter:
                    if debug:
                        print(f"[S12] Hour {hr} not allowed for {a}")
                    return None, None

            # Pretrade gating to mirror pipeline (_gated_backtest_single)
            try:
                # Fetch 60s (anchor) and auxiliary TFs (30s/15s) for gating features
                c60 = await asyncio.wait_for(_candles(qx, a, 60, bars=200), timeout=8.0)
                if not c60 or len(c60) < 120:
                    if debug:
                        print(f"[S12] Gate skip {a}: insufficient 60s bars ({len(c60) if isinstance(c60, list) else 0})")
                    return None, None
                # Optional TFs
                c30 = await asyncio.wait_for(_candles(qx, a, 30, bars=80), timeout=8.0)
                c15 = await asyncio.wait_for(_candles(qx, a, 15, bars=120), timeout=8.0)

                closes60 = [float(x.get("close", 0)) for x in c60]
                e11_60 = _ema(closes60, 11)
                e55_60 = _ema(closes60, 55)
                e7_60  = _ema(closes60, 7)
                atr60  = _atr([float(x.get("high",0)) for x in c60], [float(x.get("low",0)) for x in c60], closes60, 14)

                # Align current ts to 60s index
                ts_cur = _candle_time(candles[-1])
                i60 = _find_idx_leq(c60, ts_cur) if ts_cur else -1
                if i60 < 0 or i60 >= len(c60):
                    return None, None

                # Features at anchor
                align60 = False
                microtrend_ok = False
                body_ok = False
                atr_ok = False
                atr_pctile = 0.0
                if i60 < len(e11_60) and i60 < len(e55_60):
                    align60 = (e11_60[i60] > e55_60[i60]) if direction == "call" else (e11_60[i60] < e55_60[i60])
                # micro-trend: 2 of last 3 bodies in direction AND last 2 closes on EMA7 side
                if i60 >= 2:
                    last3 = c60[i60-2:i60+1]
                    bodies = [(float(k.get("close",0)) - float(k.get("open",0))) for k in last3]
                    dir_bools = [(b > 0) if direction == "call" else (b < 0) for b in bodies]
                    micro_bodies_ok = sum(1 for x in dir_bools if x) >= 2
                    if i60 >= 1 and i60 < len(e7_60):
                        c1 = (float(c60[i60-1]["close"]) > float(e7_60[i60-1])) if direction == "call" else (float(c60[i60-1]["close"]) < float(e7_60[i60-1]))
                        c2 = (float(c60[i60]["close"])   > float(e7_60[i60]))   if direction == "call" else (float(c60[i60]["close"])   < float(e7_60[i60]))
                        microtrend_ok = micro_bodies_ok and (c1 and c2)
                # body ratio on 60s bar at anchor
                try:
                    body = abs(float(c60[i60]["close"]) - float(c60[i60]["open"]))
                    rng = max(1e-9, float(c60[i60]["high"]) - float(c60[i60]["low"]))
                    body_ok = (body / rng) >= 0.33
                except Exception:
                    body_ok = False
                # ATR percentile band 35–80 over last 120 bars
                if i60 < len(atr60) and i60 >= 1:
                    window = atr60[max(0, i60-119):i60+1]
                    atr_pctile = _pct_rank(window, float(atr60[i60]))
                    atr_ok = 35.0 <= atr_pctile <= 80.0

                # 30s alignment and 15s slope features
                align30 = False
                slope15 = False
                if c30 and len(c30) >= 20:
                    closes30 = [float(x.get("close", 0)) for x in c30]
                    e11_30 = _ema(closes30, 11)
                    e55_30 = _ema(closes30, 55)
                    j30 = _find_idx_leq(c30, ts_cur) if ts_cur else -1
                    if j30 >= 0 and j30 < len(e11_30) and j30 < len(e55_30):
                        align30 = (e11_30[j30] > e55_30[j30]) if direction == "call" else (e11_30[j30] < e55_30[j30])
                if c15 and len(c15) >= 60:
                    closes15 = [float(x.get("close", 0)) for x in c15]
                    e11_15 = _ema(closes15, 11)
                    k15 = _find_idx_leq(c15, ts_cur) if ts_cur else -1
                    if k15 >= 1 and k15 < len(e11_15):
                        slope15 = (e11_15[k15] > e11_15[k15-1]) if direction == "call" else (e11_15[k15] < e11_15[k15-1])

                # Score and threshold (use same weights + min_conf logic as pipeline)
                w = {"align60": 0.30, "align30": 0.20, "slope15": 0.15, "micro": 0.20, "body": 0.10, "atr": 0.05}
                score = 0.0
                if align60: score += w["align60"]
                if align30: score += w["align30"]
                if slope15: score += w["slope15"]
                if microtrend_ok: score += w["micro"]
                if body_ok: score += w["body"]
                if atr_ok: score += w["atr"]
                min_conf_gate = 0.60 if float(min_payout) >= 90.0 else 0.64
                passed = align60 and body_ok and atr_ok and (score >= min_conf_gate)
                if not passed:
                    if debug:
                        print(f"[S12] Gate fail {a}@{tf}s: score={score:.2f} align60={align60} body_ok={body_ok} atr_ok={atr_ok} atr_pct={atr_pctile:.0f}")
                    return None, None
            except Exception as _ge:
                if debug:
                    print(f"[S12] Gate error on {a}@{tf}s: {_ge}")
                return None, None

            if debug:
                print(f"[S12] Signal {direction} on {a} @ {tf}s (tradable={tradable_now})")
            return a, direction
        except asyncio.TimeoutError:
            if debug:
                print(f"[S12] Timeout scanning {a} @ {tf}s")
            return None, None
        except Exception as e:
            if debug:
                print(f"[S12] Error scanning {a} @ {tf}s: {e}")
            return None, None

    # Scan with early return on first signal
    for idx, a in enumerate(assets_pass, start=1):
        for tf in tfs:
            asset, direction = await _scan_one_asset(idx, a, tf)
            if asset:
                return asset, direction
    if debug:
        print("[S12] No signals this scan pass.")
    return None, None

