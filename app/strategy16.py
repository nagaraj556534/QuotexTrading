from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

# Reuse existing indicator helpers when available (PSAR / Williams %R from Strategy 10)
try:
    from strategy10_confluence import compute_psar, compute_williams_r  # type: ignore
except Exception:  # pragma: no cover
    try:
        from .strategy10_confluence import compute_psar, compute_williams_r  # type: ignore
    except Exception:  # pragma: no cover
        compute_psar = None
        compute_williams_r = None


# ---- Config ----
@dataclass
class S16Config:
    # Core
    timeframes_s: List[int] = None  # analysis TFs
    min_payout: float = 92.0
    allowed_hours_ist: Optional[Set[int]] = None
    expiry_s: int = 60

    # Confluence and scoring
    confluence_mode: str = "3of3"  # "2of3", "3of3", "adaptive"
    min_confidence: float = 0.98
    ema_fast: int = 11
    ema_slow: int = 55
    adx_min: float = 18.0
    rsi_period: int = 14
    wr_len: int = 7
    body_min_ratio: float = 0.38
    atr_band_pct: Tuple[float, float] = (0.45, 0.65)  # ATR% band in bp terms

    # Gates / filters
    psar_align: bool = True
    wr_zone_cross: bool = True
    near_level_min_dist_bp: float = 15.0  # avoid entries too near opposite level

    # Ops
    cooldown_sec: int = 25
    per_asset_limit_sec: int = 2  # timeout per-asset scan

    # Paper trading validation before live
    require_practice_validation: bool = True
    validation_min_trades: int = 30
    validation_min_wr: float = 0.95
    state_path: str = os.path.join(os.path.dirname(__file__), "..", "strategy16_state.json")
    signal_log_path: str = os.path.join(os.path.dirname(__file__), "..", "strategy16_signals.csv")

    def __post_init__(self):
        if self.timeframes_s is None:
            self.timeframes_s = [15, 30, 60]


# ---- Utilities ----
async def _get_assets(qx) -> List[str]:
    names: List[str] = []
    try:
        instruments = await qx.get_instruments()
        for i in instruments:
            try:
                if isinstance(i, dict):
                    nm = i.get("symbol") or i.get("asset") or i.get("name")
                elif isinstance(i, (list, tuple)) and len(i) >= 2:
                    nm = i[1]
                else:
                    nm = str(i)
                if nm:
                    names.append(str(nm))
            except Exception:
                continue
    except Exception:
        pass
    return names


async def _payout_for(qx, asset: str, expiry_min: int) -> float:
    try:
        keys: List[str] = []
        if expiry_min <= 1:
            keys = ["1", "60"]
        elif expiry_min >= 5:
            keys = ["5", "300"] if expiry_min == 5 else [str(expiry_min), str(expiry_min * 60)]
        else:
            keys = [str(expiry_min)]
        for k in keys:
            try:
                val = qx.get_payout_by_asset(asset, timeframe=k)
                if val is not None:
                    return float(val)
            except Exception:
                continue
        try:
            val = qx.get_payout_by_asset(asset)
            if val is not None:
                return float(val)
        except Exception:
            pass
        return 0.0
    except Exception:
        return 0.0


async def _fetch_tf(qx, asset: str, tf: int, bars: int) -> List[dict]:
    try:
        return await qx.get_candles(asset, time.time(), tf * bars, tf)
    except Exception:
        return []


# ---- Lightweight indicators ----
def _ema(values: List[float], period: int) -> List[float]:
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    ema: List[float] = []
    s = sum(values[:period]) / period
    ema.append(s)
    for v in values[period:]:
        s = v * k + s * (1 - k)
        ema.append(s)
    # Align length to values
    pad = len(values) - len(ema)
    if pad > 0:
        ema = [values[0]] * pad + ema
    return ema


def _rsi(values: List[float], period: int) -> List[float]:
    if len(values) < period + 1:
        return []
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(values)):
        ch = values[i] - values[i - 1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    def rma(seq: List[float], p: int) -> List[float]:
        if len(seq) < p:
            return []
        a = sum(seq[:p]) / p
        out = [a]
        alpha = 1.0 / p
        for v in seq[p:]:
            a = (a * (p - 1) + v) / p
            out.append(a)
        return out
    ag = rma(gains, period)
    al = rma(losses, period)
    if not ag or not al:
        return []
    rsi: List[float] = []
    n = min(len(ag), len(al))
    ag, al = ag[-n:], al[-n:]
    for g, l in zip(ag, al):
        rs = g / (l if l != 0 else 1e-9)
        rsi.append(100 - (100 / (1 + rs)))
    return rsi


def _atr_pct(candles: List[dict], period: int = 14) -> List[float]:
    if not candles or len(candles) < period + 1:
        return []
    highs = [float(c.get("high", 0.0)) for c in candles]
    lows = [float(c.get("low", 0.0)) for c in candles]
    closes = [float(c.get("close", 0.0)) for c in candles]
    trs: List[float] = []
    for i in range(1, len(candles)):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    # RMA of TR
    p = period
    if len(trs) < p:
        return []
    a = sum(trs[:p]) / p
    atr: List[float] = [a]
    for v in trs[p:]:
        a = (a * (p - 1) + v) / p
        atr.append(a)
    # Normalize as basis points relative to close
    out: List[float] = []
    base = closes[-len(atr):]
    for tr, c in zip(atr, base):
        out.append(10000.0 * (tr / max(c, 1e-9)))
    return out


def _body_ratio(c: dict) -> float:
    try:
        o = float(c["open"]); cl = float(c["close"]); h = float(c["high"]); l = float(c["low"])
        rng = max(h - l, 1e-9)
        body = abs(cl - o)
        return body / rng
    except Exception:
        return 0.0


def _ist_hour(ts: Optional[int] = None) -> int:
    dt = time.localtime(ts or time.time())
    # Convert local to IST via zoneinfo for accuracy
    try:
        from datetime import datetime
        return int(datetime.now(ZoneInfo("Asia/Kolkata")).hour)
    except Exception:
        return int(dt.tm_hour)


def _levels_distance_bp(candles: List[dict], direction: str, lookback: int = 40) -> float:
    if not candles:
        return 0.0
    n = min(lookback, len(candles))
    win = candles[-n:]
    highs = [float(c.get("high", 0.0)) for c in win]
    lows = [float(c.get("low", 0.0)) for c in win]
    last = float(win[-1].get("close", 0.0))
    near_res = max(highs)
    near_sup = min(lows)
    if direction == "call":
        dist = abs(near_res - last)
    else:
        dist = abs(last - near_sup)
    return 10000.0 * (dist / max(last, 1e-9))


# ---- Validation state ----
def _load_state(path: str) -> Dict[str, Any]:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}


def _save_state(path: str, data: Dict[str, Any]) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass


def s16_is_validated(cfg: S16Config) -> bool:
    if not cfg.require_practice_validation:
        return True
    st = _load_state(cfg.state_path)
    val = st.get("validated", False)
    if val:
        return True
    wins = int(st.get("wins", 0)); total = int(st.get("total", 0))
    acc = (wins / total) if total > 0 else 0.0
    return total >= cfg.validation_min_trades and acc >= cfg.validation_min_wr


# ---- Scoring ----
def _dir_sign(direction: str) -> int:
    return 1 if direction.lower() == "call" else -1


def _decide_direction_by_mtf(closes15: List[float], closes30: List[float], closes60: List[float], cfg: S16Config) -> Tuple[Optional[str], Dict[str, Any]]:
    # EMA alignment on each TF
    out: Dict[str, Any] = {"align15": False, "align30": False, "align60": False, "slope15": 0.0, "slope30": 0.0, "slope60": 0.0}
    ef15 = _ema(closes15, cfg.ema_fast) if closes15 else []
    es15 = _ema(closes15, cfg.ema_slow) if closes15 else []
    ef30 = _ema(closes30, cfg.ema_fast) if closes30 else []
    es30 = _ema(closes30, cfg.ema_slow) if closes30 else []
    ef60 = _ema(closes60, cfg.ema_fast) if closes60 else []
    es60 = _ema(closes60, cfg.ema_slow) if closes60 else []

    def slope_last(ema: List[float]) -> float:
        if not ema or len(ema) < 6:
            return 0.0
        return ema[-1] - ema[-5]

    if ef60 and es60:
        out["align60_up"] = ef60[-1] > es60[-1]
        out["align60_dn"] = ef60[-1] < es60[-1]
        out["slope60"] = slope_last(ef60)
    if ef30 and es30:
        out["align30_up"] = ef30[-1] > es30[-1]
        out["align30_dn"] = ef30[-1] < es30[-1]
        out["slope30"] = slope_last(ef30)
    if ef15 and es15:
        out["align15_up"] = ef15[-1] > es15[-1]
        out["align15_dn"] = ef15[-1] < es15[-1]
        out["slope15"] = slope_last(ef15)

    # Decide base direction from 60s
    if out.get("align60_up"):
        base = "call"
    elif out.get("align60_dn"):
        base = "put"
    else:
        base = None

    return base, out


def _williams_zone_cross(closes: List[float], highs: List[float], lows: List[float], length: int, direction: str) -> bool:
    if not compute_williams_r:
        return True  # skip gate if not available
    try:
        wr = compute_williams_r(highs, lows, closes, length)
        if len(wr) < 2:
            return False
        prev, cur = wr[-2], wr[-1]
        if direction == "call":
            return prev <= -80 and cur > -80  # exit OS zone upward
        else:
            return prev >= -20 and cur < -20  # exit OB zone downward
    except Exception:
        return False


def _psar_agree(highs: List[float], lows: List[float], closes: List[float], direction: str) -> bool:
    if not compute_psar:
        return True  # skip gate if not available
    try:
        psar = compute_psar(highs, lows)
        if not psar:
            return False
        ps_last = psar[-1]
        if direction == "call":
            return ps_last < closes[-1]
        else:
            return ps_last > closes[-1]
    except Exception:
        return False


def _score_for_asset(asset: str, t15: List[dict], t30: List[dict], t60: List[dict], cfg: S16Config, debug: bool = False) -> Tuple[bool, Optional[str], float, Dict[str, Any]]:
    # Prepare series
    c15 = [float(c.get("close", 0.0)) for c in t15] if t15 else []
    c30 = [float(c.get("close", 0.0)) for c in t30] if t30 else []
    c60 = [float(c.get("close", 0.0)) for c in t60] if t60 else []
    h60 = [float(c.get("high", 0.0)) for c in t60] if t60 else []
    l60 = [float(c.get("low", 0.0)) for c in t60] if t60 else []

    # Decide base direction by 60s EMA alignment
    direction, feats = _decide_direction_by_mtf(c15, c30, c60, cfg)
    if not direction:
        return False, None, 0.0, {"reason": "no_base_alignment"}

    # Gates
    atrp = _atr_pct(t60, 14)
    atr_ok = False
    if atrp:
        lo, hi = cfg.atr_band_pct
        atr_ok = (atrp[-1] >= lo) and (atrp[-1] <= hi)
    body_ok = False
    if t60 and len(t60) >= 1:
        body_ok = _body_ratio(t60[-1]) >= cfg.body_min_ratio

    psar_ok = _psar_agree(h60, l60, c60, direction) if cfg.psar_align else True

    wr_ok = True
    if cfg.wr_zone_cross and t30 and len(t30) >= 14:
        h30 = [float(c.get("high", 0.0)) for c in t30]
        l30 = [float(c.get("low", 0.0)) for c in t30]
        wr_ok = _williams_zone_cross([float(x) for x in c30], h30, l30, cfg.wr_len, direction)

    dist_ok = True
    if t60 and len(t60) >= 20:
        dist_bp = _levels_distance_bp(t60, direction, 40)
        dist_ok = dist_bp >= cfg.near_level_min_dist_bp

    # Confluence across TFs
    align60 = feats.get("align60_up") if direction == "call" else feats.get("align60_dn")
    align30 = feats.get("align30_up") if direction == "call" else feats.get("align30_dn")
    align15 = feats.get("align15_up") if direction == "call" else feats.get("align15_dn")
    agree = sum(1 for x in (align15, align30, align60) if x)

    # Confidence score (0..1)
    w = {
        "align60": 0.40,
        "align30": 0.20,
        "align15": 0.15,
        "body": 0.10,
        "atr": 0.07,
        "psar": 0.04,
        "wr": 0.04,
    }
    score = 0.0
    if align60: score += w["align60"]
    if align30: score += w["align30"]
    if align15: score += w["align15"]
    if body_ok: score += w["body"]
    if atr_ok: score += w["atr"]
    if psar_ok: score += w["psar"]
    if wr_ok: score += w["wr"]

    # Hard gates for ultra-selective behavior
    must_haves = bool(align60 and body_ok and atr_ok and psar_ok and wr_ok and dist_ok)

    # Confluence requirement
    conf_req = cfg.confluence_mode
    if conf_req == "3of3":
        conf_ok = agree >= 3
    elif conf_req == "2of3":
        conf_ok = agree >= 2
    else:  # adaptive
        conf_ok = (agree >= 3) or (agree >= 2 and score >= max(0.96, cfg.min_confidence))

    ok = must_haves and conf_ok and (score >= cfg.min_confidence)
    feats.update({
        "asset": asset,
        "direction": direction,
        "score": score,
        "agree": agree,
        "body_ok": body_ok,
        "atr_ok": atr_ok,
        "psar_ok": psar_ok,
        "wr_ok": wr_ok,
        "dist_ok": dist_ok,
    })
    return ok, direction if ok else None, score, feats


def _log_signal(cfg: S16Config, feats: Dict[str, Any]) -> None:
    try:
        p = cfg.signal_log_path
        new = not os.path.exists(p)
        with open(p, "a", encoding="utf-8") as f:
            if new:
                f.write("ts,asset,dir,score,agree,body_ok,atr_ok,psar_ok,wr_ok,dist_ok\n")
            ts = int(time.time())
            f.write(f"{ts},{feats.get('asset')},{feats.get('direction')},{feats.get('score'):.4f},{feats.get('agree')},{int(feats.get('body_ok'))},{int(feats.get('atr_ok'))},{int(feats.get('psar_ok'))},{int(feats.get('wr_ok'))},{int(feats.get('dist_ok'))}\n")
    except Exception:
        pass


# ---- Public API ----
async def find_first_signal_s16(
    qx,
    cfg: S16Config,
    min_payout: float,
    debug: bool = False,
) -> Tuple[Optional[str], Optional[str]]:
    """Scan assets and return the first (asset, direction) passing ultra-selective gates.
    - Honors IST hours and payout filters
    - Multi-timeframe 15s/30s/60s confluence
    - Paper trading validation gate: if not validated and LIVE, returns (None, None)
    """
    try:
        # Practice validation gate
        try:
            mode = qx.get_account_mode() if hasattr(qx, "get_account_mode") else "PRACTICE"
        except Exception:
            mode = "PRACTICE"
        if mode != "PRACTICE" and cfg.require_practice_validation and not s16_is_validated(cfg):
            if debug:
                print("[S16] LIVE mode blocked until PRACTICE validation passes.")
            return None, None

        assets = await _get_assets(qx)
        if not assets:
            return None, None

        # Sort to prefer higher payouts first
        expiry_min = int(max(1, cfg.expiry_s // 60))
        pay_map: Dict[str, float] = {}
        for a in assets:
            p = await _payout_for(qx, a, expiry_min)
            if p >= max(min_payout, cfg.min_payout):
                pay_map[a] = p
        if not pay_map:
            if debug:
                print("[S16] No assets meet payout filter.")
            return None, None
        sorted_assets = sorted(pay_map.keys(), key=lambda x: pay_map[x], reverse=True)

        # Hour filter (IST)
        ist_allowed = cfg.allowed_hours_ist
        cur_h = _ist_hour()
        if ist_allowed is not None and cur_h not in ist_allowed:
            if debug:
                print(f"[S16] IST hour {cur_h} not in allowed hours -> skip scan.")
            return None, None

        # Cooldown handling via in-memory global map
        now = time.time()
        cool = globals().setdefault("S16_COOLDOWNS", {})  # type: ignore

        async def _scan_one(a: str) -> Tuple[Optional[str], Optional[str]]:
            # Cooldown
            until = cool.get(a, 0.0)
            if until > now:
                return None, None
            # Tradability quick check (best-effort)
            try:
                tradable = await qx.is_tradable(a) if hasattr(qx, "is_tradable") else True
                if not tradable:
                    return None, None
            except Exception:
                pass
            # Fetch data with timeout
            try:
                async def _fetch_all():
                    # Fetch more bars on faster TFs
                    t60 = await _fetch_tf(qx, a, 60, 120)
                    t30 = await _fetch_tf(qx, a, 30, 160)
                    t15 = await _fetch_tf(qx, a, 15, 220)
                    return t15, t30, t60
                t15, t30, t60 = await asyncio.wait_for(_fetch_all(), timeout=cfg.per_asset_limit_sec)
                ok, direction, score, feats = _score_for_asset(a, t15, t30, t60, cfg, debug)
                if ok and direction:
                    if debug:
                        print(f"[S16] {a} {direction} score={score:.3f} payout={pay_map.get(a, 0):.0f}%")
                    _log_signal(cfg, feats)
                    # Apply cooldown on success
                    cool[a] = time.time() + cfg.cooldown_sec
                    return a, direction
            except asyncio.TimeoutError:
                if debug:
                    print(f"[S16] Timeout scanning {a}")
            except Exception as e:
                if debug:
                    print(f"[S16] Error on {a}: {e}")
            return None, None

        # Sequential scan prioritizing high payout assets
        for a in sorted_assets:
            asset, direction = await _scan_one(a)
            if asset:
                return asset, direction

        if debug:
            print("[S16] No signals this pass.")
        return None, None

    except Exception:
        return None, None


__all__ = [
    "S16Config",
    "find_first_signal_s16",
    "s16_is_validated",
]

