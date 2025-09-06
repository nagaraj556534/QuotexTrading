from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional
import asyncio
import time

# Lightweight indicator helpers — reuse S12 where possible
try:
    from strategy12.features import ema as _ema  # type: ignore
    from strategy12.features import sma as _sma  # type: ignore
except Exception:
    try:
        from .strategy12.features import ema as _ema  # type: ignore
        from .strategy12.features import sma as _sma  # type: ignore
    except Exception:
        def _ema(series: List[float], period: int) -> List[float]:
            if not series or period <= 1:
                return series[:]
            k = 2 / (period + 1)
            out: List[float] = []
            avg = series[0]
            for v in series:
                avg = v * k + avg * (1 - k)
                out.append(avg)
            return out
        def _sma(series: List[float], period: int) -> List[float]:
            n = len(series)
            if n == 0 or period <= 1:
                return series[:]
            out: List[float] = []
            s = 0.0
            for i, v in enumerate(series):
                s += float(v)
                if i >= period:
                    s -= float(series[i - period])
                if i + 1 >= period:
                    out.append(s / float(period))
                else:
                    out.append(s / float(i + 1))
            return out


def _rsi(closes: List[float], period: int = 14) -> List[float]:
    if not closes:
        return []
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i - 1]
        gains.append(max(0.0, ch))
        losses.append(max(0.0, -ch))
    # Seed with SMA
    out: List[float] = [50.0]
    if len(gains) < period:
        return [50.0] * len(closes)
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    rs = (avg_gain / avg_loss) if avg_loss > 1e-9 else 999.0
    rsi = 100.0 - (100.0 / (1.0 + rs))
    out.append(rsi)
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = (avg_gain / avg_loss) if avg_loss > 1e-9 else 999.0
        rsi = 100.0 - (100.0 / (1.0 + rs))
        out.append(rsi)
    # pad to same length
    while len(out) < len(closes):
        out.insert(0, 50.0)
    return out


def _atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> List[float]:
    if not highs or not lows or not closes:
        return []
    trs: List[float] = []
    prev_close = closes[0]
    for h, l, c in zip(highs, lows, closes):
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
        prev_close = c
    out: List[float] = []
    s = 0.0
    for i, tr in enumerate(trs):
        s += tr
        if i + 1 >= period:
            if i + 1 == period:
                out.append(s / period)
            else:
                out.append((out[-1] * (period - 1) + tr) / period)
    while len(out) < len(closes):
        out.insert(0, out[0] if out else 0.0)
    return out


async def _candles(qx, asset: str, tf: int, bars: int) -> List[Dict[str, Any]]:
    try:
        return await qx.get_candles(asset, time.time(), tf * max(120, bars), tf)
    except Exception:
        return []


def _slope(series: List[float], lookback: int = 5) -> float:
    n = len(series)
    if n < max(2, lookback + 1):
        return 0.0
    a = series[-lookback - 1]
    b = series[-1]
    return (b - a) / max(1e-9, abs(a))


def _pattern_bull_engulf(candles: List[Dict[str, Any]]) -> bool:
    if len(candles) < 2:
        return False
    p, c = candles[-2], candles[-1]
    p_open, p_close = float(p.get("open", 0)), float(p.get("close", 0))
    c_open, c_close = float(c.get("open", 0)), float(c.get("close", 0))
    p_low, p_high = float(p.get("low", 0)), float(p.get("high", 0))
    return (p_close < p_open) and (c_close > c_open) and (c_close >= p_high) and (c_open <= p_low)


def _pattern_bear_engulf(candles: List[Dict[str, Any]]) -> bool:
    if len(candles) < 2:
        return False
    p, c = candles[-2], candles[-1]
    p_open, p_close = float(p.get("open", 0)), float(p.get("close", 0))
    c_open, c_close = float(c.get("open", 0)), float(c.get("close", 0))
    p_low, p_high = float(p.get("low", 0)), float(p.get("high", 0))
    return (p_close > p_open) and (c_close < c_open) and (c_close <= p_low) and (c_open >= p_high)


def _pinbar(c: Dict[str, Any], bias: str) -> bool:
    o = float(c.get("open", 0)); h = float(c.get("high", 0)); l = float(c.get("low", 0)); cl = float(c.get("close", 0))
    body = abs(cl - o); rng = max(1e-9, h - l)
    upper = h - max(cl, o); lower = min(cl, o) - l
    if bias == "bull":
        return lower >= 0.66 * rng and body <= 0.33 * rng
    else:
        return upper >= 0.66 * rng and body <= 0.33 * rng


def _levels(candles: List[Dict[str, Any]], lookback: int = 60) -> Tuple[float, float]:
    if not candles:
        return 0.0, 0.0
    sub = candles[-lookback:]
    highs = [float(x.get("high", 0)) for x in sub]
    lows = [float(x.get("low", 0)) for x in sub]
    return (max(highs) if highs else 0.0, min(lows) if lows else 0.0)


def _corr(a: List[float], b: List[float]) -> float:
    n = min(len(a), len(b))
    if n < 10:
        return 0.0
    a = a[-n:]; b = b[-n:]
    ma = sum(a) / n; mb = sum(b) / n
    cov = sum((x - ma) * (y - mb) for x, y in zip(a, b)) / max(1, n - 1)
    va = sum((x - ma) ** 2 for x in a) / max(1, n - 1)
    vb = sum((y - mb) ** 2 for y in b) / max(1, n - 1)
    import math
    den = math.sqrt(max(1e-12, va * vb))
    return float(cov / den) if den > 0 else 0.0


async def collect_live_features(qx, asset: str) -> Dict[str, Any]:
    tfs = [60, 30, 15, 300]
    need = {60: 150, 30: 120, 15: 180, 300: 100}
    candles_by_tf: Dict[int, List[Dict[str, Any]]] = {}

    # Fetch in sequence with tighter timeouts per TF to avoid analyzer timeouts
    for tf in tfs:
        try:
            tf_timeout = 2.5 if tf in (60, 30) else 1.5
            candles = await asyncio.wait_for(_candles(qx, asset, tf, bars=need[tf]), timeout=tf_timeout)
        except asyncio.TimeoutError:
            candles = []
        candles_by_tf[tf] = candles or []

    out: Dict[str, Any] = {"asset": asset, "tfs": tfs}

    # Build per-TF features
    trend: Dict[int, Dict[str, Any]] = {}
    momentum: Dict[int, Dict[str, Any]] = {}
    vol: Dict[int, Dict[str, Any]] = {}

    def _tf_params(tf: int) -> Tuple[int, int]:
        if tf == 60:
            return 11, 55
        if tf == 30:
            return 7, 21
        if tf == 15:
            return 7, 21
        return 20, 50  # 300s

    for tf, candles in candles_by_tf.items():
        if not candles or len(candles) < 30:
            continue
        closes = [float(x.get("close", 0)) for x in candles]
        highs = [float(x.get("high", 0)) for x in candles]
        lows = [float(x.get("low", 0)) for x in candles]
        ef, es = _tf_params(tf)
        e_fast = _ema(closes, ef)
        e_slow = _ema(closes, es)
        align_up = (e_fast[-1] > e_slow[-1]) if e_fast and e_slow else False
        align_down = (e_fast[-1] < e_slow[-1]) if e_fast and e_slow else False
        slope_fast = _slope(e_fast, lookback=5)
        slope_slow = _slope(e_slow, lookback=5)
        trend[tf] = {
            "ema_fast": e_fast[-1] if e_fast else 0.0,
            "ema_slow": e_slow[-1] if e_slow else 0.0,
            "align_up": align_up,
            "align_down": align_down,
            "slope_fast": slope_fast,
            "slope_slow": slope_slow,
        }
        rsi = _rsi(closes, 14)
        momentum[tf] = {
            "rsi": float(rsi[-1]) if rsi else 50.0,
            "rsi_delta": float((rsi[-1] - rsi[-5]) if len(rsi) >= 5 else 0.0),
        }
        atr = _atr(highs, lows, closes, 14)
        rng = float(atr[-1]) if atr else 0.0
        price = float(closes[-1]) if closes else 1.0
        atr_norm = (rng / max(1e-9, price))
        atr_vals = atr[-60:] if atr else []
        if atr_vals:
            sorted_vals = sorted(atr_vals)
            idx = int(0.85 * (len(sorted_vals) - 1))
            atr_p85 = sorted_vals[idx]
            atr_pctile = sum(1 for v in atr_vals if v <= atr[-1]) / max(1, len(atr_vals))
        else:
            atr_p85 = 0.0
            atr_pctile = 0.5
        vol[tf] = {
            "atr": rng,
            "atr_norm": atr_norm,
            "atr_pctile": float(atr_pctile),
            "atr_p85": float(atr_p85),
        }

    # Patterns on 60s
    patt = {"bull_engulf": False, "bear_engulf": False, "pin_bull": False, "pin_bear": False}
    c60 = candles_by_tf.get(60) or []
    if c60:
        patt["bull_engulf"] = _pattern_bull_engulf(c60)
        patt["bear_engulf"] = _pattern_bear_engulf(c60)
        if len(c60) >= 1:
            last = c60[-1]
            patt["pin_bull"] = _pinbar(last, "bull")
            patt["pin_bear"] = _pinbar(last, "bear")

    # Key levels and distances
    sr_high, sr_low = _levels(c60, lookback=80) if c60 else (0.0, 0.0)
    cur = float(c60[-1].get("close", 0)) if c60 else 0.0
    dist_sup = abs(cur - sr_low) / max(1e-9, cur)
    dist_res = abs(sr_high - cur) / max(1e-9, cur)

    # Liquidity proxy (use 15s + 30s average range / price)
    def _avg_range_norm(candles: List[Dict[str, Any]]) -> float:
        if not candles:
            return 0.0
        rs = [(float(x.get("high", 0)) - float(x.get("low", 0))) for x in candles[-40:]]
        price = float(candles[-1].get("close", 1.0))
        return (sum(rs) / max(1, len(rs))) / max(1e-9, price)
    liq = 0.5 * _avg_range_norm(candles_by_tf.get(15, [])) + 0.5 * _avg_range_norm(candles_by_tf.get(30, []))
    # Normalize liquidity into 0..1 (heuristic):
    # 0.0 for <0.0003, 1.0 for >=0.002
    liq_score = max(0.0, min(1.0, (liq - 0.0003) / max(1e-9, (0.002 - 0.0003))))

    # Cross-asset correlation (optional, fast): limit to EURUSD/GBPUSD only and short timeout
    correlations: Dict[str, float] = {}
    try:
        majors = ["EURUSD", "GBPUSD"]
        a60 = [float(x.get("close", 0)) for x in (candles_by_tf.get(60) or [])]
        a_ret = [a60[i] - a60[i - 1] for i in range(1, len(a60))]
        for m in majors:
            if m == asset:
                continue
            try:
                cm = await asyncio.wait_for(_candles(qx, m, 60, bars=80), timeout=1.2)
                if not cm or len(cm) < 30:
                    continue
                m60 = [float(x.get("close", 0)) for x in cm]
                m_ret = [m60[i] - m60[i - 1] for i in range(1, len(m60))]
                corr = _corr(a_ret, m_ret)
                correlations[m] = float(corr)
            except Exception:
                continue
    except Exception:
        correlations = {}

    # Regime classification
    align_count = sum(1 for tf, t in trend.items() if tf in (60, 30, 15) and (t.get("align_up") or t.get("align_down")))
    atrp = (vol.get(60, {}).get("atr_pctile", 0.5))
    if atrp >= 0.9 and align_count <= 1:
        regime = "volatile"
    elif atrp <= 0.2 and align_count <= 1:
        regime = "calm"
    elif align_count >= 2:
        regime = "trending"
    else:
        regime = "ranging"

    out.update({
        "trend": trend,
        "momentum": momentum,
        "volatility": vol,
        "patterns": patt,
        "levels": {"sr_high": sr_high, "sr_low": sr_low, "dist_sup": dist_sup, "dist_res": dist_res},
        "liquidity_score": float(liq_score),
        "correlations": correlations,
        "regime": regime,
    })
    return out

