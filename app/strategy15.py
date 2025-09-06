from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Tuple, List, Dict
import os
import csv
import time
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

# --- Module-level state for schedule execution dedup ---
S15_EXECUTED: set[str] = set()
S15_LAST_TF: Optional[int] = None
S15_LAST_MODE: Optional[str] = None  # 'scheduled' | 'live'

# --- Logging paths ---
S15_SIGNAL_LOG = os.path.join(os.path.dirname(__file__), "..", "strategy15_signals.csv")


def _ensure_s15_log_header() -> None:
    try:
        if not os.path.exists(S15_SIGNAL_LOG):
            with open(S15_SIGNAL_LOG, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    "ts_local", "mode", "asset", "direction", "note",
                    "schedule_ts", "schedule_str", "payout", "tf_s"
                ])
    except Exception:
        pass

S15_EXEC_LOG = os.path.join(os.path.dirname(__file__), "..", "strategy15_exec.csv")



# --- Timing memory (preferred entry second per asset) ---
S15_TIMING_MEM = os.path.join(os.path.dirname(__file__), "..", "strategy15_timing_mem.json")
S15_POSTEVAL_LOG = os.path.join(os.path.dirname(__file__), "..", "strategy15_posteval.csv")


def _timing_mem_load() -> dict:
    try:
        if os.path.exists(S15_TIMING_MEM):
            import json
            with open(S15_TIMING_MEM, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return {}
    return {}


def _timing_mem_save(mem: dict) -> None:
    try:
        import json
        with open(S15_TIMING_MEM, "w", encoding="utf-8") as f:
            json.dump(mem, f)
    except Exception:
        pass


def update_timing_memory(asset: str, sec_in_minute: int, result: str) -> None:
    mem = _timing_mem_load()
    a = mem.get(asset) or {}
    s = str(int(sec_in_minute))
    slot = a.get(s) or {"wins": 0, "loss": 0, "draw": 0, "total": 0}
    slot["total"] = int(slot.get("total", 0)) + 1
    r = result.upper()
    if r == "WIN":
        slot["wins"] = int(slot.get("wins", 0)) + 1
    elif r == "LOSS":
        slot["loss"] = int(slot.get("loss", 0)) + 1
    elif r == "DRAW":
        slot["draw"] = int(slot.get("draw", 0)) + 1
    a[s] = slot
    mem[asset] = a
    _timing_mem_save(mem)


def get_preferred_second(asset: str, min_samples: int = 3) -> Optional[int]:
    mem = _timing_mem_load()
    a = mem.get(asset)
    if not a:
        return None
    best_sec = None
    best_score = -1e9
    for s, slot in a.items():
        tot = int(slot.get("total", 0))
        if tot < min_samples:
            continue
        wins = int(slot.get("wins", 0))
        loss = int(slot.get("loss", 0))
        draw = int(slot.get("draw", 0))
        score = wins - loss + 0.25 * draw
        if score > best_score:
            best_score = score
            best_sec = int(s)
    return best_sec


def _ensure_posteval_header() -> None:
    try:
        if not os.path.exists(S15_POSTEVAL_LOG):
            with open(S15_POSTEVAL_LOG, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    "entry_ts", "asset", "dir", "eval_tf_s", "bars",
                    "in_favor", "against", "net_points"
                ])
    except Exception:
        pass


async def post_trade_quick_eval(qx, asset: str, entry_ts: float, direction: str,
                                eval_tf_s: int = 15, bars: int = 2) -> dict:
    """After trade closes, check next N candles direction bias quickly."""
    try:
        _ensure_posteval_header()
        # Fetch a bit after entry time
        candles = await qx.get_candles(asset, entry_ts + 1, eval_tf_s * (bars + 5), eval_tf_s)
    except Exception:
        candles = []
    if not candles:
        return {"in_favor": 0, "against": 0, "net_points": 0.0}
    in_favor = 0
    against = 0
    net = 0.0
    sign = 1 if direction.lower() == "call" else -1
    for c in candles[:bars]:
        body = float(c["close"]) - float(c["open"])  # positive is green
        if sign * body > 0:
            in_favor += 1
        elif sign * body < 0:
            against += 1
        net += sign * body
    try:
        with open(S15_POSTEVAL_LOG, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([int(entry_ts), asset, direction, eval_tf_s, bars, in_favor, against, net])
    except Exception:
        pass
    return {"in_favor": in_favor, "against": against, "net_points": net}

# --- Asset cooldown and loss history ---
S15_COOLDOWNS: dict[str, float] = {}
S15_LOSS_HIST: dict[str, list[float]] = {}


def s15_register_loss(asset: str, when_ts: float | None = None) -> None:
    now = float(when_ts) if when_ts is not None else time.time()
    try:
        hist = S15_LOSS_HIST.get(asset, [])
        # keep only last 10 minutes
        horizon = now - 600.0
        hist = [t for t in hist if t >= horizon]
        hist.append(now)
        S15_LOSS_HIST[asset] = hist
        # base cooldown 90s; if >=2 recent losses, extend to 12 minutes
        cd = 90.0 if len(hist) < 2 else 12 * 60.0
        S15_COOLDOWNS[asset] = now + cd
    except Exception:
        pass


def s15_is_on_cooldown(asset: str, now_ts: float | None = None) -> bool:
    try:
        now = float(now_ts) if now_ts is not None else time.time()
        until = float(S15_COOLDOWNS.get(asset, 0.0))
        return until > now
    except Exception:
        return False


# --- Helpers for stricter 60s candle quality ---
def _median(vals: list[float]) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return float((s[mid - 1] + s[mid]) / 2)


def _quality_60_ok(c60: list[dict], direction: str) -> bool:
    try:
        if not c60 or len(c60) < 6:
            return False
        last = c60[-1]
        prev = c60[-6:-1]
        o = float(last["open"]); h = float(last["high"]); l = float(last["low"]); c = float(last["close"])
        body = abs(c - o)
        rng = max(1e-9, h - l)
        body_ratio = body / rng
        if body_ratio < 0.33:
            return False
        up_wick = h - max(o, c)
        dn_wick = min(o, c) - l
        wick_ratio_up = up_wick / max(body, 1e-9)
        wick_ratio_dn = dn_wick / max(body, 1e-9)
        if direction.lower() == "call" and wick_ratio_up > 1.5:
            return False
        if direction.lower() == "put" and wick_ratio_dn > 1.5:
            return False
        prev_ranges = [float(p["high"]) - float(p["low"]) for p in prev]
        med_rng = _median(prev_ranges)
        if rng < 0.7 * med_rng:
            return False
        return True
    except Exception:
        return False

def _ensure_s15_exec_header() -> None:
    try:
        if not os.path.exists(S15_EXEC_LOG):
            with open(S15_EXEC_LOG, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    "entry_ts", "entry_ist", "asset", "direction", "amount",
                    "result", "delta", "expiry_s", "sec_in_minute", "tf_hint", "payout", "mode"
                ])
    except Exception:
        pass

# --- Comprehensive Market Trend Analysis ---
from enum import Enum
from dataclasses import dataclass


class TrendType(Enum):
    UPTREND = "uptrend"
    DOWNTREND = "downtrend"
    SIDEWAYS = "sideways"
    UNKNOWN = "unknown"


@dataclass
class TrendAnalysis:
    trend_type: TrendType
    strength: float  # 0.0 to 1.0
    confidence: float  # 0.0 to 1.0
    support_level: Optional[float] = None
    resistance_level: Optional[float] = None
    breakout_direction: Optional[str] = None  # "up" | "down" | None


def _calculate_swing_points(highs: List[float], lows: List[float], window: int = 5) -> Tuple[List[int], List[int]]:
    """Find swing highs and lows indices."""
    swing_highs: List[int] = []
    swing_lows: List[int] = []
    for i in range(window, len(highs) - window):
        if all(highs[i] >= highs[j] for j in range(i - window, i + window + 1) if j != i):
            swing_highs.append(i)
        if all(lows[i] <= lows[j] for j in range(i - window, i + window + 1) if j != i):
            swing_lows.append(i)
    return swing_highs, swing_lows


def _detect_trend_structure(highs: List[float], lows: List[float], closes: List[float]) -> TrendType:
    if len(closes) < 20:
        return TrendType.UNKNOWN
    swing_highs, swing_lows = _calculate_swing_points(highs, lows)
    if len(swing_highs) < 3 or len(swing_lows) < 3:
        return TrendType.UNKNOWN
    recent_highs = swing_highs[-3:]
    recent_lows = swing_lows[-3:]
    higher_highs = all(highs[recent_highs[i]] > highs[recent_highs[i-1]] for i in range(1, len(recent_highs)))
    higher_lows = all(lows[recent_lows[i]] > lows[recent_lows[i-1]] for i in range(1, len(recent_lows)))
    lower_highs = all(highs[recent_highs[i]] < highs[recent_highs[i-1]] for i in range(1, len(recent_highs)))
    lower_lows = all(lows[recent_lows[i]] < lows[recent_lows[i-1]] for i in range(1, len(recent_lows)))
    if higher_highs and higher_lows:
        return TrendType.UPTREND
    if lower_highs and lower_lows:
        return TrendType.DOWNTREND
    return TrendType.SIDEWAYS


def _calculate_trend_strength(closes: List[float], ema_fast: List[float], ema_slow: List[float], adx_vals: List[float]) -> float:
    if not all([closes, ema_fast, ema_slow, adx_vals]) or len(closes) < 10:
        return 0.0
    ema_sep = abs(ema_fast[-1] - ema_slow[-1]) / max(abs(ema_slow[-1]), 1e-9)
    ema_strength = min(ema_sep * 100, 1.0)
    adx_strength = min(adx_vals[-1] / 50.0, 1.0) if adx_vals else 0.0
    price_change = abs(closes[-1] - closes[-10]) / max(abs(closes[-10]), 1e-9)
    momentum_strength = min(price_change * 50, 1.0)
    return (ema_strength * 0.4 + adx_strength * 0.4 + momentum_strength * 0.2)


def _find_support_resistance(highs: List[float], lows: List[float], closes: List[float]) -> Tuple[Optional[float], Optional[float]]:
    # Backward compatibility (simple SR)
    if len(closes) < 20:
        return None, None
    swing_highs, swing_lows = _calculate_swing_points(highs, lows)
    if not swing_highs or not swing_lows:
        return None, None
    recent_high_levels = [highs[i] for i in swing_highs[-5:]]
    recent_low_levels = [lows[i] for i in swing_lows[-5:]]
    current_price = closes[-1]
    resistance_candidates = [h for h in recent_high_levels if h > current_price]
    support_candidates = [l for l in recent_low_levels if l < current_price]
    resistance = min(resistance_candidates) if resistance_candidates else None
    support = max(support_candidates) if support_candidates else None
    return support, resistance


def _get_vol(c: dict) -> float:
    try:
        for k in ("volume", "vol", "Volume"):
            if k in c:
                return float(c[k])
    except Exception:
        pass
    return 1.0


def _find_support_resistance_enhanced(c60: List[dict], c30: List[dict], c15: List[dict]) -> Tuple[Optional[float], Optional[float]]:
    """Multi-TF SR with strength scoring; prefers levels present across TFs and with more touches/volume."""
    try:
        if not c60 or len(c60) < 30:
            return None, None
        def _levels(candles: List[dict], win: int = 5) -> Tuple[List[float], List[float]]:
            if not candles or len(candles) < (2*win + 3):
                return [], []
            highs = [float(x["high"]) for x in candles]
            lows = [float(x["low"]) for x in candles]
            sh, sl = _calculate_swing_points(highs, lows, window=win)
            return [highs[i] for i in sh], [lows[i] for i in sl]
        h60, l60 = _levels(c60, 5)
        h30, l30 = _levels(c30, 5) if c30 else ([], [])
        h15, l15 = _levels(c15, 4) if c15 else ([], [])
        current = float(c60[-1]["close"])
        # Cluster levels within 0.05% tolerance
        eps = 0.0005
        def _cluster(levels: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
            # levels: list of (price, strength)
            levels = sorted(levels, key=lambda x: x[0])
            clusters: List[List[Tuple[float, float]]] = []
            for p, s in levels:
                if not clusters or abs(p - clusters[-1][-1][0]) / max(1e-9, p) > eps:
                    clusters.append([(p, s)])
                else:
                    clusters[-1].append((p, s))
            out: List[Tuple[float, float]] = []
            for cl in clusters:
                price = sum(p for p, _ in cl) / len(cl)
                strength = sum(s for _, s in cl)
                out.append((price, strength))
            return out
        # Build candidate levels with TF weights and volume around levels
        def _score_levels(candles: List[dict], hi_levels: List[float], lo_levels: List[float], w: float) -> List[Tuple[float, float]]:
            res: List[Tuple[float, float]] = []
            if not candles:
                return res
            closes = [float(x["close"]) for x in candles]
            vols = [_get_vol(x) for x in candles]
            for L in hi_levels + lo_levels:
                touches = sum(1 for close in closes if abs(close - L) / max(1e-9, L) <= eps)
                v_sum = sum(vols[i] for i, close in enumerate(closes) if abs(close - L) / max(1e-9, L) <= eps)
                res.append((L, w * (touches + 0.1 * v_sum)))
            return res
        cand60 = c60[-200:]
        cand30 = c30[-200:] if c30 else []
        cand15 = c15[-200:] if c15 else []
        candidates: List[Tuple[float, float]] = []
        candidates += _score_levels(cand60, h60, l60, 1.0)
        candidates += _score_levels(cand30, h30, l30, 0.7)
        candidates += _score_levels(cand15, h15, l15, 0.5)
        clusters = _cluster(candidates)
        below = [(p, s) for p, s in clusters if p < current]
        above = [(p, s) for p, s in clusters if p > current]
        support = max(below, key=lambda x: x[1])[0] if below else None
        resistance = min(above, key=lambda x: x[1])[0] if above else None
        return support, resistance
    except Exception:
        return None, None


async def _analyze_market_trend(qx, asset: str, tf: int, cfg: S15LiveConfig) -> TrendAnalysis:
    try:
        candles = await qx.get_candles(asset, time.time(), tf * 200, tf)
        if not candles or len(candles) < 50:
            return TrendAnalysis(TrendType.UNKNOWN, 0.0, 0.0)
        closes = [float(c["close"]) for c in candles]
        highs = [float(c["high"]) for c in candles]
        lows = [float(c["low"]) for c in candles]
        ema_fast = _ema(closes, cfg.ema_fast)
        ema_slow = _ema(closes, cfg.ema_slow)
        adx_vals = _adx(highs, lows, closes, cfg.adx_period)
        if not all([ema_fast, ema_slow, adx_vals]):
            return TrendAnalysis(TrendType.UNKNOWN, 0.0, 0.0)
        trend_type = _detect_trend_structure(highs, lows, closes)
        strength = _calculate_trend_strength(closes, ema_fast, ema_slow, adx_vals)
        # Enhanced multi-TF SR detection; fallback to simple SR if needed
        try:
            c60 = await qx.get_candles(asset, time.time(), 60 * 240, 60)
            c30 = await qx.get_candles(asset, time.time(), 30 * 240, 30)
            c15 = await qx.get_candles(asset, time.time(), 15 * 240, 15)
        except Exception:
            c60 = c30 = c15 = []
        s_enh, r_enh = _find_support_resistance_enhanced(c60 or [], c30 or [], c15 or [])
        if s_enh is not None or r_enh is not None:
            support, resistance = s_enh, r_enh
        else:
            support, resistance = _find_support_resistance(highs, lows, closes)
        confidence = 0.0
        if len(ema_fast) >= 5 and len(ema_slow) >= 5:
            if trend_type == TrendType.UPTREND and ema_fast[-1] > ema_slow[-1]:
                confidence += 0.3
            elif trend_type == TrendType.DOWNTREND and ema_fast[-1] < ema_slow[-1]:
                confidence += 0.3
        if adx_vals and adx_vals[-1] > 25:
            confidence += 0.3
        if trend_type in [TrendType.UPTREND, TrendType.DOWNTREND]:
            confidence += 0.2
        elif trend_type == TrendType.SIDEWAYS and support and resistance:
            confidence += 0.15
        if len(closes) >= 10:
            recent_momentum = closes[-1] - closes[-10]
            if trend_type == TrendType.UPTREND and recent_momentum > 0:
                confidence += 0.2
            elif trend_type == TrendType.DOWNTREND and recent_momentum < 0:
                confidence += 0.2
        confidence = min(confidence, 1.0)
        breakout_direction = None
        if trend_type == TrendType.SIDEWAYS and support and resistance:
            price = closes[-1]
            if price > (resistance * 0.999):
                breakout_direction = "up"
            elif price < (support * 1.001):
                breakout_direction = "down"
        return TrendAnalysis(
            trend_type=trend_type,
            strength=strength,
            confidence=confidence,
            support_level=support,
            resistance_level=resistance,
            breakout_direction=breakout_direction,
        )
    except Exception:
        return TrendAnalysis(TrendType.UNKNOWN, 0.0, 0.0)

    try:
        if not os.path.exists(S15_EXEC_LOG):
            with open(S15_EXEC_LOG, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    "entry_ts", "entry_ist", "asset", "direction", "amount",
                    "result", "delta", "expiry_s", "sec_in_minute", "tf_hint", "payout", "mode"
                ])
    except Exception:
        pass


def log_s15_execution(entry_ts: float, asset: str, direction: str, amount: float,
                       result: str, delta: float, expiry_s: int,
                       sec_in_minute: int, tf_hint: int | None = None,
                       payout: float | None = None, mode: str | None = None) -> None:
    try:
        _ensure_s15_exec_header()
        ist = ""
        try:
            tz = ZoneInfo("Asia/Kolkata") if ZoneInfo else timezone.utc
            ist = datetime.fromtimestamp(entry_ts, tz).strftime("%Y-%m-%d %H:%M:%S %Z")
        except Exception:
            ist = ""
        with open(S15_EXEC_LOG, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                int(entry_ts), ist, asset, direction, float(amount), result,
                float(delta), int(expiry_s), int(sec_in_minute), tf_hint or "",
                payout if payout is not None else "", mode or ""
            ])
    except Exception:
        pass

        pass


def _log_s15_signal(mode: str, asset: str, direction: str, note: str = "",
                     schedule_ts: Optional[int] = None,
                     payout: Optional[float] = None,
                     tf_s: Optional[int] = None) -> None:
    try:
        _ensure_s15_log_header()
        sch_str = (
            datetime.fromtimestamp(int(schedule_ts), tz=timezone.utc).astimezone(
                ZoneInfo("Asia/Kolkata") if ZoneInfo else timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S %Z")
            if schedule_ts else ""
        )
        with open(S15_SIGNAL_LOG, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                datetime.now().isoformat(timespec="seconds"), mode, asset, direction,
                note, schedule_ts or "", sch_str, payout if payout is not None else "",
                tf_s if tf_s is not None else "",
            ])
    except Exception:
        pass


# ---- Schedule (trades.txt) helpers ----
@dataclass
class S15Schedule:
    when_ts: int  # epoch seconds for intended entry start (aligned to minute)
    asset: str
    direction: str  # "call" or "put"
    raw: str = ""


@dataclass
class S15LiveConfig:
    timeframes_s: List[int] = field(default_factory=lambda: [15, 30, 60])
    ema_fast: int = 10
    ema_slow: int = 40
    rsi_period: int = 14
    rsi_buy_min: float = 40.0   # widened for more entries
    rsi_sell_max: float = 60.0   # widened for more entries
    adx_period: int = 14
    adx_min: float = 15.0
    min_body_ratio: float = 0.25


@dataclass
class S15Config:
    mode: str = "hybrid"  # "scheduled" | "live" | "hybrid"
    trades_file: Optional[str] = None
    schedule_tz: str = "IST"  # "IST" | "LOCAL" | "UTC"
    match_window_sec: int = 20
    allowed_hours_ist: Optional[set[int]] = None
    live: S15LiveConfig = field(default_factory=S15LiveConfig)


# --- Small helpers (indicators) ---
def _ema(values: List[float], period: int) -> List[float]:
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    out = [sum(values[:period]) / period]
    for v in values[period:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out


def _rma(values: List[float], period: int) -> List[float]:
    if len(values) < period:
        return []
    out = [sum(values[:period]) / period]
    for v in values[period:]:
        out.append((out[-1] * (period - 1) + v) / period)
    return out


def _adx(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> List[float]:
    if len(highs) < period + 1:
        return []
    plus_dm = [0.0]
    minus_dm = [0.0]
    trs = [0.0]
    for i in range(1, len(highs)):
        up_move = highs[i] - highs[i - 1]
        down_move = lows[i - 1] - lows[i]
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)
        trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
    atr = _rma(trs[1:], period)
    if len(atr) < period:
        return []
    sm_plus = _rma(plus_dm[1:], period)
    sm_minus = _rma(minus_dm[1:], period)
    dx: List[float] = []
    for i in range(len(atr)):
        denom = atr[i] if atr[i] != 0 else 1e-9
        plus_di = 100 * (sm_plus[i] / denom)
        minus_di = 100 * (sm_minus[i] / denom)
        s = plus_di + minus_di if (plus_di + minus_di) != 0 else 1e-9
        dx.append(100 * abs(plus_di - minus_di) / s)
    return _rma(dx, period)


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

# --- Multi-timeframe direction validation ---
async def _fetch_tf(qx, asset: str, tf: int, bars: int) -> List[dict]:
    try:
        return await qx.get_candles(asset, time.time(), tf * bars, tf)
    except Exception:
        return []


def _dir_sign(direction: str) -> int:
    return 1 if direction.lower() == "call" else -1


def _candle_bodies(candles: List[dict]) -> List[float]:
    out: List[float] = []
    for c in candles:
        try:
            out.append(float(c["close"]) - float(c["open"]))
        except Exception:
            out.append(0.0)
    return out


def _ema_align_and_slope(closes: List[float], cfg: S15LiveConfig, direction: str) -> tuple[bool, bool]:
    ef = _ema(closes, cfg.ema_fast); es = _ema(closes, cfg.ema_slow)
    if not ef or not es:
        return False, False
    align = (ef[-1] > es[-1]) if direction == "call" else (ef[-1] < es[-1])
    slope = 0.0
    if len(ef) >= 6:
        slope = (ef[-1] - ef[-5]) if direction == "call" else (ef[-5] - ef[-1])
    slope_ok = slope > 0
    return align, slope_ok


def _momentum_ok(closes: List[float], k: int, direction: str, vols: Optional[List[float]] = None) -> tuple[bool, float]:
    if len(closes) < k + 1:
        return False, 0.0
    raw = closes[-1] - closes[-k]
    s = _dir_sign(direction)
    m = s * raw
    # volume-weighted tweak (optional)
    if vols and len(vols) >= k + 1:
        recent = sum(vols[-k:]) / k
        prev = sum(vols[-2*k:-k]) / k if len(vols) >= 2*k else recent
        boost = min(max((recent - prev) / max(prev, 1e-9), -0.5), 0.5)
        m = m * (1.0 + 0.2 * boost)
    return (m > 0), m


def _candle_bias_ok(candles: List[dict], direction: str, n: int = 5) -> bool:
    if not candles:
        return False
    bodies = _candle_bodies(candles[-n:])
    s = _dir_sign(direction)
    wins = sum(1 for b in bodies if s * b > 0)
    return wins >= max(3, n - 2)


async def _validate_direction_multitf(qx, asset: str, desired_dir: str, cfg: S15LiveConfig, debug: bool = False) -> tuple[bool, float, str]:
    """Validate that 60s/30s/15s align for desired_dir; returns (ok, score, info)."""
    tf60 = await _fetch_tf(qx, asset, 60, 120)
    tf30 = await _fetch_tf(qx, asset, 30, 160)
    tf15 = await _fetch_tf(qx, asset, 15, 200)
    if not tf60 or len(tf60) < 30:
        return False, 0.0, "no60"
    closes60 = [float(c["close"]) for c in tf60]
    closes30 = [float(c["close"]) for c in tf30] if tf30 else []
    closes15 = [float(c["close"]) for c in tf15] if tf15 else []
    vols30 = [_get_vol(c) for c in tf30] if tf30 else None
    vols15 = [_get_vol(c) for c in tf15] if tf15 else None

    # 60s: trend and EMA alignment required
    t60 = await _analyze_market_trend(qx, asset, 60, cfg)
    trend_match = (t60.trend_type.name.lower() == ("uptrend" if desired_dir == "call" else "downtrend"))
    align60, slope60 = _ema_align_and_slope(closes60, cfg, desired_dir)
    ok60_mom, mom60 = _momentum_ok(closes60, 10, desired_dir)

    if debug:
        print(f"[S15] DirCheck60 {asset} dir={desired_dir} trend_match={trend_match} s={t60.strength:.2f} c={t60.confidence:.2f} align={align60} slope={slope60} mom={mom60:.5f}")

    if (not trend_match) or (t60.strength < 0.45) or (t60.confidence < 0.55) or (not align60) or (not slope60) or (not ok60_mom):
        return False, 0.0, "60fail"

    # 30s: momentum + EMA slope
    ok30_mom, mom30 = _momentum_ok(closes30, 10, desired_dir, vols=vols30) if closes30 else (False, 0.0)
    align30, slope30 = _ema_align_and_slope(closes30, cfg, desired_dir) if closes30 else (False, False)

    # 15s: immediate momentum + candle bias last 5
    ok15_mom, mom15 = _momentum_ok(closes15, 5, desired_dir, vols=vols15) if closes15 else (False, 0.0)
    bias15 = _candle_bias_ok(tf15, desired_dir, n=5) if tf15 else False

    # Score aggregation
    score = 0.0
    score += 0.35  # 60s passed hard gate
    score += 0.25 if ok30_mom else 0.0
    score += 0.10 if align30 else 0.0
    score += 0.15 if ok15_mom else 0.0
    score += 0.15 if bias15 else 0.0

    if debug:
        print(f"[S15] DirCheck30 mom={mom30:.5f} ok={ok30_mom} align={align30} slope={slope30}")
        print(f"[S15] DirCheck15 mom={mom15:.5f} ok={ok15_mom} bias5={bias15}")

    ok = (score >= 0.60) and ok30_mom and ok15_mom and bias15
    return ok, score, "ok"

# --- Advanced Signal Quality Scoring ---
async def _calculate_signal_quality_score(qx, asset: str, direction: str, cfg: S15LiveConfig) -> float:
    """Calculate comprehensive signal quality score (0.0 to 1.0) for trade confidence."""
    try:
        score = 0.0

        # Multi-timeframe momentum alignment (40% weight)
        tf_scores = []
        for tf in [15, 30, 60]:
            try:
                candles = await qx.get_candles(asset, time.time(), tf * 100, tf)
                if candles and len(candles) >= 20:
                    closes = [float(c["close"]) for c in candles]
                    # Short, medium, long momentum
                    mom_short = closes[-1] - closes[-5]
                    mom_med = closes[-1] - closes[-10]
                    mom_long = closes[-1] - closes[-20]

                    sign = 1 if direction == "call" else -1
                    tf_score = 0.0
                    if sign * mom_short > 0: tf_score += 0.4
                    if sign * mom_med > 0: tf_score += 0.3
                    if sign * mom_long > 0: tf_score += 0.3
                    tf_scores.append(tf_score)
            except Exception:
                tf_scores.append(0.0)

        if tf_scores:
            score += 0.4 * (sum(tf_scores) / len(tf_scores))

        # Volume trend confirmation (20% weight)
        try:
            c60 = await qx.get_candles(asset, time.time(), 60 * 50, 60)
            if c60 and len(c60) >= 10:
                vols = [_get_vol(c) for c in c60]
                recent_vol = sum(vols[-5:]) / 5
                prev_vol = sum(vols[-10:-5]) / 5
                vol_trend = min(max((recent_vol - prev_vol) / max(prev_vol, 1), -1), 1)
                score += 0.2 * (0.5 + 0.5 * vol_trend)
        except Exception:
            pass

        # Price action quality (25% weight)
        try:
            c15 = await qx.get_candles(asset, time.time(), 15 * 30, 15)
            if c15 and len(c15) >= 10:
                # Check for clean directional movement vs choppy action
                closes = [float(c["close"]) for c in c15]
                highs = [float(c["high"]) for c in c15]
                lows = [float(c["low"]) for c in c15]

                # Trend consistency
                sign = 1 if direction == "call" else -1
                consistent_moves = 0
                for i in range(1, len(closes)):
                    if sign * (closes[i] - closes[i-1]) > 0:
                        consistent_moves += 1
                consistency = consistent_moves / (len(closes) - 1)

                # Range vs body ratio (avoid choppy markets)
                total_range = sum(highs[i] - lows[i] for i in range(-5, 0))
                total_body = sum(abs(closes[i] - float(c15[i]["open"])) for i in range(-5, 0))
                body_dominance = total_body / max(total_range, 1e-9)

                score += 0.25 * (0.6 * consistency + 0.4 * body_dominance)
        except Exception:
            pass

        # Support/Resistance respect (15% weight)
        try:
            trend_analysis = await _analyze_market_trend(qx, asset, 60, cfg)
            if trend_analysis.support_level and trend_analysis.resistance_level:
                c15 = await qx.get_candles(asset, time.time(), 15 * 10, 15)
                if c15:
                    current_price = float(c15[-1]["close"])
                    support = trend_analysis.support_level
                    resistance = trend_analysis.resistance_level

                    # Reward trades that respect key levels
                    if direction == "call" and current_price <= support * 1.002:
                        score += 0.15  # Buying near support
                    elif direction == "put" and current_price >= resistance * 0.998:
                        score += 0.15  # Selling near resistance
                    elif support < current_price < resistance:
                        # Penalize trades in middle of range
                        score += 0.05
        except Exception:
            pass

        return min(score, 1.0)

    except Exception:
        return 0.0


async def _enhanced_direction_validation(qx, asset: str, direction: str, cfg: S15LiveConfig) -> tuple[bool, float, str]:
    """Ultra-strict direction validation with multiple confirmation layers."""
    try:
        # Get signal quality score first
        quality_score = await _calculate_signal_quality_score(qx, asset, direction, cfg)

        if quality_score < 0.65:  # Require high quality signals only
            return False, quality_score, "low_quality"

        # Multi-timeframe momentum must ALL align
        sign = 1 if direction == "call" else -1
        momentum_checks = []

        for tf, bars in [(15, 5), (30, 8), (60, 12)]:
            try:
                candles = await qx.get_candles(asset, time.time(), tf * (bars + 10), tf)
                if candles and len(candles) >= bars + 5:
                    closes = [float(c["close"]) for c in candles]
                    momentum = closes[-1] - closes[-bars]
                    momentum_checks.append(sign * momentum > 0)
            except Exception:
                momentum_checks.append(False)

        # ALL timeframes must agree
        if not all(momentum_checks):
            return False, quality_score, "momentum_conflict"

        # Last 3 candles on 15s must support direction
        try:
            c15 = await qx.get_candles(asset, time.time(), 15 * 10, 15)
            if c15 and len(c15) >= 5:
                last_3_bodies = []
                for i in range(-3, 0):
                    body = float(c15[i]["close"]) - float(c15[i]["open"])
                    last_3_bodies.append(sign * body > 0)

                if sum(last_3_bodies) < 2:  # At least 2 of 3 must support
                    return False, quality_score, "candle_conflict"
        except Exception:
            return False, quality_score, "candle_error"

        # EMA alignment on all timeframes
        ema_aligned = 0
        for tf in [15, 30, 60]:
            try:
                candles = await qx.get_candles(asset, time.time(), tf * 100, tf)
                if candles and len(candles) >= max(cfg.ema_fast, cfg.ema_slow):
                    closes = [float(c["close"]) for c in candles]
                    ef = _ema(closes, cfg.ema_fast)
                    es = _ema(closes, cfg.ema_slow)
                    if ef and es:
                        if direction == "call" and ef[-1] > es[-1]:
                            ema_aligned += 1
                        elif direction == "put" and ef[-1] < es[-1]:
                            ema_aligned += 1
            except Exception:
                pass

        if ema_aligned < 2:  # At least 2 of 3 timeframes must have EMA alignment
            return False, quality_score, "ema_misalign"

        return True, quality_score, "validated"

    except Exception:
        return False, 0.0, "error"

# --- Fast Signal Quality Scoring (Optimized for Speed) ---
async def _fast_signal_quality_score(qx, asset: str, direction: str, cfg: S15LiveConfig) -> float:
    """Lightweight signal quality scoring optimized for speed."""
    try:
        score = 0.0
        sign = 1 if direction == "call" else -1

        # Single candle fetch for all timeframes (parallel-ready)
        c15 = await qx.get_candles(asset, time.time(), 15 * 25, 15)  # Reduced bars
        c30 = await qx.get_candles(asset, time.time(), 30 * 15, 30)  # Reduced bars
        c60 = await qx.get_candles(asset, time.time(), 60 * 12, 60)  # Reduced bars

        # Multi-timeframe momentum (50% weight) - simplified
        momentum_score = 0.0
        if c15 and len(c15) >= 8:
            closes15 = [float(c["close"]) for c in c15]
            mom15 = closes15[-1] - closes15[-5]
            if sign * mom15 > 0: momentum_score += 0.2

        if c30 and len(c30) >= 8:
            closes30 = [float(c["close"]) for c in c30]
            mom30 = closes30[-1] - closes30[-6]
            if sign * mom30 > 0: momentum_score += 0.15

        if c60 and len(c60) >= 8:
            closes60 = [float(c["close"]) for c in c60]
            mom60 = closes60[-1] - closes60[-8]
            if sign * mom60 > 0: momentum_score += 0.15

        score += momentum_score

        # Last 3 candles consistency (30% weight)
        if c15 and len(c15) >= 5:
            consistent = 0
            for i in range(-3, 0):
                body = float(c15[i]["close"]) - float(c15[i]["open"])
                if sign * body > 0: consistent += 1
            score += 0.3 * (consistent / 3.0)

        # EMA alignment quick check (20% weight)
        ema_score = 0.0
        for candles, weight in [(c15, 0.05), (c30, 0.08), (c60, 0.07)]:
            if candles and len(candles) >= max(cfg.ema_fast, cfg.ema_slow):
                closes = [float(c["close"]) for c in candles]
                ef = _ema(closes, cfg.ema_fast)
                es = _ema(closes, cfg.ema_slow)
                if ef and es:
                    if (direction == "call" and ef[-1] > es[-1]) or (direction == "put" and ef[-1] < es[-1]):
                        ema_score += weight
        score += ema_score

        return min(score, 1.0)

    except Exception:
        return 0.0


async def _fast_direction_validation(qx, asset: str, direction: str, cfg: S15LiveConfig) -> tuple[bool, float, str]:
    """Ultra-permissive validation for maximum signal generation."""
    try:
        # Very basic quality check
        quality_score = await _fast_signal_quality_score(qx, asset, direction, cfg)

        # Much lower threshold - accept more signals
        if quality_score < 0.25:  # Lowered from 0.55
            return False, quality_score, "very_low_quality"

        # Minimal momentum check - just 15s
        sign = 1 if direction == "call" else -1
        c15 = await qx.get_candles(asset, time.time(), 15 * 8, 15)

        if not c15 or len(c15) < 4:
            return False, quality_score, "no_data"

        closes15 = [float(c["close"]) for c in c15]

        # Very short momentum check (just 3 bars)
        if len(closes15) >= 4:
            mom15 = closes15[-1] - closes15[-3]
            # Allow even small momentum
            if sign * mom15 > -0.00001:  # Almost any movement accepted
                return True, quality_score, "validated"

        # If momentum check fails, try last candle direction
        if len(c15) >= 2:
            last_body = float(c15[-1]["close"]) - float(c15[-1]["open"])
            if sign * last_body > 0:
                return True, quality_score * 0.8, "last_candle"

        # Final fallback - accept if quality is decent
        if quality_score >= 0.40:
            return True, quality_score * 0.7, "quality_override"

        return False, quality_score, "all_checks_failed"

    except Exception:
        return False, 0.0, "error"


# --- Parallel Asset Scanning for Speed ---
async def _scan_assets_parallel(qx, assets: List[str], cfg: S15LiveConfig, debug: bool = False) -> List[tuple[str, str, float, str]]:
    """Scan multiple assets in parallel for faster signal detection."""
    import asyncio

    async def scan_single_asset(asset: str) -> Optional[tuple[str, str, float, str]]:
        try:
            # Multi-layer approach: Try different methods until we find a signal

            # Method 1: Trend-based (relaxed thresholds)
            trend60 = await _analyze_market_trend(qx, asset, 60, cfg)
            trade_dir = None
            mode = None
            confidence_boost = 0.0

            # Strong trends (high confidence)
            if trend60.strength >= 0.35 and trend60.confidence >= 0.45:
                if trend60.trend_type.name.lower() == "uptrend":
                    trade_dir = "call"; mode = "trend_follow"; confidence_boost = 0.15
                elif trend60.trend_type.name.lower() == "downtrend":
                    trade_dir = "put"; mode = "trend_follow"; confidence_boost = 0.15
                elif trend60.trend_type.name.lower() == "sideways":
                    # Breakout detection
                    if trend60.breakout_direction == "up":
                        trade_dir = "call"; mode = "breakout"; confidence_boost = 0.10
                    elif trend60.breakout_direction == "down":
                        trade_dir = "put"; mode = "breakout"; confidence_boost = 0.10

            # Method 2: EMA crossover (if trend method didn't work)
            if not trade_dir:
                try:
                    c60 = await qx.get_candles(asset, time.time(), 60 * 30, 60)
                    if c60 and len(c60) >= max(cfg.ema_fast, cfg.ema_slow):
                        closes = [float(c["close"]) for c in c60]
                        ef = _ema(closes, cfg.ema_fast)
                        es = _ema(closes, cfg.ema_slow)
                        if ef and es and len(ef) >= 3 and len(es) >= 3:
                            # Price direction confirmation
                            current_price = closes[-1]
                            prev_price = closes[-2] if len(closes) >= 2 else current_price
                            price_direction = current_price - prev_price

                            # Recent crossover
                            cross_up = ef[-2] <= es[-2] and ef[-1] > es[-1]
                            cross_dn = ef[-2] >= es[-2] and ef[-1] < es[-1]
                            # Strong alignment
                            strong_up = ef[-1] > es[-1] and (ef[-1] - es[-1]) / es[-1] > 0.001
                            strong_dn = ef[-1] < es[-1] and (es[-1] - ef[-1]) / ef[-1] > 0.001

                            # CRITICAL: EMA signal must match actual price movement direction
                            if (cross_up or strong_up) and price_direction >= 0:
                                trade_dir = "call"; mode = "ema_cross"; confidence_boost = 0.08
                            elif (cross_dn or strong_dn) and price_direction <= 0:
                                trade_dir = "put"; mode = "ema_cross"; confidence_boost = 0.08
                except Exception:
                    pass

            # Method 3: Momentum-based (if EMA didn't work)
            if not trade_dir:
                try:
                    c15 = await qx.get_candles(asset, time.time(), 15 * 20, 15)
                    c30 = await qx.get_candles(asset, time.time(), 30 * 15, 30)

                    if c15 and c30 and len(c15) >= 10 and len(c30) >= 8:
                        closes15 = [float(c["close"]) for c in c15]
                        closes30 = [float(c["close"]) for c in c30]

                        # Multi-timeframe momentum
                        mom15_short = closes15[-1] - closes15[-3]
                        mom15_med = closes15[-1] - closes15[-6]
                        mom30_short = closes30[-1] - closes30[-3]
                        mom30_med = closes30[-1] - closes30[-6]

                        # Count positive momentum signals
                        call_signals = sum([mom15_short > 0, mom15_med > 0, mom30_short > 0, mom30_med > 0])
                        put_signals = sum([mom15_short < 0, mom15_med < 0, mom30_short < 0, mom30_med < 0])

                        if call_signals >= 3:
                            trade_dir = "call"; mode = "momentum"; confidence_boost = 0.05
                        elif put_signals >= 3:
                            trade_dir = "put"; mode = "momentum"; confidence_boost = 0.05
                except Exception:
                    pass

            # Method 4: RSI extremes (last resort)
            if not trade_dir:
                try:
                    c15 = await qx.get_candles(asset, time.time(), 15 * 30, 15)
                    if c15 and len(c15) >= 20:
                        closes = [float(c["close"]) for c in c15]
                        # Simple RSI calculation
                        gains = []; losses = []
                        for i in range(1, len(closes)):
                            change = closes[i] - closes[i-1]
                            gains.append(max(change, 0))
                            losses.append(max(-change, 0))

                        if len(gains) >= 14:
                            avg_gain = sum(gains[-14:]) / 14
                            avg_loss = sum(losses[-14:]) / 14
                            if avg_loss > 0:
                                rs = avg_gain / avg_loss
                                rsi = 100 - (100 / (1 + rs))

                                if rsi <= 35:  # Oversold
                                    trade_dir = "call"; mode = "rsi_oversold"; confidence_boost = 0.03
                                elif rsi >= 65:  # Overbought
                                    trade_dir = "put"; mode = "rsi_overbought"; confidence_boost = 0.03
                except Exception:
                    pass

            if not trade_dir:
                return None

            # Lightweight validation (much more permissive)
            base_quality = await _fast_signal_quality_score(qx, asset, trade_dir, cfg)
            final_quality = min(base_quality + confidence_boost, 1.0)

            # Much lower threshold for signals
            if final_quality >= 0.35:  # Lowered from 0.55
                return (asset, trade_dir, final_quality, mode)

            return None

        except Exception:
            return None

    # Run scans in parallel (batches of 10 for API limits)
    results = []
    batch_size = 10

    for i in range(0, len(assets), batch_size):
        batch = assets[i:i + batch_size]
        batch_tasks = [scan_single_asset(asset) for asset in batch]
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

        for result in batch_results:
            if result and not isinstance(result, Exception):
                results.append(result)

    # Sort by quality score (highest first)
    results.sort(key=lambda x: x[2], reverse=True)

    if debug and results:
        print(f"[S15] Parallel scan found {len(results)} candidates, top quality: {results[0][2]:.2f}")

    return results



async def _payout_for(qx, asset: str, expiry_min: int) -> float:
    """Fetch payout percentage for an asset/expiry. Always returns a float (0.0 if unknown)."""
    try:
        keys: List[str] = []
        if expiry_min <= 1:
            keys = ["1", "60"]
        elif expiry_min >= 5:
            keys = ["5", "300"] if expiry_min == 5 else [str(expiry_min), str(expiry_min * 60)]
        else:
            keys = [str(expiry_min)]
        # Try timeframe-specific first
        for k in keys:
            try:
                val = qx.get_payout_by_asset(asset, timeframe=k)
                if val is not None:
                    return float(val)
            except Exception:
                continue
        # Fallback: without timeframe
        try:
            val = qx.get_payout_by_asset(asset)
            if val is not None:
                return float(val)
        except Exception:
            pass
        # Last resort: return 0.0 to avoid None
        return 0.0
    except Exception:
        return 0.0


async def _confluence_ok(qx, asset: str, tf: int, desired_dir: str, cfg: S15LiveConfig, debug: bool = False) -> bool:
    """Check confluence on tf using multiple fallbacks: original decision -> trend -> EMA alignment."""
    try:
        okc, dc = await _decide_live_for_asset(qx, asset, tf, cfg)
        if okc and dc == desired_dir:
            if debug:
                print(f"[S15] Confluence ok via _decide on {tf}s: {dc}")
            return True
    except Exception:
        pass
    # Fallback: trend agreement on this TF
    try:
        trend = await _analyze_market_trend(qx, asset, tf, cfg)
        if desired_dir == "call" and trend.trend_type == TrendType.UPTREND and trend.strength >= 0.45 and trend.confidence >= 0.50:
            if debug:
                print(f"[S15] Confluence ok via trend on {tf}s: uptrend s={trend.strength:.2f} c={trend.confidence:.2f}")
            return True
        if desired_dir == "put" and trend.trend_type == TrendType.DOWNTREND and trend.strength >= 0.45 and trend.confidence >= 0.50:
            if debug:
                print(f"[S15] Confluence ok via trend on {tf}s: downtrend s={trend.strength:.2f} c={trend.confidence:.2f}")
            return True
    except Exception:
        pass
    # Last fallback: EMA alignment on this TF
    try:
        candles = await qx.get_candles(asset, time.time(), tf * 120, tf)
        closes = [float(c["close"]) for c in candles] if candles else []
        if not closes or len(closes) < max(cfg.ema_fast, cfg.ema_slow):
            return False
        ef = _ema(closes, cfg.ema_fast)
        es = _ema(closes, cfg.ema_slow)
        if not ef or not es:
            return False
        if desired_dir == "call" and ef[-1] > es[-1]:
            if debug:
                print(f"[S15] Confluence ok via EMA on {tf}s: ef>es")
            return True
        if desired_dir == "put" and ef[-1] < es[-1]:
            if debug:
                print(f"[S15] Confluence ok via EMA on {tf}s: ef<es")
            return True
    except Exception:
        return False
    return False





def _ist_hour(ts: float) -> int:
    try:
        if ZoneInfo is None:
            return int(datetime.utcfromtimestamp(ts).hour)
        return int(datetime.fromtimestamp(ts, ZoneInfo("Asia/Kolkata")).hour)
    except Exception:
        return int(datetime.utcfromtimestamp(ts).hour)


def _parse_time_to_ts(s: str, tz_mode: str) -> Optional[int]:
    s = s.strip()
    # Supported formats:
    #   HH:MM
    #   YYYY-MM-DD HH:MM
    #   HH:MM:SS
    # Interpret as today if no date
    try:
        now = datetime.now()
        if len(s) <= 5:  # HH:MM
            hh, mm = s.split(":")
            dt_local = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
        elif len(s) <= 8 and s.count(":") == 2:  # HH:MM:SS
            hh, mm, ss = s.split(":")
            dt_local = now.replace(hour=int(hh), minute=int(mm), second=int(ss), microsecond=0)
        else:
            # Has date component; assume local date part
            # Example: 2025-08-16 12:40
            parts = s.split()
            date_str = parts[0]
            time_str = parts[1] if len(parts) > 1 else "00:00"
            y, m, d = [int(x) for x in date_str.split("-")]
            hh, mm = [int(x) for x in time_str.split(":")[:2]]
            ss = int(time_str.split(":")[2]) if time_str.count(":") >= 2 else 0
            dt_local = datetime(year=y, month=m, day=d, hour=hh, minute=mm, second=ss)
        # Map tz
        if tz_mode.upper() == "IST" and ZoneInfo is not None:
            dt = dt_local.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
            return int(dt.timestamp())
        elif tz_mode.upper() == "UTC":
            dt = dt_local.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        else:
            # LOCAL
            try:
                from tzlocal import get_localzone  # optional
                tz = get_localzone()
                dt = dt_local.replace(tzinfo=tz)  # type: ignore
            except Exception:
                dt = dt_local.replace(tzinfo=None)
            return int(dt.timestamp())
    except Exception:
        return None


def load_trades_file(path: str, tz_mode: str = "IST") -> List[S15Schedule]:
    out: List[S15Schedule] = []
    if not path or not os.path.exists(path):
        return out
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if not raw or raw.startswith("#"):
                    continue
                # Expected comma-separated: time,asset,direction
                # Example: 12:40, AUDUSD, CALL
                parts = [p.strip() for p in raw.split(",")]
                if len(parts) < 3:
                    continue
                t_str, asset, direction = parts[0], parts[1], parts[2]
                ts = _parse_time_to_ts(t_str, tz_mode)
                if ts is None:
                    continue
                dir_lower = direction.lower()
                if dir_lower not in ("call", "put"):
                    continue
                # Align to minute start
                ts -= (ts % 60)
                out.append(S15Schedule(when_ts=int(ts), asset=asset, direction=dir_lower, raw=raw))
    except Exception:
        return out
    return out


def _due_scheduled(now_ts: int, sched: List[S15Schedule], window_sec: int, allowed_hours_ist: Optional[set[int]]) -> Optional[S15Schedule]:
    # Pick the first due item within [ts, ts+window)
    for item in sched:
        if allowed_hours_ist and _ist_hour(item.when_ts) not in allowed_hours_ist:
            continue
        key = f"{item.when_ts}|{item.asset}|{item.direction}"
        if key in S15_EXECUTED:
            continue
        if item.when_ts <= now_ts < (item.when_ts + max(1, window_sec)):
            S15_EXECUTED.add(key)
            return item
    return None


async def _decide_live_for_asset(qx, asset: str, tf: int, cfg: S15LiveConfig) -> Tuple[bool, str]:
    # Fetch candles
    try:
        candles = await qx.get_candles(asset, time.time(), tf * 120, tf)
    except Exception:
        candles = []
    if not candles or len(candles) < 30:
        return False, "call"
    closes = [float(c["close"]) for c in candles]
    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]
    opens = [float(c["open"]) for c in candles]

    ema_f = _ema(closes, cfg.ema_fast)
    ema_s = _ema(closes, cfg.ema_slow)
    if len(ema_f) < 3 or len(ema_s) < 3:
        return False, "call"
    cross_up = ema_f[-2] <= ema_s[-2] and ema_f[-1] > ema_s[-1]
    cross_dn = ema_f[-2] >= ema_s[-2] and ema_f[-1] < ema_s[-1]

    # RSI
    try:
        r = await qx.calculate_indicator(asset, "RSI", {"period": cfg.rsi_period}, timeframe=tf)
        rsi_vals = r.get("rsi", []) if isinstance(r, dict) else []
    except Exception:
        rsi_vals = []
    rsi_ok_up = bool(rsi_vals) and (cfg.rsi_buy_min <= float(rsi_vals[-1]) <= 70)
    rsi_ok_dn = bool(rsi_vals) and (30 <= float(rsi_vals[-1]) <= cfg.rsi_sell_max)

    # ADX
    adx_vals = _adx(highs, lows, closes, cfg.adx_period)
    adx_ok = bool(adx_vals) and float(adx_vals[-1]) >= float(cfg.adx_min)

    # Body quality
    last = candles[-1]
    body = abs(float(last["close"]) - float(last["open"]))
    rng = max(1e-9, float(last["high"]) - float(last["low"]))
    body_ok = (body / rng) >= float(cfg.min_body_ratio)

    if cross_up and rsi_ok_up and adx_ok and body_ok:
        return True, "call"
    if cross_dn and rsi_ok_dn and adx_ok and body_ok:
        return True, "put"
    return False, "call"

async def _decide_live_for_asset_relaxed(qx, asset: str, tf: int, cfg: S15LiveConfig) -> Tuple[bool, str]:
    """Relaxed variant to increase entries when primary gate is tight."""
    try:
        candles = await qx.get_candles(asset, time.time(), tf * 120, tf)
    except Exception:
        candles = []
    if not candles or len(candles) < 20:
        return False, "call"
    closes = [float(c["close"]) for c in candles]
    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]

    ema_f = _ema(closes, cfg.ema_fast)
    ema_s = _ema(closes, cfg.ema_slow)
    if len(ema_f) < 3 or len(ema_s) < 3:
        return False, "call"

    # RSI last
    try:
        r = await qx.calculate_indicator(asset, "RSI", {"period": cfg.rsi_period}, timeframe=tf)
        rsi_vals = r.get("rsi", []) if isinstance(r, dict) else []
    except Exception:
        rsi_vals = []
    r_last = float(rsi_vals[-1]) if rsi_vals else 50.0

    # Relaxed ADX/body thresholds
    body_min = max(0.20, float(cfg.min_body_ratio) - 0.05)
    last = candles[-1]
    body = abs(float(last["close"]) - float(last["open"]))
    rng = max(1e-9, float(last["high"]) - float(last["low"]))
    body_ok = (body / rng) >= body_min

    # Relaxed adx check (optional)
    adx_vals = _adx(highs, lows, closes, cfg.adx_period)
    adx_ok_relaxed = True
    try:
        if adx_vals:
            adx_ok_relaxed = float(adx_vals[-1]) >= max(10.0, float(cfg.adx_min) - 3.0)
    except Exception:
        pass

    # Accept either cross OR alignment+momentum
    cross_up = ema_f[-2] <= ema_s[-2] and ema_f[-1] > ema_s[-1]
    cross_dn = ema_f[-2] >= ema_s[-2] and ema_f[-1] < ema_s[-1]
    align_up = (ema_f[-1] > ema_s[-1]) and (ema_f[-1] > ema_f[-2]) and (r_last >= 50)
    align_dn = (ema_f[-1] < ema_s[-1]) and (ema_f[-1] < ema_f[-2]) and (r_last <= 50)

    if body_ok and adx_ok_relaxed:
        if cross_up or align_up:
            return True, "call"
        if cross_dn or align_dn:
            return True, "put"
    return False, "call"


async def find_first_signal_s15(
    qx,
    cfg: S15Config,
    min_payout: float,
    expiry_min: int,
    debug: bool = False,
) -> Tuple[Optional[str], Optional[str]]:
    """Hybrid scanner for Strategy 15.
    Returns (asset, direction) from schedule (if due) or from live analysis.
    """
    now_ts = int(time.time())

    # 1) Scheduled path
    if cfg.mode in ("scheduled", "hybrid") and cfg.trades_file:
        sched = load_trades_file(cfg.trades_file, tz_mode=cfg.schedule_tz)
        due = _due_scheduled(now_ts, sched, cfg.match_window_sec, cfg.allowed_hours_ist)
        if due:
            # Optional payout guard
            p = await _payout_for(qx, due.asset, expiry_min)
            if p >= min_payout:
                if debug:
                    print(f"[S15] Scheduled due: {due.raw} (payout={p:.0f}%)")
                _log_s15_signal("scheduled", due.asset, due.direction, note="due", schedule_ts=due.when_ts, payout=p, tf_s=expiry_min*60)
                globals()["S15_LAST_TF"] = int(expiry_min) * 60
                globals()["S15_LAST_MODE"] = "scheduled"
                return due.asset, due.direction
            else:
                if debug:
                    print(f"[S15] Scheduled due but payout too low: {due.raw} ({p:.0f}%<{min_payout:.0f}%)")

    # 2) Live analysis path
    if cfg.mode in ("live", "hybrid"):
        try:
            assets = await _get_assets(qx)
        except Exception:
            assets = []
        # Filter by payout
        eligible: List[str] = []
        hi_payout: List[str] = []
        for a in assets:
            p = await _payout_for(qx, a, expiry_min)
            if p >= min_payout:
                # Hour filter (IST now)
                if cfg.allowed_hours_ist is not None:
                    cur_h = _ist_hour(now_ts)
                    if cur_h not in cfg.allowed_hours_ist:
                        continue
                # Cooldown filter
                if s15_is_on_cooldown(a, now_ts):
                    continue
                eligible.append(a)
                if p >= max(87.0, float(min_payout)):
                    hi_payout.append(a)
        pool = hi_payout if hi_payout else eligible
        if debug:
            print(f"[S15] Eligible by payout: {len(eligible)} (hi={len(hi_payout)})")
        # Confluence-first scan: require 60s pass + 30s or 15s agreement
        # Always try 60s base first
        base_tf = 60
        conf_tfs = [30, 15]
        # Use parallel scanning for speed optimization
        if debug:
            print(f"[S15] Starting parallel scan of {len(pool)} assets...")

        scan_results = await _scan_assets_parallel(qx, pool, cfg.live, debug)

        if not scan_results:
            if debug:
                print(f"[S15] No signals from parallel scan across {len(pool)} assets")
            return None, None

        # Use smart asset selection with advanced timing analysis
        best_candidate = await _select_best_asset_with_conditions(qx, scan_results, cfg.live, debug)

        if not best_candidate:
            if debug:
                print(f"[S15] No candidates passed advanced validation")
            return None, None

        best_asset, best_direction, final_score, best_mode, optimal_expiry = best_candidate

        if debug:
            print(f"[S15] Final selection: {best_asset} {best_direction} score={final_score:.2f} mode={best_mode} expiry={optimal_expiry}s")

        # Store optimal expiry for main.py to use
        try:
            globals()["S15_OPTIMAL_EXPIRY"] = optimal_expiry
        except Exception:
            pass

        # Light confluence check (since we already did heavy validation)
        conf_ok = False
        for tfc in conf_tfs:
            if await _confluence_ok(qx, best_asset, tfc, best_direction, cfg.live, debug):
                conf_ok = True
                break

        # If confluence fails, accept high-score signals anyway (since we did heavy validation)
        if not conf_ok:
            if final_score >= 0.60:  # Use final_score instead of best_quality
                if debug:
                    print(f"[S15] Confluence failed but accepting due to final score {final_score:.2f} >= 0.60")
                conf_ok = True
            else:
                if debug:
                    print(f"[S15] Confluence failed and final score too low ({final_score:.2f} < 0.60)")
                return None, None


        # Log and return the selected signal
        _log_s15_signal(
            "live",
            best_asset,
            best_direction,
            note=f"parallel_scan score={final_score:.2f} mode={best_mode}",
            payout=await _payout_for(qx, best_asset, expiry_min),
            tf_s=60,
        )

        try:
            globals()["S15_LAST_TF"] = 60
            globals()["S15_LAST_MODE"] = "live"
        except Exception:
            pass

        return best_asset, best_direction
    return None, None


# --- Real-Time Entry Validation (Final Gate Before Trade) ---
async def _final_entry_validation(qx, asset: str, direction: str, cfg: S15LiveConfig) -> tuple[bool, str]:
    """Ultra-strict final validation right before trade execution."""
    try:
        # Get fresh data (last 10 seconds)
        c15 = await qx.get_candles(asset, time.time(), 15 * 8, 15)
        if not c15 or len(c15) < 5:
            return False, "no_fresh_data"

        # Check last 2 candles (most recent 30 seconds)
        last_candle = c15[-1]
        prev_candle = c15[-2]

        last_open = float(last_candle["open"])
        last_close = float(last_candle["close"])
        last_high = float(last_candle["high"])
        last_low = float(last_candle["low"])

        prev_close = float(prev_candle["close"])

        # Direction validation
        sign = 1 if direction == "call" else -1

        # 1. Last candle must support direction
        last_body = last_close - last_open
        if sign * last_body <= 0:
            return False, "last_candle_opposite"

        # 2. Price momentum in last 30 seconds
        momentum_30s = last_close - prev_close
        if sign * momentum_30s <= 0:
            return False, "momentum_30s_opposite"

        # 3. Current price position check
        current_price = last_close
        candle_range = last_high - last_low

        if direction == "call":
            # For CALL: price should be in upper half of candle
            if (current_price - last_low) / max(candle_range, 1e-9) < 0.4:
                return False, "call_price_too_low"
        else:
            # For PUT: price should be in lower half of candle
            if (last_high - current_price) / max(candle_range, 1e-9) < 0.4:
                return False, "put_price_too_high"

        # 4. Volatility check - avoid dead markets
        if candle_range / max(abs(last_open), 1e-9) < 0.0001:  # Less than 0.01% range
            return False, "market_too_quiet"

        # 5. Wick dominance check - avoid reversal candles
        body_size = abs(last_body)
        if direction == "call":
            upper_wick = last_high - max(last_open, last_close)
            if upper_wick > body_size * 2:  # Upper wick too big for CALL
                return False, "call_upper_wick_dominant"
        else:
            lower_wick = min(last_open, last_close) - last_low
            if lower_wick > body_size * 2:  # Lower wick too big for PUT
                return False, "put_lower_wick_dominant"

        # 6. Multi-candle trend check (last 3 candles)
        if len(c15) >= 4:
            closes = [float(c["close"]) for c in c15[-4:]]
            trend_score = 0
            for i in range(1, len(closes)):
                if sign * (closes[i] - closes[i-1]) > 0:
                    trend_score += 1

            if trend_score < 2:  # At least 2 of 3 moves in our direction
                return False, "multi_candle_trend_weak"

        return True, "validated"

    except Exception as e:
        return False, f"error_{str(e)[:20]}"


# --- Smart Asset Selection with Market Condition Filter ---
async def _select_best_asset_with_conditions(qx, scan_results: List[tuple[str, str, float, str]], cfg: S15LiveConfig, debug: bool = False) -> Optional[tuple[str, str, float, str]]:
    """Select best asset considering current market conditions."""
    try:
        if not scan_results:
            return None

        # Filter candidates through final validation
        validated_candidates = []

        for asset, direction, quality, mode in scan_results[:5]:  # Check top 5 only
            # Enhanced final entry validation with optimal expiry
            entry_ok, entry_reason = await _final_entry_validation(qx, asset, direction, cfg)

            if entry_ok:
                # Advanced market microstructure analysis
                micro_ok, micro_reason, micro_score = await _analyze_market_microstructure(qx, asset, direction)

                if micro_ok:
                    # Direction hold time analysis
                    optimal_expiry, hold_confidence = await _analyze_direction_hold_time(qx, asset, direction)

                    # Combined scoring with timing analysis
                    timing_score = (hold_confidence * 0.4) + (micro_score * 0.6)
                    final_score = quality * 0.5 + timing_score * 0.5

                    validated_candidates.append((asset, direction, final_score, mode, entry_reason, optimal_expiry, micro_score))

                    if debug:
                        print(f"[S15] Advanced validation: {asset} {direction} quality={quality:.2f} micro={micro_score:.2f} timing={timing_score:.2f} final={final_score:.2f} expiry={optimal_expiry}s")
                else:
                    if debug:
                        print(f"[S15] Microstructure failed: {asset} {direction} reason={micro_reason} score={micro_score:.2f}")
            else:
                if debug:
                    print(f"[S15] Entry validation failed: {asset} {direction} reason={entry_reason}")

        if not validated_candidates:
            if debug:
                print(f"[S15] No candidates passed advanced validation")
            return None

        # Sort by final score and return best with expiry info
        validated_candidates.sort(key=lambda x: x[2], reverse=True)
        best = validated_candidates[0]

        # Return with optimal expiry information
        return (best[0], best[1], best[2], best[3], best[5])  # asset, direction, score, mode, expiry

    except Exception:
        return None


async def _calculate_market_condition_score(qx, asset: str, direction: str) -> float:
    """Calculate market condition score for better entry timing."""
    try:
        score = 0.0

        # Get recent price action
        c15 = await qx.get_candles(asset, time.time(), 15 * 12, 15)
        if not c15 or len(c15) < 8:
            return 0.3  # Default neutral score

        closes = [float(c["close"]) for c in c15]
        highs = [float(c["high"]) for c in c15]
        lows = [float(c["low"]) for c in c15]

        # 1. Trend consistency (40% weight)
        sign = 1 if direction == "call" else -1
        consistent_moves = 0
        for i in range(1, len(closes)):
            if sign * (closes[i] - closes[i-1]) > 0:
                consistent_moves += 1
        consistency = consistent_moves / (len(closes) - 1)
        score += 0.4 * consistency

        # 2. Volatility appropriateness (30% weight)
        ranges = [highs[i] - lows[i] for i in range(len(c15))]
        avg_range = sum(ranges) / len(ranges)
        current_range = ranges[-1]

        # Prefer moderate volatility (not too quiet, not too wild)
        vol_ratio = current_range / max(avg_range, 1e-9)
        if 0.8 <= vol_ratio <= 1.5:  # Good volatility
            score += 0.3
        elif 0.5 <= vol_ratio <= 2.0:  # Acceptable volatility
            score += 0.15

        # 3. Price position (30% weight)
        current_price = closes[-1]
        recent_high = max(highs[-5:])
        recent_low = min(lows[-5:])
        price_position = (current_price - recent_low) / max(recent_high - recent_low, 1e-9)

        if direction == "call" and price_position < 0.7:  # Good entry for CALL
            score += 0.3
        elif direction == "put" and price_position > 0.3:  # Good entry for PUT
            score += 0.3

        return min(score, 1.0)

    except Exception:
        return 0.3


# --- Direction Hold Time Analysis ---
async def _analyze_direction_hold_time(qx, asset: str, direction: str) -> tuple[int, float]:
    """Analyze how long the direction typically holds for this asset."""
    try:
        # Get extended historical data
        c15 = await qx.get_candles(asset, time.time(), 15 * 100, 15)  # 25 minutes of data
        if not c15 or len(c15) < 50:
            return 60, 0.5  # Default 1 minute, 50% confidence

        closes = [float(c["close"]) for c in c15]
        sign = 1 if direction == "call" else -1

        # Analyze direction hold patterns
        hold_times = []
        current_hold = 0

        for i in range(1, len(closes)):
            move = closes[i] - closes[i-1]
            if sign * move > 0:  # Moving in our direction
                current_hold += 1
            else:
                if current_hold > 0:
                    hold_times.append(current_hold * 15)  # Convert to seconds
                current_hold = 0

        if current_hold > 0:
            hold_times.append(current_hold * 15)

        if not hold_times:
            return 60, 0.3

        # Calculate statistics
        avg_hold = sum(hold_times) / len(hold_times)

        # Determine optimal expiry
        if avg_hold >= 120:  # 2+ minutes average
            optimal_expiry = min(180, int(avg_hold * 0.8))  # 80% of average, max 3 min
            confidence = 0.8
        elif avg_hold >= 90:  # 1.5+ minutes average
            optimal_expiry = min(120, int(avg_hold * 0.9))  # 90% of average, max 2 min
            confidence = 0.7
        else:  # Short holds
            optimal_expiry = 60  # Stick to 1 minute
            confidence = 0.4

        return optimal_expiry, confidence

    except Exception:
        return 60, 0.5


# --- Market Microstructure Analysis ---
async def _analyze_market_microstructure(qx, asset: str, direction: str) -> tuple[bool, str, float]:
    """Analyze market microstructure for optimal entry timing."""
    try:
        # Get very recent data (last 5 minutes)
        c15 = await qx.get_candles(asset, time.time(), 15 * 20, 15)
        if not c15 or len(c15) < 10:
            return False, "insufficient_data", 0.0

        closes = [float(c["close"]) for c in c15]
        highs = [float(c["high"]) for c in c15]
        lows = [float(c["low"]) for c in c15]
        volumes = [_get_vol(c) for c in c15]

        sign = 1 if direction == "call" else -1
        current_price = closes[-1]

        # 1. Momentum acceleration check
        mom_1min = closes[-1] - closes[-4]  # Last 1 minute
        mom_2min = closes[-1] - closes[-8]  # Last 2 minutes

        momentum_accelerating = (abs(mom_1min) > abs(mom_2min) * 0.5) and (sign * mom_1min > 0)

        # 2. Volume confirmation
        recent_vol = sum(volumes[-4:]) / 4  # Last 1 minute average
        prev_vol = sum(volumes[-8:-4]) / 4  # Previous 1 minute average
        volume_increasing = recent_vol > prev_vol * 1.1

        # 3. Price level analysis
        recent_high = max(highs[-8:])
        recent_low = min(lows[-8:])
        price_range = recent_high - recent_low

        if direction == "call":
            # For CALL: check if we're breaking above resistance
            resistance_break = current_price > recent_high * 0.999
            support_distance = (current_price - recent_low) / max(price_range, 1e-9)
            level_score = 0.8 if resistance_break else (0.6 if support_distance > 0.7 else 0.3)
        else:
            # For PUT: check if we're breaking below support
            support_break = current_price < recent_low * 1.001
            resistance_distance = (recent_high - current_price) / max(price_range, 1e-9)
            level_score = 0.8 if support_break else (0.6 if resistance_distance > 0.7 else 0.3)

        # 4. Trend continuation vs reversal
        trend_score = 0.0
        if len(closes) >= 12:
            short_trend = closes[-1] - closes[-4]   # 1 min
            med_trend = closes[-1] - closes[-8]     # 2 min
            long_trend = closes[-1] - closes[-12]   # 3 min

            if sign * short_trend > 0 and sign * med_trend > 0 and sign * long_trend > 0:
                trend_score = 0.9  # Strong continuation
            elif sign * short_trend > 0 and sign * med_trend > 0:
                trend_score = 0.7  # Medium continuation
            elif sign * short_trend > 0:
                trend_score = 0.4  # Weak/potential reversal
            else:
                trend_score = 0.1  # Against trend

        # Combine all factors
        microstructure_score = (
            0.30 * (0.8 if momentum_accelerating else 0.2) +
            0.15 * (0.8 if volume_increasing else 0.4) +
            0.25 * level_score +
            0.30 * trend_score
        )

        # Decision logic
        if microstructure_score >= 0.65:
            return True, "excellent_setup", microstructure_score
        elif microstructure_score >= 0.50:
            return True, "good_setup", microstructure_score
        elif microstructure_score >= 0.35:
            return True, "acceptable_setup", microstructure_score
        else:
            return False, "poor_setup", microstructure_score

    except Exception:
        return False, "analysis_error", 0.0