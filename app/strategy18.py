from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple, List
import os
import time
from datetime import datetime, timezone
import asyncio


try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

# Reuse PSAR implementation from Strategy 10 helpers
try:
    from strategy10_confluence import compute_psar
except Exception:
    try:
        from .strategy10_confluence import compute_psar  # type: ignore
    except Exception:
        compute_psar = None  # type: ignore


@dataclass
class S18Config:
    """Configuration for Strategy 18.
    - allowed_hours_ist: IST hours filter (0-23). If None, allow all hours.
    - timeframe_s: fixed analysis timeframe (default 60s)
    - expiry_s: fixed expiry (default 60s)
    - min_bars: minimum bars needed for stable computations
    """
    allowed_hours_ist: Optional[set[int]] = None
    timeframe_s: int = 60
    expiry_s: int = 60
    min_bars: int = 80
    # Scan controls
    scan_interval_s: float = 1.5  # loop interval when no signal
    minute_sync: bool = True      # align entries early in minute for stability
    entry_window_start: int = 2   # second to start entries (inclusive)
    entry_window_end: int = 15    # second to end entries (inclusive)
    # Quality gates (configurable)
    psar_confirm_bars: int = 2            # 1 or 2 (video-style stricter = 2)
    bears_monotonic_3: bool = True        # use 3-point monotonic check
    min_body_ratio: float = 0.25          # candle body/true-range minimum
    min_fast_slope_ratio: float = 0.0     # min |ΔEMA3|/price_ref (0 = disabled)
    min_ema_gap_ratio: float = 0.0        # min |EMA3-EMA7|/price_ref (0 = disabled)
    min_bears_delta_ratio: float = 0.0    # min |ΔBears|/price_ref (0 = disabled)
    # ADX filter (trend strength)
    adx_period: int = 14
    adx_skip_below: float = 20.0          # skip if ADX < this
    adx_skip_yellow: bool = False         # optionally skip 20-25 zone if True
    adx_yellow_min: float = 20.0
    adx_yellow_max: float = 25.0
    # Diagnostics
    verbose: bool = True

# Short cooldowns for problematic assets (e.g., no data)
S18_COOLDOWNS: dict[str, float] = {}
# Per-asset consecutive zero-candle fail counts
S18_FAIL_COUNTS: dict[str, int] = {}
# Temporary blacklist for consistently failing symbols
S18_BLACKLIST: dict[str, float] = {}
# Pass-level storm detection
S18_HIGH_ZERO_STREAK: int = 0
S18_STORM_MODE_UNTIL: float = 0.0
# Throttle forced reconnects
S18_LAST_RECONNECT_TS: float = 0.0
# Hard reconnect escalation tracking
S18_CONSECUTIVE_HIGH_ZERO_PASSES: int = 0
S18_LAST_HARD_RECONNECT_TS: float = 0.0

def s18_is_on_cooldown(asset: str, now_ts: float) -> bool:
    try:
        until = S18_COOLDOWNS.get(asset, 0.0)
        return now_ts < float(until)
    except Exception:
        return False


def s18_is_blacklisted(asset: str, now_ts: float) -> bool:
    try:
        until = S18_BLACKLIST.get(asset, 0.0)
        return now_ts < float(until)
    except Exception:
        return False


# --- Diagnostics CSV ---
S18_SIGNAL_LOG = os.path.join(os.path.dirname(__file__), "..", "strategy18_signals.csv")


def _ensure_s18_log_header() -> None:
    try:
        if not os.path.exists(S18_SIGNAL_LOG):
            import csv
            with open(S18_SIGNAL_LOG, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    "ts_local", "asset", "dir", "payout", "confluence",
                    "ema7", "ema3", "ema_cross", "ema7_slope",
                    "psar_pos", "bears", "bears_slope", "candle", "note",
                ])
    except Exception:
        pass


def _log_signal(asset: str, direction: str, payout: float, confluence: bool,
                ema7: float, ema3: float, ema_cross: str, ema7_slope: float,
                psar_pos: str, bears: float, bears_slope: float, candle: str,
                note: str = "") -> None:
    try:
        _ensure_s18_log_header()
        ist = ""
        try:
            tz = ZoneInfo("Asia/Kolkata") if ZoneInfo else timezone.utc
            ist = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S %Z")
        except Exception:
            ist = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        import csv
        with open(S18_SIGNAL_LOG, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                ist, asset, direction, float(payout), int(bool(confluence)),
                ema7, ema3, ema_cross, ema7_slope, psar_pos, bears, bears_slope,
                candle, note,
            ])
    except Exception:
        pass


# --- Small Helpers ---

def _ist_hour(ts: float) -> int:
    try:
        if ZoneInfo is None:
            return int(datetime.utcfromtimestamp(ts).hour)
        return int(datetime.fromtimestamp(ts, ZoneInfo("Asia/Kolkata")).hour)
    except Exception:
        return int(datetime.utcfromtimestamp(ts).hour)


def _ema(values: List[float], period: int) -> List[float]:
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    out = [sum(values[:period]) / period]
    for v in values[period:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out


def _get_assets(qx) -> List[str]:
    names: List[str] = []
    try:
        instruments = awaitable_or_value(qx.get_instruments)
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


def awaitable_or_value(fn):
    """Helper to support qx methods that may or may not be async in some environments."""
    try:
        res = fn()
        if hasattr(res, "__await__"):
            import asyncio as _aio
            return _aio.get_event_loop().run_until_complete(res)  # pragma: no cover
        return res
    except Exception:
        return []

# --- Async-safe helpers with timeouts/retries ---
async def _safe_get_instruments(qx, timeout: float = 5.0) -> List:
    try:
        return await asyncio.wait_for(qx.get_instruments(), timeout=timeout)
    except Exception:
        return []


async def _safe_get_candles(
    qx,
    asset: str,
    tf: int,
    bars_need: int,
    now_ts: float,
    timeout: float = 8.0,
    tries: int = 2,
    max_total_seconds: float | None = None,
) -> List[dict]:
    """Fetch candles with robust fallbacks to reduce 0-candle cases and recover faster.
    Also tries lower timeframes (30s/15s) and aggregates back to 60s when needed.
    """
    import time as _t
    start = _t.time()
    delay = 0.4
    try:
        import math
        aligned = math.floor(now_ts / max(1, tf)) * max(1, tf)
    except Exception:
        aligned = now_ts
    # Compute ADX safely if needed for trend filter
    # (We compute at desired tf; if ADX blocking is ON we may skip asset early.)
    # More end_ts candidates (live and historical)
    end_candidates = [
        now_ts,
        aligned,
        aligned - tf * 60,
        aligned - tf * 210,
        aligned - tf * 600,
        aligned - tf * 1200,
        aligned - tf * 3600,
    ]
    # Smaller window sizes for problematic assets
    win_sizes = [tf * bars_need, tf * 600, tf * 300, tf * 180, tf * 120, tf * 90]
    for k in range(max(1, int(tries))):
        for end_ts in end_candidates:
            for win in win_sizes:
                try:
                    if max_total_seconds is not None and (_t.time() - start) > max_total_seconds:
                        return []
                    c = await asyncio.wait_for(qx.get_candles(asset, end_ts, win, tf), timeout=timeout)
                    if c:
                        return c
                except Exception:
                    pass
        if max_total_seconds is not None and (_t.time() - start) > max_total_seconds:
            return []
        await asyncio.sleep(delay)
        delay = min(3.5, delay * 1.8)

    # If main tf failed (likely 60s), try multi-TF fallback and aggregate
    try:
        # Prefer 30s then 15s candles
        for tf_low in (30, 15):
            low_bars_need = max(bars_need * (tf // tf_low), int(bars_need * tf / tf_low)) if tf_low < tf else bars_need
            try:
                low = await asyncio.wait_for(qx.get_candles(asset, aligned, tf_low * low_bars_need, tf_low), timeout=timeout)
            except Exception:
                low = []
            if not low:
                continue
            # Aggregate low timeframe candles to desired tf (e.g., 30s->60s or 15s->60s)
            agg: List[dict] = []
            bucket = None
            bucket_start = None
            for cd in low:
                try:
                    ts = int(cd.get("time") or cd.get("ts") or 0)
                    if bucket_start is None:
                        bucket_start = (ts // tf) * tf
                    # Start new bucket if crossed
                    if ts >= (bucket_start + tf):
                        if bucket:
                            agg.append(bucket)
                        bucket_start = (ts // tf) * tf
                        bucket = None
                    if bucket is None:
                        bucket = {
                            "time": bucket_start,
                            "open": float(cd["open"]),
                            "high": float(cd["high"]),
                            "low": float(cd["low"]),
                            "close": float(cd["close"]),
                        }
                    else:
                        bucket["high"] = max(bucket["high"], float(cd["high"]))
                        bucket["low"] = min(bucket["low"], float(cd["low"]))
                        bucket["close"] = float(cd["close"])  # last close
                except Exception:
                    continue
            if bucket:
                agg.append(bucket)
            if len(agg) >= max(60, bars_need):
                return agg[-max(60, bars_need):]
    except Exception:
        pass

    return []


# --- Connection health (best-effort, local to S18) ---
async def _ensure_ws_connection_local(qx, debug: bool = False) -> None:
    try:
        try:
            from pyquotex import global_value as _gv  # type: ignore
        except Exception:
            _gv = None
        gv_conn = None
        gv_err = False
        try:
            if _gv is not None:
                gv_conn = getattr(_gv, 'check_websocket_if_connect', None)
                gv_err = bool(getattr(_gv, 'check_websocket_if_error', False))
        except Exception:
            pass
        if gv_conn != 1 or gv_err:
            if debug:
                print("[S18] WS unhealthy; attempting reconnect...")
            try:
                await qx.reconnect()
            except Exception:
                pass
            try:
                await qx.connect()
            except Exception:
                pass
            # Clear sticky flags if possible
            try:
                if _gv is not None:
                    _gv.check_websocket_if_error = False
                    _gv.websocket_error_reason = None
            except Exception:
                pass
    except Exception:
        pass


async def _safe_calc_ema(qx, asset: str, period: int, tf: int, timeout: float = 4.0) -> List[float]:
    try:
        resp = await asyncio.wait_for(qx.calculate_indicator(asset, "EMA", {"period": period}, timeframe=tf), timeout=timeout)
        return [float(x) for x in (resp.get("ema", []) if isinstance(resp, dict) else [])]
    except Exception:
        return []

async def _safe_calc_adx(qx, asset: str, period: int, tf: int, timeout: float = 4.0) -> List[float]:
    try:
        params = {"period": period}
        resp = await asyncio.wait_for(qx.calculate_indicator(asset, "ADX", params, timeframe=tf), timeout=timeout)
        arr = resp.get("adx", []) if isinstance(resp, dict) else []
        return [float(x) for x in arr]
    except Exception:
        return []


async def _payout_for(qx, asset: str, expiry_min: int) -> float:
    try:
        keys: List[str] = ["1", "60"] if expiry_min <= 1 else [str(expiry_min)]
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
    except Exception:
        pass
    return 0.0


def _compute_bears_power(closes: List[float], lows: List[float], period: int = 55) -> List[float]:
    """Elder Bears Power: Low - EMA(Closes, period)."""
    if not closes or not lows or len(closes) != len(lows) or len(closes) < period:
        return []
    ema_c = _ema(closes, period)  # length = len(closes) - period + 1
    out: List[float] = []
    start = period - 1
    for i in range(start, len(closes)):
        j = i - start
        out.append(lows[i] - ema_c[j])
    return out  # aligned to closes[start:]


# --- Core Confluence Checks ---

def _candle_color(c: dict) -> str:
    try:
        return "green" if float(c["close"]) > float(c["open"]) else ("red" if float(c["close"]) < float(c["open"]) else "doji")
    except Exception:
        return "doji"


def _psar_position(psar_last: float, high_last: float, low_last: float) -> str:
    try:
        if psar_last <= low_last:
            return "below"
        if psar_last >= high_last:
            return "above"
    except Exception:
        pass
    return "mid"


async def find_first_signal_s18(
    qx,
    cfg: S18Config,
    min_payout: float,
    debug: bool = False,
) -> Tuple[Optional[str], Optional[str]]:
    """Continuously scan until all 4 indicators align on 60s timeframe.
    Returns first (asset, direction). 1-minute expiry intended.
    """
    # Precompute minute-sync control
    def _in_entry_window(ts: float) -> bool:
        try:
            sec = int(ts % 60)
            return cfg.entry_window_start <= sec <= cfg.entry_window_end
        except Exception:
            return True

    while True:
        now_ts = time.time()

        # Optional minute-sync: wait for early-in-minute window
        if cfg.minute_sync and not _in_entry_window(now_ts):
            await asyncio.sleep(0.25)
            continue

        # Ensure WS connection is healthy before data fetch
        await _ensure_ws_connection_local(qx, debug)

        # 1) Gather eligible assets by payout and IST hour
        instruments = await _safe_get_instruments(qx, timeout=6.0)
        assets: List[str] = []
        for i in instruments:
            try:
                if isinstance(i, dict):
                    nm = i.get("symbol") or i.get("asset") or i.get("name")
                elif isinstance(i, (list, tuple)) and len(i) >= 2:
                    nm = i[1]
                else:
                    nm = str(i)
                if not nm:
                    continue
                # Per-asset cooldown
                if s18_is_on_cooldown(nm, now_ts):
                    continue
                # Tradability quick check when available
                try:
                    if hasattr(qx, "is_tradable"):
                        tradable = await asyncio.wait_for(qx.is_tradable(nm), timeout=2.0)
                        if not tradable:
                            # cooldown 30s if not tradable
                            S18_COOLDOWNS[nm] = now_ts + 30.0
                            continue
                except Exception:
                    pass
                p = await _payout_for(qx, nm, 1)
                if p < float(min_payout):
                    continue
                if cfg.allowed_hours_ist is not None:
                    h = _ist_hour(now_ts)
                    if h not in cfg.allowed_hours_ist:
                        continue
                assets.append(nm)
            except Exception:
                continue

        if debug:
            try:
                print(f"[S18] Eligible by payout/hours: {len(assets)}")
            except Exception:
                pass

        # 2) Analyze each asset on 60s timeframe with per-asset timeout
        zero_count = 0
        total_scanned = 0
        zero_assets: list[str] = []
        import time as _t
        last_asset_print_time = _t.time()
        consecutive_zero_any = 0

        for asset in assets:
            try:
                # Skip blacklisted assets
                if s18_is_blacklisted(asset, now_ts):
                    continue
                total_scanned += 1

                # Per-asset timeout detection (15 seconds since last asset print)
                current_time = _t.time()
                if current_time - last_asset_print_time > 15.0:
                    print(f"[S18][TIMEOUT] Asset scan stuck on {asset}, reconnecting and continuing...")
                    await _ensure_ws_connection_local(qx, debug=True)
                    # Skip this problematic asset and continue
                    S18_COOLDOWNS[asset] = now_ts + 60.0  # 1-minute cooldown
                    last_asset_print_time = current_time
                    consecutive_zero_any = 0
                    continue

                # Fetch sufficient candles (for EMA55 on Bears Power) with timeout
                tf = int(cfg.timeframe_s)
                bars_need = max(cfg.min_bars, 120)
                candles = await _safe_get_candles(
                    qx, asset, tf, bars_need, now_ts,
                    timeout=8.0, tries=3, max_total_seconds=10.0,
                )
                if not candles or len(candles) < max(cfg.min_bars, 60):
                    n = len(candles) if isinstance(candles, list) else 0
                    if n == 0:
                        zero_count += 1
                        zero_assets.append(asset)
                        # Increase per-asset fail count and maybe blacklist
                        try:
                            S18_FAIL_COUNTS[asset] = S18_FAIL_COUNTS.get(asset, 0) + 1
                            fc = S18_FAIL_COUNTS[asset]
                            # If the same asset hits 0-candles repeatedly, force a reconnect (throttled)
                            if fc >= 2:
                                try:
                                    global S18_LAST_RECONNECT_TS
                                    last = float(S18_LAST_RECONNECT_TS)
                                except Exception:
                                    last = 0.0
                                import time as _t
                                if (_t.time() - last) > 8.0:
                                    print(f"[S18][RECOVERY] {asset}: repeated zero-candle ({fc}). Reconnecting WS and continuing...")
                                    await _ensure_ws_connection_local(qx, debug=True)
                                    try:
                                        S18_LAST_RECONNECT_TS = _t.time()
                                    except Exception:
                                        pass
                            if fc >= 3:
                                # Blacklist 10 minutes (storm hardening)
                                S18_BLACKLIST[asset] = now_ts + 600.0
                        except Exception:
                            pass
                    if debug:
                        print(f"[S18] {asset}: insufficient candles ({n})")
                    # Apply short cooldown 20s for assets with no candles
                    try:
                        if n == 0:
                            S18_COOLDOWNS[asset] = now_ts + 20.0
                    except Exception:
                        pass
                    # Mid-pass zero streak detection and WS reconnect (throttled)
                    try:
                        consecutive_zero_any += 1
                        import time as _t
                        try:
                            global S18_LAST_RECONNECT_TS
                            last2 = float(S18_LAST_RECONNECT_TS)
                        except Exception:
                            last2 = 0.0
                        if consecutive_zero_any >= 2 and (_t.time() - last2) > 8.0:
                            print("[S18][RECOVERY] Mid-pass zero streak >=2. Reconnecting WS and continuing...")
                            await _ensure_ws_connection_local(qx, debug=True)
                            try:
                                S18_LAST_RECONNECT_TS = _t.time()
                            except Exception:
                                pass
                    except Exception:
                        pass
                    continue
                # Reset fail count on success
                try:
                    if asset in S18_FAIL_COUNTS:
                        del S18_FAIL_COUNTS[asset]
                except Exception:
                    pass

                closes = [float(c["close"]) for c in candles]
                highs = [float(c["high"]) for c in candles]
                lows = [float(c["low"]) for c in candles]

                # EMAs (7 and 3)
                ema7 = await _safe_calc_ema(qx, asset, 7, tf, timeout=4.0)
                if not ema7:
                    ema7 = _ema(closes, 7)
                    ema7 = ([None] * (len(closes) - len(ema7))) + ema7 if ema7 else []  # pad to align
                ema3 = await _safe_calc_ema(qx, asset, 3, tf, timeout=4.0)
                if not ema3:
                    ema3 = _ema(closes, 3)
                    ema3 = ([None] * (len(closes) - len(ema3))) + ema3 if ema3 else []

                if not ema7 or not ema3 or len(ema7) < 3 or len(ema3) < 3:
                    if debug:
                        print(f"[S18] {asset}: EMA data unavailable")
                    continue

                # ADX filter (trend strength)
                adx_ok = True
                try:
                    if float(cfg.adx_skip_below) > 0:
                        adx_vals = await _safe_calc_adx(qx, asset, int(cfg.adx_period), tf, timeout=4.0)
                        if adx_vals:
                            adx_last = float(adx_vals[-1])
                            if adx_last < float(cfg.adx_skip_below):
                                if debug:
                                    print(f"[S18] {asset}: ADX too low ({adx_last:.1f}) – skip")
                                continue
                            if bool(cfg.adx_skip_yellow) and (float(cfg.adx_yellow_min) <= adx_last <= float(cfg.adx_yellow_max)):
                                if debug:
                                    print(f"[S18] {asset}: ADX yellow zone ({adx_last:.1f}) – skip")
                                continue
                except Exception:
                    pass

                # PSAR (step=0.03, max=0.6)
                psar = []
                if compute_psar is not None:
                    try:
                        psar = compute_psar(highs, lows, step=0.03, max_step=0.6)
                    except Exception:
                        psar = []
                if not psar or len(psar) != len(closes):
                    if debug:
                        print(f"[S18] {asset}: PSAR unavailable")
                    continue

                # Bears Power (period 55): Low - EMA55(Close)
                bears_all = _compute_bears_power(closes, lows, period=55)
                if not bears_all or len(bears_all) < 3:
                    if debug:
                        print(f"[S18] {asset}: Bears Power insufficient")
                    continue
                # Align bears to candles (bears_all aligns to closes[54:])
                # Create convenience last values
                b_last = float(bears_all[-1])
                b_prev = float(bears_all[-2])
                b_prev2 = float(bears_all[-3]) if len(bears_all) >= 3 else b_prev

                # Prepare recent EMA values (aligned as end-only)
                ema7c, ema7p = float(ema7[-1]), float(ema7[-2])  # slow (green)
                ema3c, ema3p = float(ema3[-1]), float(ema3[-2])  # fast (red)
                # Crossover defined as fast (EMA3) crossing slow (EMA7)
                diff_now = ema3c - ema7c
                diff_prev = ema3p - ema7p
                fast_slope = ema3c - ema3p

                # PSAR position
                psar_pos_now = _psar_position(float(psar[-1]), highs[-1], lows[-1])
                psar_pos_prev = _psar_position(float(psar[-2]), highs[-2], lows[-2])

                # Candle color
                c_last = candles[-1]
                color = _candle_color(c_last)

                # Confluence rules (with configurable quality gates)
                price_ref = max(1e-6, abs(ema7c))
                # Strength gates (config-driven; default mostly off except PSAR/Bears strictness)
                ema_gap_ok = (abs(diff_now) / price_ref) >= float(cfg.min_ema_gap_ratio)
                fast_slope_ok = (abs(fast_slope) / price_ref) >= float(cfg.min_fast_slope_ratio)
                bears_delta_ok = (abs(b_last - b_prev) / price_ref) >= float(cfg.min_bears_delta_ratio)
                # Candle body ratio
                rng = max(1e-9, (float(c_last.get("high")) - float(c_last.get("low"))))
                body = abs(float(c_last.get("close")) - float(c_last.get("open")))
                body_ok = (body / rng) >= float(cfg.min_body_ratio)

                # PSAR confirmation bars
                if int(cfg.psar_confirm_bars) >= 2:
                    psar_up = (psar_pos_now == "below" and psar_pos_prev == "below")
                    psar_dn = (psar_pos_now == "above" and psar_pos_prev == "above")
                else:
                    psar_up = (psar_pos_now == "below")
                    psar_dn = (psar_pos_now == "above")

                # Bears monotonic trend
                if bool(cfg.bears_monotonic_3):
                    bears_up = (b_last > b_prev) and (b_prev >= b_prev2)
                    bears_dn = (b_last < b_prev) and (b_prev <= b_prev2)
                else:
                    bears_up = (b_last > b_prev)
                    bears_dn = (b_last < b_prev)

                # Final CALL/PUT rules with quality gates
                cross_up = (diff_prev <= 0.0 and diff_now > 0.0)
                cross_dn = (diff_prev >= 0.0 and diff_now < 0.0)
                call_ok = (cross_up and psar_up and bears_up and (color == "green")
                           and (ema_gap_ok or cfg.min_ema_gap_ratio == 0.0)
                           and (fast_slope_ok or cfg.min_fast_slope_ratio == 0.0)
                           and (bears_delta_ok or cfg.min_bears_delta_ratio == 0.0)
                           and body_ok)
                put_ok = (cross_dn and psar_dn and bears_dn and (color == "red")
                          and (ema_gap_ok or cfg.min_ema_gap_ratio == 0.0)
                          and (fast_slope_ok or cfg.min_fast_slope_ratio == 0.0)
                          and (bears_delta_ok or cfg.min_bears_delta_ratio == 0.0)
                          and body_ok)

                direction: Optional[str] = None
                if call_ok:
                    direction = "call"
                elif put_ok:
                    direction = "put"

                if debug:
                    try:
                        cross_state = "up" if cross_up else ("down" if cross_dn else "none")
                        print(
                            f"[S18] {asset} cross={cross_state} ema7={ema7c:.6f} ema3={ema3c:.6f} "
                            f"fast_slope={fast_slope:.6g} psar={psar_pos_now} bears={b_last:.6g} -> "
                            f"call={call_ok} put={put_ok} candle={color}"
                        )
                    except Exception:
                        pass

                if direction:
                    payout = await _payout_for(qx, asset, 1)
                    _log_signal(
                        asset=asset,
                        direction=direction,
                        payout=payout,
                        confluence=True,
                        ema7=ema7c,
                        ema3=ema3c,
                        ema_cross=("up" if cross_up else ("down" if cross_dn else "none")),
                        ema7_slope=fast_slope,
                        psar_pos=psar_pos_now,
                        bears=b_last,
                        bears_slope=(b_last - b_prev),
                        candle=color,
                        note="confluence",
                    )
                    return asset, direction
            except Exception as e:
                if debug:
                    try:
                        print(f"[S18] {asset} error: {e}")
                    except Exception:
                        pass
                continue

        # Pass-level diagnostics and recovery
        try:
            if total_scanned > 0:
                zero_pct = (zero_count / float(total_scanned)) * 100.0
                # Lower threshold: if >=2 zero-candle assets OR >=20% zero rate
                if (zero_count >= 2) or (zero_pct >= 20.0 and zero_count >= 1):
                    print(
                        f"[S18][RECOVERY] High zero-candle rate: {zero_count}/{total_scanned}"
                    )
                    print(
                        f"[S18][RECOVERY] Reconnecting WS (zero={zero_count}, "
                        f"total={total_scanned}, pct={zero_pct:.0f}%)"
                    )
                    await _ensure_ws_connection_local(qx, debug=True)
                    # Log problematic assets
                    if zero_assets:
                        preview = ", ".join(zero_assets[:8])
                        tail = "..." if len(zero_assets) > 8 else ""
                        print(f"[S18][RECOVERY] Zero-candle assets: {preview}{tail}")
            if debug and total_scanned > 0:
                ok = total_scanned - zero_count
                print(f"[S18][PASS] scanned={total_scanned} ok={ok} zero={zero_count}")
        except Exception:
            pass

        # No signal this pass; wait and retry
        try:
            await asyncio.sleep(float(max(0.2, cfg.scan_interval_s)))
        except Exception:
            # Fallback: small sleep
            await asyncio.sleep(1.0)

    # Unreachable (loop returns on signal)
    return None, None

