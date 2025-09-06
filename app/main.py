"""Main trading application entry.
Corrupted Strategy 19 snippet removed here; real implementation lives inside async main().
"""

from __future__ import annotations
import os, sys, asyncio, math, statistics, random, time, json, csv
from typing import List, Tuple, Optional
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except Exception:  # Python <3.9 fallback (not expected here)
    ZoneInfo = None  # type: ignore

# Banner (restored after earlier corruption)
BANNER = """\n=== Quotex Multi-Strategy Bot (Strategy 19 Telegram + Others) ===\n"""
try:
    from colorama import Fore, Style, init as colorama_init
except Exception:
    class _Dummy:  # fallback
        RESET_ALL = ""
    class _ForeDummy:
        RED=GREEN=YELLOW=CYAN=MAGENTA=RESET=""
    Fore = _ForeDummy()
    class _StyleDummy:
        RESET_ALL=""
    Style=_StyleDummy()
    def colorama_init(*a, **k):
        pass


def _zigzag_last_direction(highs: List[float], lows: List[float], deviation: float = 0.5, depth: int = 13, backstep: int = 3) -> str | None:
    """Return 'up' if last confirmed pivot is a Low, 'down' if last is a High else None.
    Simplified ZigZag direction used by multiple strategies.
    """
    piv = []
    n = len(highs)
    if n < depth * 2 + 1:
        return None
    for i in range(depth, n - depth):
        hwin = highs[i - depth:i + depth + 1]
        lwin = lows[i - depth:i + depth + 1]
        if highs[i] == max(hwin):
            if piv and i - piv[-1][0] <= backstep and piv[-1][2] == 'H':
                if highs[i] > piv[-1][1]:
                    piv[-1] = (i, highs[i], 'H')
            else:
                piv.append((i, highs[i], 'H'))
        if lows[i] == min(lwin):
            if piv and i - piv[-1][0] <= backstep and piv[-1][2] == 'L':
                if lows[i] < piv[-1][1]:
                    piv[-1] = (i, lows[i], 'L')
            else:
                piv.append((i, lows[i], 'L'))
    # deviation filter similar to pivot function
    filt: List[tuple] = []
    for p in piv:
        if not filt:
            filt.append(p)
            continue
        last = filt[-1]
        base = last[1]
        if base == 0:
            continue
        ch = abs((p[1] - base) / base) * 100
        if ch >= deviation:
            filt.append(p)
        else:
            if p[2] == last[2]:
                if (p[2] == 'H' and p[1] > last[1]) or (p[2] == 'L' and p[1] < last[1]):
                    filt[-1] = p
    if len(filt) < 1:
        return None
    last_type = filt[-1][2]
    return 'up' if last_type == 'L' else ('down' if last_type == 'H' else None)


def _sma(values: List[float], period: int) -> List[float]:
    if len(values) < period:
        return []
    out: List[float] = []
    s = sum(values[:period])
    out.append(s / period)
    for i in range(period, len(values)):
        s += values[i] - values[i - 1]
        out.append(s / period)
    return out


def _ema(values: List[float], period: int) -> List[float]:
    """Compute Exponential Moving Average list for given period.
    Returns list aligned to input sequence length - first EMA seed uses simple average.
    """
    if period <= 0 or not values:
        return []
    k = 2 / (period + 1)
    if len(values) < period:
        # Fallback: progressive EMA without initial SMA window
        ema: List[float] = []
        prev = values[0]
        ema.append(prev)
        for v in values[1:]:
            prev = (v - prev) * k + prev
            ema.append(prev)
        return ema
    ema: List[float] = []
    prev = sum(values[:period]) / period
    ema.append(prev)
    for v in values[period:]:
        prev = (v - prev) * k + prev
        ema.append(prev)
    return ema


async def _s11_finalize_trade(trade_no: int, asset: str, direction: str, result: str,
                               pnl: float, qx, expiry_min: int) -> None:
    try:
        if not S11_ADAPTIVE_ENABLE:
            return
        st = _s11_adaptive_load_state()
        feat = S11_OPEN_TRADES.pop(trade_no, None)
        if not feat:
            return
        # Fetch latest candles to compute post-signal metrics
        candles = await _get_candles_safe(qx, asset, 60, 60, ctx="s11_post")
        next1_ret = next3_ret = None
        vol_jump = False
        notes = ""
        if candles and len(candles) >= 5:
            # Find index of signal candle by timestamp proximity
            sig_ts = feat.get("ts")
            idx = max(0, len(candles) - 1)
            for i in range(len(candles)):
                if int(candles[i].get("time") or 0) == sig_ts:
                    idx = i
                    break
            closes = [float(c["close"]) for c in candles]
            if idx + 1 < len(closes):
                next1_ret = (closes[idx + 1] - closes[idx]) / max(1e-9, closes[idx])
            if idx + 3 < len(closes):
                next3_ret = (closes[idx + 3] - closes[idx]) / max(1e-9, closes[idx])
            # Vol jump via ATR median vs latest
            highs = [float(c["high"]) for c in candles]
            lows = [float(c["low"]) for c in candles]
            atr14 = _atr(highs, lows, closes, 14)
            if atr14 and len(atr14) >= 10:
                recent = [float(x) for x in atr14[-10:]]
                med = sorted(recent)[len(recent)//2]
                vol_jump = float(atr14[-1]) > 1.6 * med
        # Update state stats
        st["completed"] = int(st.get("completed", 0)) + 1
        st["wins"] = int(st.get("wins", 0)) + (1 if result == "WIN" else 0)
        arr = st.setdefault("recent_buy" if direction == "call" else "recent_sell", [])
        arr.append(result == "WIN")
        if len(arr) > 100:
            del arr[0:len(arr)-100]
        a = st.setdefault("assets", {}).setdefault(asset, {"wins": 0, "total": 0})
        a["total"] += 1
        if result == "WIN":
            a["wins"] += 1
        hour = feat.get("hour_ist") or int(datetime.now(ZoneInfo("Asia/Kolkata")).hour)
        h = st.setdefault("hours", {}).setdefault(str(hour), {"wins": 0, "total": 0})
        h["total"] += 1
        if result == "WIN":
            h["wins"] += 1
        # Adjust thresholds
        _s11_adaptive_adjust_thresholds(st)
        # Persist
        _s11_adaptive_save_state(st)
        # Log CSV row
        _s11_adaptive_log_header()
        with open(S11_ADAPTIVE_LOG_PATH, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                datetime.now().isoformat(timespec="seconds"), feat.get("asset"), direction, feat.get("hour_ist"),
                feat.get("k_prev"), feat.get("d_prev"), feat.get("k_cur"), feat.get("d_cur"),
                feat.get("ema2_prev"), feat.get("ema7_prev"), feat.get("ema2_cur"), feat.get("ema7_cur"),
                feat.get("ema_sep_pct"), feat.get("atr_pct"), feat.get("rsi_last"), feat.get("adx_last"),
                result, float(pnl or 0.0), next1_ret, next3_ret, int(bool(vol_jump)), notes,
            ])
        # Periodic report
        if int(st.get("completed", 0)) % max(1, int(S11_ADAPTIVE_REPORT_EVERY)) == 0:
            dyn = st.get("dynamic", {})
            wins = int(st.get("wins", 0))
            tot = int(st.get("completed", 0))
            wr = (wins / tot) if tot else 0.0
            print(f"[S11 Adaptive] Trades={tot} WR={wr:.2%} DynOS={dyn.get('os')} DynOB={dyn.get('ob')}")
    except Exception as e:
        print(f"[S11 Adaptive] finalize error: {e}")




# S11 hours filter disabled; always allow all hours
# When False, bot will place real orders per account selection (Demo/Live)
S11_DRY_RUN: bool = False



async def _get_candles_safe(qx, asset: str, tf: int, bars_hint: int, ctx: str = "") -> list:
    """Fetch candles with basic error handling and debug logging.
    bars_hint: desired count threshold to consider data usable
    """
    try:
        candles = await asyncio.wait_for(
            qx.get_candles(asset, time.time(), tf * max(60, bars_hint * 2), tf),
            timeout=S11_GET_CANDLES_TIMEOUT_SEC,
        )
    except Exception as e:
        msg = str(e) if e else ""
        _dbg(f"{asset} tf={tf} {ctx}: get_candles exception: {msg}")
        # On connection-closed style errors, try a soft reconnect once per window
        try:
            import time as _t
            low = msg.lower()
            if ("closed" in low or "connection" in low):
                global _LAST_RECONNECT_TS
                now = _t.time()
                if now - float(_LAST_RECONNECT_TS or 0.0) >= float(S11_RECONNECT_MIN_GAP_SEC):
                    try:
                        await qx.reconnect()
                        await qx.connect()
                    except Exception:
                        pass
                    _LAST_RECONNECT_TS = now
        except Exception:
            pass
        # Mark temporarily bad for S11 decision path only
        try:
            _s11_mark_bad(asset, ctx)
        except Exception:
            pass
        return []
    n = len(candles) if candles else 0
    if not candles or n < bars_hint:
        try:
            # Suppress noisy insufficient logs for S11 during quarantine
            if not (ctx.startswith("s11") and asset in S11_BAD_ASSETS):
                _dbg(f"{asset} tf={tf} {ctx}: insufficient candles ({n}<{bars_hint})")
        except Exception:
            _dbg(f"{asset} tf={tf} {ctx}: insufficient candles ({n}<{bars_hint})")
        return candles or []
    # Clear bad markers if data recovered
    try:
        _s11_clear_bad(asset)
    except Exception:
        pass
    return candles


def _rsi(values: List[float], period: int = 14) -> List[float]:
    if len(values) < period + 1:
        return []
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(values)):
        ch = values[i] - values[i - 1]
        gains.append(max(0.0, ch))
        losses.append(max(0.0, -ch))
    avg_gain = _rma(gains, period)
    avg_loss = _rma(losses, period)
    if not avg_gain or not avg_loss:
        return []
    out: List[float] = []
    for g, l in zip(avg_gain[-len(avg_loss):], avg_loss):
        if l == 0:
            out.append(100.0)
        else:
            rs = g / l
            out.append(100.0 - (100.0 / (1.0 + rs)))
    # Align output length to input minus period
    return out

# Cache for best timeframe per asset for Strategy 7
_S7_TF_CACHE: dict[str, int] = {}


async def run_verify_mode_if_requested(qx) -> None:
    """Interactive verify mode: ask user if they want to verify candles first.
    Runs after successful connection and before strategy selection.
    """
    try:
        ans = input("\nDo you want to verify candle data first? (Y/N): ").strip().lower()
        if ans not in ("y", "yes"):
            return
        while True:
            asset = input("Enter asset (e.g., EURUSD, ADAUSD_otc): ").strip()
            if not asset:
                print("Please enter a valid asset symbol.")
                continue
            try:
                tf_raw = input("Enter timeframe in seconds (default 60): ").strip()
                timeframe = int(tf_raw) if tf_raw else 60
            except Exception:
                print("Invalid timeframe, using 60 seconds.")
                timeframe = 60
            try:
                bars_raw = input("Enter number of bars to fetch (default 50): ").strip()
                bars = int(bars_raw) if bars_raw else 50
            except Exception:
                print("Invalid bars, using 50.")
                bars = 50

            print("\nInstructions: Open the Quotex chart for the same asset and timeframe."
                  " Compare the last candles' OHLC and timestamps with the output below.")
            await verify_candles(qx, asset, timeframe=timeframe, bars=bars)

            again = input("\nVerify another asset/timeframe? (Y to continue, any other key to proceed to trading): ").strip().lower()
            if again not in ("y", "yes"):
                print("Exiting Verify Mode. Proceeding to trading setup...\n")
                break
    except Exception as e:
        print(f"[Verify Mode] Error: {e}")
        # Allow user to retry or exit
        try:
            retry = input("Retry Verify Mode? (Y/N): ").strip().lower()
            if retry in ("y", "yes"):
                return await run_verify_mode_if_requested(qx)
        except Exception:
            pass
        print("Skipping Verify Mode and continuing to trading.")

async def verify_candles(qx, asset: str, timeframe: int = 60, bars: int = 50) -> None:
    """Fetch candles via API and print a compact, human-verifiable OHLC table.
    Use this to cross-check Quotex UI vs bot data. Does not place trades.
    """
    try:
        print(f"\n[Candle Verify] Asset={asset} TF={timeframe}s Bars={bars}")
        candles = await _get_candles_safe(qx, asset, timeframe, bars, ctx="verify")
        n = len(candles) if candles else 0
        print(f"[Candle Verify] Retrieved: {n} candles")
        if not candles:
            print("[Candle Verify] No data returned.")
            return
        # Show last up to 10 candles in readable format
        tail = candles[-min(10, n):]
        print("[Candle Verify] Last candles (most recent last):")
        for c in tail:
            # Quotex returns epoch-like time bases internally; if a timestamp field exists, print it.
            # If not available, derive approximate timestamp using current time and index distance.
            ts = c.get("time") or c.get("from") or None
            o = float(c.get("open"))
            hi = float(c.get("high"))
            lo = float(c.get("low"))
            cl = float(c.get("close"))
            if ts:
                try:
                    # tz-aware: convert epoch to IST (UTC+5:30)
                    from datetime import datetime, timezone
                    ts_int = int(ts)
                    if ts_int > 10**12:
                        ts_int //= 1000  # ms to s
                    dt_utc = datetime.fromtimestamp(ts_int, tz=timezone.utc)
                    dt_ist = dt_utc.astimezone(ZoneInfo("Asia/Kolkata"))
                    tstr = dt_ist.strftime("%Y-%m-%d %H:%M:%S IST")
                except Exception:
                    tstr = str(ts)
            else:
                tstr = "(no_ts)"
            print(f"  {tstr}  O={o:.6f} H={hi:.6f} L={lo:.6f} C={cl:.6f}")
        # Quick consistency checks
        bad = 0
        for c in tail:
            try:
                o = float(c["open"])
                hi = float(c["high"])
                lo = float(c["low"])
                cl = float(c["close"])
                ok_range = (hi >= max(o, cl)) and (lo <= min(o, cl)) and (hi >= lo)
                if not ok_range:
                    bad += 1
            except Exception:
                bad += 1
        if bad:
            print(f"[Candle Verify] Inconsistencies detected in last {len(tail)} candles: {bad}")
        else:
            print("[Candle Verify] OHLC range checks OK for recent candles.")
        print("[Candle Verify] Cross-check these OHLC values against the Quotex chart for the same TF.")
    except Exception as e:
        print(f"[Candle Verify] Error: {e}")

async def _s7_best_tf(qx, asset: str) -> int | None:
    # Try 30s first; if insufficient, use 60s. Cache per asset.
    if asset in _S7_TF_CACHE:
        return _S7_TF_CACHE[asset]
    for tf in (30, 60):
        try:
            candles = await qx.get_candles(asset, time.time(), tf * 200, tf)
            if candles and len(candles) >= 30:
                _S7_TF_CACHE[asset] = tf
                return tf
        except Exception:
            continue
    _S7_TF_CACHE[asset] = 60
    return 60


def _stoch_slow(closes: List[float], highs: List[float], lows: List[float], k_period: int = 14, smooth_k: int = 3, smooth_d: int = 3) -> tuple[List[float], List[float]]:
    if len(closes) < k_period + smooth_k + smooth_d:
        return [], []
    fast_k: List[float] = []
    for i in range(k_period - 1, len(closes)):
        hh = max(highs[i - k_period + 1:i + 1])
        ll = min(lows[i - k_period + 1:i + 1])
        k = 50.0 if hh == ll else (closes[i] - ll) / (hh - ll) * 100
        fast_k.append(k)
    slow_k = _sma(fast_k, smooth_k)
    slow_d = _sma(slow_k, smooth_d) if slow_k else []
    return slow_k, slow_d


async def decide_signal_and_direction(qx, asset: str, strategy: int, timeframe: int = 60) -> Tuple[bool, str]:
    """Return (has_signal, direction)."""
    try:
        if strategy == 1:  # EMA(5/20) crossover (original)
            e5 = await qx.calculate_indicator(asset, "EMA", {"period": 5}, timeframe=timeframe)
            e20 = await qx.calculate_indicator(asset, "EMA", {"period": 20}, timeframe=timeframe)
            ema5 = e5.get("ema", [])
            ema20 = e20.get("ema", [])
            if len(ema5) < 2 or len(ema20) < 2:
                return False, "call"
            n = min(len(ema5), len(ema20))
            ema5, ema20 = ema5[-n:], ema20[-n:]
            prev_up = ema5[-2] > ema20[-2]
            now_up = ema5[-1] > ema20[-1]
            if not prev_up and now_up:
                return True, "call"
            if prev_up and not now_up:
                return True, "put"
            return False, "call" if now_up else "put"
        elif strategy == 6:  # EMA 5/20 Pro: Trend + Slope + RSI + ATR + Body
            # Use 1m timeframe for stability
            tf = 60
            candles = await _get_candles_safe(qx, asset, tf, 60, ctx="s6")
            if not candles or len(candles) < 60:
                _dbg(f"{asset} s6: no/low candles on 60s")
                return False, "call"
            closes = [float(c["close"]) for c in candles]
            highs = [float(c["high"]) for c in candles]
            lows = [float(c["low"]) for c in candles]
            opens = [float(c["open"]) for c in candles]

            # EMAs
            e5 = await qx.calculate_indicator(asset, "EMA", {"period": 5}, timeframe=tf)
            e20 = await qx.calculate_indicator(asset, "EMA", {"period": 20}, timeframe=tf)
            ema5 = e5.get("ema", [])
            ema20 = e20.get("ema", [])
            if len(ema5) < 3 or len(ema20) < 3:
                return False, "call"
            # Trend filter with EMA50
            ema50 = _ema(closes, 50)
            if len(ema50) < 2:
                return False, "call"
            trend_up = closes[-1] > ema50[-1]
            trend_down = closes[-1] < ema50[-1]

            # Crossover + slope (allow cross on either of last 2 closed candles)
            prev_up = ema5[-2] > ema20[-2]
            now_up = ema5[-1] > ema20[-1]
            prev2_up = ema5[-3] > ema20[-3]
            cross_now_up = (not prev_up and now_up)
            cross_now_down = (prev_up and not now_up)
            cross_prev_up = (not prev2_up and ema5[-2] > ema20[-2])
            cross_prev_down = (prev2_up and not (ema5[-2] > ema20[-2]))
            slope_up = ema20[-1] > ema20[-2]
            slope_down = ema20[-1] < ema20[-2]
            cross_up = (cross_now_up or cross_prev_up) and trend_up and slope_up
            cross_down = (cross_now_down or cross_prev_down) and trend_down and slope_down

            # ATR distance (avoid marginal crosses) — relaxed to 0.05 x ATR
            atr14 = _atr(highs, lows, closes, 14)
            if not atr14:
                return False, "call"
            dist_up = (closes[-1] - ema20[-1]) if cross_up else 0.0
            dist_down = (ema20[-1] - closes[-1]) if cross_down else 0.0
            min_dist = 0.05 * atr14[-1]
            dist_ok = (cross_up and dist_up >= min_dist) or (cross_down and dist_down >= min_dist)

            # RSI mid-zone guard
            rsi = await qx.calculate_indicator(asset, "RSI", {"period": 14}, timeframe=tf)
            rvals = rsi.get("rsi", [])
            if not rvals:
                return False, "call"
            rcur = rvals[-1]
            rsi_ok_up = 45 <= rcur <= 70
            rsi_ok_down = 30 <= rcur <= 55

            # Candle quality (body to range) — relaxed to 0.3
            last = candles[-1]
            body = abs(float(last["close"]) - float(last["open"]))
            rng = max(1e-6, float(last["high"]) - float(last["low"]))
            body_ok = (body / rng) >= 0.3

            if cross_up and dist_ok and rsi_ok_up and body_ok:
                return True, "call"
            if cross_down and dist_ok and rsi_ok_down and body_ok:
                return True, "put"
            return False, "call"
        elif strategy == 2:  # RSI
            r = await qx.calculate_indicator(asset, "RSI", {"period": 14}, timeframe=timeframe)
            vals = r.get("rsi", [])
            if not vals:
                return False, "call"
            cur = vals[-1]
            if cur <= 30:
                return True, "call"
            if cur >= 70:
                return True, "put"
            return False, "call" if cur >= 50 else "put"
        elif strategy == 3:  # strong candle body
            candles = await qx.get_candles(asset, time.time(), timeframe * 120, timeframe)
            if not candles:
                return False, "call"
            last = candles[-1]
            body = abs(float(last["close"]) - float(last["open"]))
            rng = max(1e-6, float(last["high"]) - float(last["low"]))
            if body / rng >= 0.6:
                return True, "call" if float(last["close"]) > float(last["open"]) else "put"
            return False, "call"
        elif strategy == 4:  # Strategy 4 exact
            tf = 30  # signal timeframe
            candles = await _get_candles_safe(qx, asset, tf, 30, ctx="s4")
            if not candles or len(candles) < 30:
                _dbg(f"{asset} s4: no/low candles on 30s")
                return False, "call"
            closes = [float(c["close"]) for c in candles]
            highs = [float(c["high"]) for c in candles]
            lows = [float(c["low"]) for c in candles]
            opens = [float(c["open"]) for c in candles]

            # Keltner Middle = EMA(21); ATR(9); Multiplier 2
            mid = _ema(closes, 21)
            if len(mid) < 2:
                return False, "call"
            prev_mid, cur_mid = mid[-2], mid[-1]
            prev_close, cur_close = closes[-2], closes[-1]
            is_green = cur_close > opens[-1]
            is_red = cur_close < opens[-1]
            # allow cross on latest or previous candle (accuracy-preserving window)
            cross_now_up = prev_close < prev_mid and cur_close > cur_mid and is_green
            cross_now_down = prev_close > prev_mid and cur_close < cur_mid and is_red
            prev2_mid = mid[-3] if len(mid) >= 3 else prev_mid
            prev2_close = closes[-3] if len(closes) >= 3 else prev_close
            prev_is_green = prev_close > opens[-2] if len(opens) >= 2 else False
            prev_is_red = prev_close < opens[-2] if len(opens) >= 2 else False
            cross_prev_up = prev2_close < prev2_mid and prev_close > prev_mid and prev_is_green
            cross_prev_down = prev2_close > prev2_mid and prev_close < prev_mid and prev_is_red
            cross_up = cross_now_up or cross_prev_up
            cross_down = cross_now_down or cross_prev_down

            # Slow Stochastic (14,3,3)
            k_slow, d_slow = _stoch_slow(closes, highs, lows, 14, 3, 3)
            if len(k_slow) < 2 or len(d_slow) < 2:
                return False, "call"
            prev_k, cur_k = k_slow[-2], k_slow[-1]
            prev_d, cur_d = d_slow[-2], d_slow[-1]
            bull_stoch = prev_k < prev_d and cur_k > cur_d and min(prev_k, cur_k) <= 20
            bear_stoch = prev_k > prev_d and cur_k < cur_d and max(prev_k, cur_k) >= 80

            # ZigZag Deviation=5 Depth=13 Backstep=3
            zz_dir = _zigzag_last_direction(highs, lows, deviation=0.5, depth=13, backstep=3)

            if cross_up and bull_stoch and zz_dir == "up":
                return True, "call"
            if cross_down and bear_stoch and zz_dir == "down":
                return True, "put"
            return False, "call"
        elif strategy == 7:  # Strategy 7: 30s ZigZag + Keltner(21/ATR9/2) + Stoch(14,3,3) + ZigZag touch
            tf = 30
            best_tf = await _s7_best_tf(qx, asset)
            tf = best_tf or 30
            candles = await _get_candles_safe(qx, asset, tf, 30, ctx="s7")
            if not candles or len(candles) < 30:
                _dbg(f"{asset} s7: no/low candles on {tf}s")
                return False, "call"
            closes = [float(c["close"]) for c in candles]
            highs = [float(c["high"]) for c in candles]
            lows = [float(c["low"]) for c in candles]
            opens = [float(c["open"]) for c in candles]

            mid = _ema(closes, 21)
            if len(mid) < 2:
                return False, "call"
            prev_mid, cur_mid = mid[-2], mid[-1]
            prev_close, cur_close = closes[-2], closes[-1]
            is_green = cur_close > opens[-1]
            is_red = cur_close < opens[-1]
            # allow cross on latest or previous candle
            cross_now_up = prev_close < prev_mid and cur_close > cur_mid and is_green
            cross_now_down = prev_close > prev_mid and cur_close < cur_mid and is_red
            prev2_mid = mid[-3] if len(mid) >= 3 else prev_mid
            prev2_close = closes[-3] if len(closes) >= 3 else prev_close
            prev_is_green = prev_close > (opens[-2] if len(opens) >= 2 else prev_close)
            prev_is_red = prev_close < (opens[-2] if len(opens) >= 2 else prev_close)
            cross_prev_up = prev2_close < prev2_mid and prev_close > prev_mid and prev_is_green
            cross_prev_down = prev2_close > prev2_mid and prev_close < prev_mid and prev_is_red
            cross_up = cross_now_up or cross_prev_up
            cross_down = cross_now_down or cross_prev_down

            k_slow, d_slow = _stoch_slow(closes, highs, lows, 14, 3, 3)
            if len(k_slow) < 2 or len(d_slow) < 2:
                return False, "call"
            prev_k, cur_k = k_slow[-2], k_slow[-1]
            prev_d, cur_d = d_slow[-2], d_slow[-1]
            bull = prev_k < prev_d and cur_k > cur_d and min(prev_k, cur_k) <= 20
            bear = prev_k > prev_d and cur_k < cur_d and max(prev_k, cur_k) >= 80

            # ADX trend strength filter
            adx_vals = _adx(highs, lows, closes, 14)
            if not adx_vals or adx_vals[-1] < 18:
                return False, "call"

            zz_dir = _zigzag_last_direction(highs, lows, deviation=0.5, depth=13, backstep=3)
            last_pivot = _zigzag_last_pivot(highs, lows, deviation=0.5, depth=13, backstep=3)
            # If direction is None due to strict deviation, attempt a softer direction (0.3)
            if zz_dir is None:
                zz_soft = _zigzag_last_direction(highs, lows, deviation=0.3, depth=13, backstep=3)
                if zz_soft in ("up", "down"):
                    zz_dir = zz_soft

            touched = False
            if last_pivot:
                _, piv_price, piv_type = last_pivot
                # touch = current candle high/low reaches pivot price depending on type
                if piv_type == "H":
                    touched = highs[-1] >= piv_price or highs[-2] >= piv_price
                else:
                    touched = lows[-1] <= piv_price or lows[-2] <= piv_price

            # BUY: green candle crosses Keltner mid up, Stoch cross up from oversold, ZigZag up + touch
            if cross_up and bull and zz_dir == "up" and touched:
                return True, "call"
            # SELL: red candle crosses Keltner mid down, Stoch cross down from overbought, ZigZag down + touch
            if cross_down and bear and zz_dir == "down" and touched:
                return True, "put"
            return False, "call"

        elif strategy == 11:  # Strategy 11: "Sureshot" Breakout+Retest+Error Candle with EMA(5)
            tf = 60  # 1-minute timeframe
            candles = await _get_candles_safe(qx, asset, tf, 30, ctx="s11_sureshot")
            if not candles or len(candles) < 30:
                _dbg(f"{asset} s11: insufficient 60s candles for sureshot")
                try:
                    import time as _t
                    S11_BAD_ASSETS[asset] = _t.time() + float(S11_BAD_TTL_SEC)
                except Exception:
                    pass
                return False, "call"

            closes = [float(c["close"]) for c in candles]
            highs = [float(c["high"]) for c in candles]
            lows = [float(c["low"]) for c in candles]
            opens = [float(c["open"]) for c in candles]

            # EMA(5) for trend confirmation
            ema5 = _ema(closes, 5)
            if len(ema5) < 5:
                _dbg(f"{asset} s11: insufficient EMA5 data")
                return False, "call"

            # ATR for retest tolerance calculation
            atr14 = _atr(highs, lows, closes, 14)
            if not atr14:
                _dbg(f"{asset} s11: no ATR data")
                return False, "call"

            retest_tolerance = 0.2 * float(atr14[-1])  # 0.2 ATR tolerance for retest

            # Find recent swing highs and lows (last 20 bars)
            swing_lookback = min(20, len(candles) - 5)  # Ensure we have enough data
            recent_highs = highs[-swing_lookback:]
            recent_lows = lows[-swing_lookback:]

            # Identify swing high/low levels
            swing_high = max(recent_highs)
            swing_low = min(recent_lows)

            current_close = closes[-1]
            current_open = opens[-1]
            prev_close = closes[-2] if len(closes) >= 2 else current_close
            prev_open = opens[-2] if len(opens) >= 2 else current_open

            # Check for breakout
            breakout_up = False
            breakout_down = False

            # UP Breakout: Current or previous candle broke above swing high
            if current_close > swing_high or prev_close > swing_high:
                breakout_up = True
                breakout_level = swing_high

            # DOWN Breakout: Current or previous candle broke below swing low
            if current_close < swing_low or prev_close < swing_low:
                breakout_down = True
                breakout_level = swing_low

            if not (breakout_up or breakout_down):
                return False, "call"

            # Check for retest
            retest_up = False
            retest_down = False

            if breakout_up:
                # Look for retest of the broken resistance (now support)
                # Price should come back near the level and hold above it
                distance_to_level = abs(current_close - breakout_level)
                if distance_to_level <= retest_tolerance and current_close >= breakout_level:
                    retest_up = True

            if breakout_down:
                # Look for retest of the broken support (now resistance)
                # Price should come back near the level and hold below it
                distance_to_level = abs(current_close - breakout_level)
                if distance_to_level <= retest_tolerance and current_close <= breakout_level:
                    retest_down = True

            if not (retest_up or retest_down):
                return False, "call"

            # Check for error candle and confirmation
            current_body = abs(current_close - current_open)
            current_range = max(1e-6, highs[-1] - lows[-1])
            current_body_ratio = current_body / current_range
            is_current_green = current_close > current_open
            is_current_red = current_close < current_open

            # Error candle detection and entry logic — only enter on confirmation candle
            if retest_up:
                # Need previous candle to be the small red error candle
                if len(candles) >= 2 and is_current_green and current_close > ema5[-1]:
                    prev_body = abs(prev_close - prev_open)
                    prev_range = max(1e-6, highs[-2] - lows[-2])
                    prev_body_ratio = prev_body / prev_range
                    was_prev_red = prev_close < prev_open
                    if was_prev_red and prev_body_ratio <= 0.35:
                        _dbg(f"{asset} s11 sureshot: UP signal - green confirmation after error candle")
                        return True, "call"

            if retest_down:
                # Need previous candle to be the small green error candle
                if len(candles) >= 2 and is_current_red and current_close < ema5[-1]:
                    prev_body = abs(prev_close - prev_open)
                    prev_range = max(1e-6, highs[-2] - lows[-2])
                    prev_body_ratio = prev_body / prev_range
                    was_prev_green = prev_close > prev_open
                    if was_prev_green and prev_body_ratio <= 0.35:
                        _dbg(f"{asset} s11 sureshot: DOWN signal - red confirmation after error candle")
                        return True, "put"

            return False, "call"

        elif strategy == 10:  # Strategy 10: EMA(11/55) + PSAR(0.02,0.3) + Williams %R(7)
            tf = timeframe
            # Ensure helper functions are available
            if compute_psar is None or compute_williams_r is None:
                return False, "call"
            # Fetch sufficient candles for PSAR and %R stability
            candles = await _get_candles_safe(qx, asset, tf, 60, ctx="s10")
            if not candles or len(candles) < 60:
                return False, "call"
            closes = [float(c["close"]) for c in candles]
            highs = [float(c["high"]) for c in candles]
            lows = [float(c["low"]) for c in candles]

            # EMAs using broker helper; request enough history for EMA55
            e11 = await qx.calculate_indicator(asset, "EMA", {"period": 11}, history_size=tf * 120, timeframe=tf)
            e55 = await qx.calculate_indicator(asset, "EMA", {"period": 55}, history_size=tf * 120, timeframe=tf)
            ema11 = e11.get("ema", []) if isinstance(e11, dict) else []
            ema55 = e55.get("ema", []) if isinstance(e55, dict) else []
            if not ema11 or not ema55:
                return False, "call"
            ema11c = float(ema11[-1])
            ema55c = float(ema55[-1])

            # Parabolic SAR (local calc)
            psar = compute_psar(highs, lows, step=0.02, max_step=0.3)
            if not psar:
                return False, "call"
            psar_val = float(psar[-1])
            psar_below = psar_val <= float(lows[-1])
            psar_above = psar_val >= float(highs[-1])

            # Williams %R(7) (local calc) — require slope confirmation
            willr = compute_williams_r(highs, lows, closes, period=7)
            if len(willr) < 2:
                return False, "call"
            wr_prev, wr_cur = float(willr[-2]), float(willr[-1])
            wr_up = wr_cur > wr_prev
            wr_down = wr_cur < wr_prev

            # Additional filters for conservatism
            # 1) Trend strength via ADX
            adx_vals = _adx(highs, lows, closes, 14)
            adx_ok = bool(adx_vals) and float(adx_vals[-1]) >= 18
            # 2) Volatility/distance gate via ATR
            atr14 = _atr(highs, lows, closes, 14)
            atr_ok = bool(atr14)
            dist_from_ema55 = abs(closes[-1] - ema55c)
            dist_ok_up = atr_ok and (closes[-1] - ema55c) >= 0.03 * float(atr14[-1])
            dist_ok_down = atr_ok and (ema55c - closes[-1]) >= 0.03 * float(atr14[-1])
            # 3) EMA55 slope confirmation
            ema55_prev = float(ema55[-2]) if len(ema55) >= 2 else ema55c
            ema_slope_up = ema55c >= ema55_prev
            ema_slope_down = ema55c <= ema55_prev
            # 4) PSAR consistency on last 2 bars
            psar_cons_up = len(psar) >= 2 and (psar[-1] <= lows[-1] and psar[-2] <= lows[-2])
            psar_cons_down = len(psar) >= 2 and (psar[-1] >= highs[-1] and psar[-2] >= highs[-2])
            # 5) Candle quality in direction
            last = candles[-1]
            body = abs(float(last["close"]) - float(last["open"]))
            rng = max(1e-6, float(last["high"]) - float(last["low"]))
            body_ratio = body / rng
            green = float(last["close"]) > float(last["open"])  # bullish body
            red = float(last["close"]) < float(last["open"])    # bearish body
            body_ok_up = green and body_ratio >= 0.35
            body_ok_down = red and body_ratio >= 0.35
            # 6) Williams %R zone guard (avoid extreme overbought/oversold traps)
            # Require %R to be in mid-range towards the trade direction
            wr_zone_up = (-80.0 <= wr_cur <= -20.0) and wr_up
            wr_zone_down = (-80.0 <= wr_cur <= -20.0) and wr_down
            # 7) ZigZag direction alignment ("visual" trend confirmation)
            zz_dir = _zigzag_last_direction(highs, lows, deviation=0.5, depth=13, backstep=3)
            zz_up = (zz_dir == "up")
            zz_down = (zz_dir == "down")

            # Base confluence
            base_buy = (ema11c > ema55c) and psar_below and wr_up
            base_sell = (ema11c < ema55c) and psar_above and wr_down

            # Confidence scoring
            buy_score = 0
            if base_buy: buy_score += 1
            if ema_slope_up: buy_score += 1
            if adx_ok: buy_score += 1
            if dist_ok_up: buy_score += 1
            if psar_cons_up: buy_score += 1
            if body_ok_up: buy_score += 1
            if wr_zone_up: buy_score += 1
            if zz_up: buy_score += 1

            sell_score = 0
            if base_sell: sell_score += 1
            if ema_slope_down: sell_score += 1
            if adx_ok: sell_score += 1
            if dist_ok_down: sell_score += 1
            if psar_cons_down: sell_score += 1
            if body_ok_down: sell_score += 1
            if wr_zone_down: sell_score += 1
            if zz_down: sell_score += 1

            # Conservative: require high confidence (>= 6 of 8 signals) to trade
            buy_ok = base_buy and (buy_score >= 6)
            sell_ok = base_sell and (sell_score >= 6)

            if buy_ok:
                return True, "call"
            if sell_ok:
                return True, "put"
            return False, "call"

        elif strategy == 9:  # Strategy 9: Multi-indicator Alignment (EMA50 trend + EMA21 cross + RSI + Stoch + ADX + ATR)
            tf = 60  # recommended TF for Strategy 9 to balance reliability and availability
            candles = await _get_candles_safe(qx, asset, tf, 60, ctx="s9")
            if not candles or len(candles) < 60:
                return False, "call"
            closes = [float(c["close"]) for c in candles]
            highs = [float(c["high"]) for c in candles]
            lows = [float(c["low"]) for c in candles]
            opens = [float(c["open"]) for c in candles]
            ema50 = _ema(closes, 50)
            ema21 = _ema(closes, 21)
            if len(ema50) < 3 or len(ema21) < 3:
                return False, "call"
            # Trend filter with EMA50 and its slope
            trend_up = closes[-1] > ema50[-1] and ema50[-1] >= ema50[-2]
            trend_down = closes[-1] < ema50[-1] and ema50[-1] <= ema50[-2]
            # EMA21 cross as trigger (allow last or previous candle)
            prev_close, cur_close = closes[-2], closes[-1]
            prev_open, cur_open = opens[-2], opens[-1]
            prev_mid, cur_mid = ema21[-2], ema21[-1]
            is_green = cur_close > cur_open
            is_red = cur_close < cur_open
            cross_now_up = prev_close < prev_mid and cur_close > cur_mid and is_green
            cross_now_down = prev_close > prev_mid and cur_close < cur_mid and is_red
            # previous candle cross
            prev2_close = closes[-3]
            prev2_mid = ema21[-3]
            prev_is_green = prev_close > prev_open
            prev_is_red = prev_close < prev_open
            cross_prev_up = prev2_close < prev2_mid and prev_close > prev_mid and prev_is_green
            cross_prev_down = prev2_close > prev2_mid and prev_close < prev_mid and prev_is_red
            cross_up = cross_now_up or cross_prev_up
            cross_down = cross_now_down or cross_prev_down
            # RSI guard
            rsi = await qx.calculate_indicator(asset, "RSI", {"period": 14}, timeframe=tf)
            rvals = rsi.get("rsi", [])
            if not rvals:
                return False, "call"
            rcur = float(rvals[-1])
            rsi_ok_up = 55 <= rcur <= 70
            rsi_ok_down = 30 <= rcur <= 45
            # Stochastic confirmation
            k_slow, d_slow = _stoch_slow(closes, highs, lows, 14, 3, 3)
            if len(k_slow) < 2 or len(d_slow) < 2:
                return False, "call"
            prev_k, cur_k = k_slow[-2], k_slow[-1]
            prev_d, cur_d = d_slow[-2], d_slow[-1]
            stoch_buy = prev_k < prev_d and cur_k > cur_d and min(prev_k, cur_k) <= 40
            stoch_sell = prev_k > prev_d and cur_k < cur_d and max(prev_k, cur_k) >= 60
            # ADX and ATR gates
            adx_vals = _adx(highs, lows, closes, 14)
            atr14 = _atr(highs, lows, closes, 14)
            if not adx_vals or not atr14:
                return False, "call"
            adx_ok = adx_vals[-1] >= 18
            dist_ok = abs(cur_close - cur_mid) >= 0.03 * atr14[-1]
            # Final alignment
            if trend_up and cross_up and rsi_ok_up and stoch_buy and adx_ok and dist_ok:
                return True, "call"
            if trend_down and cross_down and rsi_ok_down and stoch_sell and adx_ok and dist_ok:
                return True, "put"
            return False, "call"
        else:  # Strategy 5: Trend + Pullback + Momentum
            # 1m trend filter: EMA(50) on 1m; signal timeframe 30s
            candles30 = await qx.get_candles(asset, time.time(), 30 * 200, 30)
            if not candles30 or len(candles30) < 60:
                return False, "call"
            closes30 = [float(c["close"]) for c in candles30]
            highs30 = [float(c["high"]) for c in candles30]
            lows30 = [float(c["low"]) for c in candles30]
            opens30 = [float(c["open"]) for c in candles30]

            # Trend on 1m
            candles60 = await qx.get_candles(asset, time.time(), 60 * 300, 60)
            if not candles60 or len(candles60) < 60:
                return False, "call"
            closes60 = [float(c["close"]) for c in candles60]
            ema50m = _ema(closes60, 50)
            if len(ema50m) < 2:
                return False, "call"
            trend_up = closes60[-1] > ema50m[-1]
            trend_down = closes60[-1] < ema50m[-1]

            # Pullback to EMA21 on 30s, then momentum candle breakout
            ema21_30 = _ema(closes30, 21)
            if len(ema21_30) < 3:
                return False, "call"
            prev_close, cur_close = closes30[-2], closes30[-1]
            prev_mid, cur_mid = ema21_30[-2], ema21_30[-1]
            is_green = cur_close > opens30[-1]
            is_red = cur_close < opens30[-1]
            pullback_up = trend_up and prev_close <= prev_mid and cur_close > cur_mid and is_green
            pullback_down = trend_down and prev_close >= prev_mid and cur_close < cur_mid and is_red

            # Momentum confirmation via RSI on 30s
            rsi30 = await qx.calculate_indicator(asset, "RSI", {"period": 7}, timeframe=30)
            rsi_vals = rsi30.get("rsi", [])
            if not rsi_vals:
                return False, "call"
            rsi_cur = rsi_vals[-1]

            if pullback_up and rsi_cur >= 55:
                return True, "call"
            if pullback_down and rsi_cur <= 45:
                return True, "put"
            return False, "call"
    except Exception:
        return False, "call"


async def ensure_asset_open(qx, asset: str) -> str:
    try:
        return (await asyncio.wait_for(qx.get_available_asset(asset, force_open=True), timeout=S11_ENSURE_OPEN_TIMEOUT_SEC))[0]
    except Exception:
        return asset


# --- Session profiling and trade logging ---
import csv
import os
from datetime import datetime

LOG_PATH = os.path.join(os.path.dirname(__file__), "..", "trades_log.csv")

# --- Global adaptive controls ---
_ASSET_COOLDOWN: dict[str, float] = {}       # asset -> epoch seconds until available
_ASSET_LOSS_STREAK: dict[str, int] = {}      # asset -> consecutive losses
# Strategy 11 (Sureshot) per-asset loss tracking for MM and switching
S11_LOSS_STREAK: dict[str, int] = {}
S11_LAST_LOSSES: dict[str, list[float]] = {}
S11_RECOVERY_PENDING: float = 0.0  # sum of last two losses to recover on next trade
S10_ALLOWED_HOURS = set(range(7, 21))        # 07:00 to 20:59 local time by default
# Strategy 10 historical win-rate guard (disabled by default to not block valid signals)
S10_WR_GUARD_ENABLED = False
S10_WR_MIN_TRADES = 20
S10_WR_MIN_WINRATE = 0.55

# Strategy 10 diagnostics and performance tracking
S10_DIAG_LAST: dict | None = None
S10_DIAG_LOG = os.path.join(os.path.dirname(__file__), "..", "strategy10_signals.csv")
# Strategy 11 diagnostics and performance tracking
S11_DIAG_LAST: dict | None = None
S11_DIAG_LOG = os.path.join(os.path.dirname(__file__), "..", "strategy11_signals.csv")


def _s11_diag_log_header():
    try:
        if not os.path.exists(S11_DIAG_LOG):
            with open(S11_DIAG_LOG, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    "ts","asset","tf","direction",
                    "k_last","d_last","bull_stoch","bear_stoch",
                    "ema7_last","ema2_last","ema_align_up","ema_align_down",
                    "ema_cross_up_win","ema_cross_dn_win",
                    "trend_up","trend_down","adx14","atr14","rsi14",
                    "htf_up","htf_down","ticks_ok","near_res","near_sup",
                    "buy_score","sell_score","has_signal","result"
                ])
    except Exception:
        pass


def _s11_log_diag(asset: str, direction: str, result: str | None = None):
    try:
        global S11_DIAG_LAST
        if not S11_DIAG_LAST:
            return
        _s11_diag_log_header()
        now = datetime.now().isoformat(timespec="seconds")
        d = S11_DIAG_LAST
        with open(S11_DIAG_LOG, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                now, asset, d.get("tf"), direction,
                d.get("k_last"), d.get("d_last"), int(d.get("bull_stoch", False)), int(d.get("bear_stoch", False)),
                d.get("ema7_last"), d.get("ema2_last"), int(d.get("ema_align_up", False)), int(d.get("ema_align_down", False)),
                int(d.get("ema_cross_up_window", False)), int(d.get("ema_cross_down_window", False)),
                int(d.get("trend_up", False)), int(d.get("trend_down", False)), d.get("adx14"), d.get("atr14"), d.get("rsi14_last"),
                int(d.get("htf_up", False)), int(d.get("htf_down", False)), int(d.get("ticks_ok", False)),
                int(d.get("near_res", False)), int(d.get("near_sup", False)),
                d.get("buy_score"), d.get("sell_score"), int(d.get("has_signal", False)), result or "",
            ])
    except Exception:
        pass

S10_PERFORMANCE_LOG = os.path.join(os.path.dirname(__file__), "..", "strategy10_performance.csv")


def _hour_utc_to_ist(hour_utc: int) -> int:
    # UTC+5:30; adding 5h30m shifts hour by +5 or +6 depending on minutes; use zone conversion
    try:
        # Use timezone-aware UTC now to avoid deprecated utcfromtimestamp/utcnow
        now_utc = datetime.now(ZoneInfo("UTC"))
        at_hour = now_utc.replace(hour=hour_utc, minute=0, second=0, microsecond=0)
        ist_hour = at_hour.astimezone(ZoneInfo("Asia/Kolkata")).hour
        return int(ist_hour)
    except Exception:
        # Fallback: simple +5 offset mod 24
        return (hour_utc + 5) % 24


def recompute_s10_allowed_hours_from_logs(min_trades: int = 10,
                                           min_winrate: float = 0.60) -> set[int]:
    """Compute best IST hours for Strategy 10 from trades_log.csv.
    Returns a set of allowed IST hours (0-23)."""
    rows = _load_recent_rows()
    if not rows:
        return S10_ALLOWED_HOURS
    # Aggregate by IST hour for strategy==10
    buckets: dict[int, tuple[int, int]] = {}  # hour -> (total, wins)
    for r in rows:
        try:
            dow, hour_utc = r[1], int(r[2])
            strat = int(r[5])
            res = (r[8] or "").upper()
            if strat != 10:
                continue
            ist_hour = _hour_utc_to_ist(hour_utc)
            total, wins = buckets.get(ist_hour, (0, 0))
            total += 1
            if res == "WIN":
                wins += 1
            buckets[ist_hour] = (total, wins)
        except Exception:
            continue
    # Select hours that meet thresholds
    allowed: set[int] = set()
    for h, (total, wins) in buckets.items():
        if total >= min_trades:
            wr = wins / max(1, total)
            if wr >= min_winrate:
                allowed.add(h)
    # If none meet, fall back to top-3 hours by WR with at least 5 trades
    if not allowed and buckets:
        scored = [
            (h, (w / max(1, t))) for h, (t, w) in buckets.items() if t >= 5
        ]
        scored.sort(key=lambda x: x[1], reverse=True)
        for h, _ in scored[:3]:
            allowed.add(h)
    return allowed or S10_ALLOWED_HOURS





def _s10_diag_log_header():
    try:
        if not os.path.exists(S10_DIAG_LOG):
            with open(S10_DIAG_LOG, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([
                    "ts","asset","tf","direction","ema11","ema55","ema55_slope",
                    "adx14","atr14","dist_to_ema55","psar_last","psar_prev",
                    "wr_prev","wr_cur","wr_zone_flag","body_ratio","zz_dir",
                    "flags","score","base_buy","base_sell","has_signal","result"
                ])
    except Exception:
        pass


def _s10_log_diag(asset: str, direction: str, result: str | None = None):
    try:
        global S10_DIAG_LAST
        if not S10_DIAG_LAST:
            return
        _s10_diag_log_header()
        now = datetime.now().isoformat(timespec="seconds")
        d = S10_DIAG_LAST
        with open(S10_DIAG_LOG, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                now, asset, d.get("tf"), direction,
                d.get("ema11"), d.get("ema55"), d.get("ema_slope"),
                d.get("adx14"), d.get("atr14"), d.get("dist_to_ema55"),
                d.get("psar_last"), d.get("psar_prev"),
                d.get("wr_prev"), d.get("wr_cur"), d.get("wr_zone_flag"),
                d.get("body_ratio"), d.get("zz_dir"),
                "|".join(sorted(d.get("flags", []))), d.get("score"),
                int(d.get("base_buy", False)), int(d.get("base_sell", False)),
                int(d.get("has_signal", False)), result or "",
            ])
    except Exception:
        pass

def _is_otc(symbol: str) -> bool:
    return symbol.lower().endswith("_otc")


def _ensure_log_header():
    if not os.path.exists(LOG_PATH):
        with open(LOG_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "ts", "dow", "hour", "asset", "is_otc", "strategy",
                "direction", "amount", "result", "pnl", "payout",
                "expiry_min", "account_mode"
            ])


def log_trade_row(asset: str, strategy: int, direction: str, amount: float,
                  result: str, pnl: float, payout: float, expiry_min: int,
                  account_mode: str):
    _ensure_log_header()
    now = datetime.now()
    row = [
        now.isoformat(timespec="seconds"),
        now.strftime("%a"),
        int(now.strftime("%H")),
        asset,
        int(_is_otc(asset)),
        strategy,
        direction.upper(),
        amount,
        result.upper(),
        pnl,
        payout,
        expiry_min,
        account_mode,
    ]
    with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)


def _load_recent_rows(days: int = 14) -> list[list[str]]:
    if not os.path.exists(LOG_PATH):
        return []
    out = []
    try:
        with open(LOG_PATH, "r", encoding="utf-8") as f:
            cr = csv.reader(f)
            headers = next(cr, None)
            for r in cr:
                out.append(r)
    except Exception:
        return []
    return out


def should_trade_now(strategy: int, min_trades: int = 5,
                      min_winrate: float = 0.55,
                      include_otc: bool = True) -> tuple[bool, str]:
    rows = _load_recent_rows()
    if not rows:
        return True, "no_history"
    now = datetime.now()
    dow = now.strftime("%a")
    hour = int(now.strftime("%H"))
    total = wins = 0
    for r in rows:
        try:
            r_dow, r_hour = r[1], int(r[2])
            r_asset = r[3]
            r_strat = int(r[5])
            r_res = r[8]
            if r_dow == dow and r_hour == hour and r_strat == strategy:
                if include_otc or not _is_otc(r_asset):
                    total += 1
                    if r_res == "WIN":
                        wins += 1
        except Exception:
            continue
    if total < min_trades:
        return True, f"bucket={dow}-{hour} insufficient_data({total}<{min_trades})"
    wr = wins / max(1, total)
    ok = wr >= min_winrate
    why = f"bucket={dow}-{hour} winrate={wr:.2f} min={min_winrate:.2f} total={total}"
    return ok, why



# --- Broker connection health + recovery helpers ---
async def _hard_recover_broker(qx) -> None:
    """Fully recover the websocket session and clear sticky error flags.
    Uses low-level API connect sequence (start_websocket + SSID) instead of the
    shallow authenticate-only reconnect to ensure a clean state.
    """
    try:
        from pyquotex import global_value as _gv  # type: ignore
        # Close existing WS if any and start a fresh one + SSID
        try:
            # This path resets WS and sends SSID (unlike stable_api.reconnect)
            await qx.api.connect(qx.account_is_demo)
        except Exception:
            # Fallback: re-run full client connect if direct path fails
            try:
                await qx.close()
            except Exception:
                pass
            try:
                await qx.connect()
            except Exception:
                pass
        # Clear sticky error flags and stale IDs
        try:
            _gv.check_websocket_if_error = False
            _gv.websocket_error_reason = None
            _gv.check_rejected_connection = False
        except Exception:
            pass
        try:
            setattr(qx.api, 'buy_id', None)
            setattr(qx.api, 'pending_id', None)
            setattr(qx.api, 'buy_successful', None)
            setattr(qx.api, 'pending_successful', None)
        except Exception:
            pass
        try:
            await asyncio.sleep(0.2)
        except Exception:
            pass
        # Metrics: count hard recoveries
        try:
            globals()['S12_HARD_RECOVER_COUNT'] = int(globals().get('S12_HARD_RECOVER_COUNT', 0)) + 1
        except Exception:
            pass
    except Exception:
        pass


async def _ensure_ws_connection(qx) -> None:
    """Ensure the websocket is healthy; if not, perform a hard recovery."""
    try:
        from pyquotex import global_value as _gv  # type: ignore
        gv_conn = getattr(_gv, 'check_websocket_if_connect', None)
        gv_err = bool(getattr(_gv, 'check_websocket_if_error', False))
        if gv_conn != 1 or gv_err:
            await _hard_recover_broker(qx)
    except Exception:
        # Best-effort fallback
        try:
            await _hard_recover_broker(qx)
        except Exception:
            pass

async def place_and_wait(qx, amount: float, asset: str, direction: str, duration_s: int) -> Tuple[bool, float]:
    """Place order and resolve result reliably using broker result status.
    Returns (won, net_delta). Net delta aims to be profit-only (not stake).
    """
    # Reset last exec flags
    try:
        setattr(qx, "last_exec_failed", False)
        setattr(qx, "last_exec_error_reason", "")
        setattr(qx, "last_exec_status", "")
        setattr(qx, "last_exec_asset", asset)
        setattr(qx, "last_exec_meta", {})
    except Exception:
        pass

    time_mode = "TIMER" if asset.endswith("_otc") else "TIME"

    # Health check WebSocket and reset API state to avoid cascading failures
    await _ensure_ws_connection(qx)
    # Reset api-side IDs to avoid stale bleed-through between trades
    try:
        setattr(qx.api, 'buy_id', None)
        setattr(qx.api, 'pending_id', None)
        setattr(qx.api, 'buy_successful', None)
        setattr(qx.api, 'pending_successful', None)
    except Exception:
        pass

    # Snapshot balance before (only as a last-resort fallback; multiple open trades can distort it)
    bal_before = await qx.get_balance()

    # Pre-trade asset validation using instruments/open flag to avoid invalid symbols
    try:
        try:
            _asset_name, _open_info = await qx.get_available_asset(asset, force_open=False)
        except Exception:
            _asset_name, _open_info = asset, None
        if (not _open_info) or (isinstance(_open_info, (list, tuple)) and not bool(_open_info[2])):
            # Unknown or closed asset
            try:
                setattr(qx, "last_exec_failed", True)
                setattr(qx, "last_exec_error_reason", "asset_closed_or_unknown")
                setattr(qx, "last_exec_status", "fail_exec")
            except Exception:
                pass
            return False, 0.0
    except Exception:
        pass

    # Optional pre-trade tradability check if available
    try:
        if hasattr(qx, 'is_tradable'):
            tradable_now = await qx.is_tradable(asset)
            if not tradable_now:
                try:
                    setattr(qx, "last_exec_failed", True)
                    setattr(qx, "last_exec_error_reason", "not_tradable")
                    setattr(qx, "last_exec_status", "fail_exec")
                except Exception:
                    pass
                return False, 0.0
    except Exception:
        pass

    # Pre-buy re-check of availability (double check)
    try:
        if hasattr(qx, 'is_tradable'):
            tradable_now2 = await qx.is_tradable(asset)
            if not tradable_now2:
                try:
                    setattr(qx, "last_exec_failed", True)
                    setattr(qx, "last_exec_error_reason", "became_unavailable")
                    setattr(qx, "last_exec_status", "fail_exec")
                except Exception:
                    pass
                return False, 0.0
    except Exception:
        pass

    ok, _ = await qx.buy(amount, asset, direction, duration_s, time_mode=time_mode)
    if not ok:
        # Post-fail availability re-check and cooldown + history-match salvage
        became_unavail = False
        try:
            if hasattr(qx, 'is_tradable'):
                trad_after = await qx.is_tradable(asset)
                if not trad_after:
                    became_unavail = True
            setattr(qx, "last_exec_failed", True)
            setattr(qx, "last_exec_error_reason", "became_unavailable" if became_unavail else "buy_rejected")
            setattr(qx, "last_exec_status", "fail_exec")
            # Metrics: count FAIL_EXEC
            try:
                globals()['S12_FAIL_EXEC_COUNT'] = int(globals().get('S12_FAIL_EXEC_COUNT', 0)) + 1
            except Exception:
                pass
            if became_unavail:
                # Apply long cooldown 20-30s on became_unavailable
                try:
                    from time import time as _now
                    if 'exec_cooldowns' not in globals():
                        globals()['exec_cooldowns'] = {}
                    globals()['exec_cooldowns'][asset] = _now() + 25.0
                except Exception:
                    pass
        except Exception:
            pass
        # Force clear sticky WS error state before next attempts
        try:
            await _ensure_ws_connection(qx)
        except Exception:
            pass
        # History-match immediate fallback: treat as placed-if-recent ticket exists
        try:
            hist = await qx.get_history()
            matched_tid = None
            if isinstance(hist, list):
                now_ts = time.time()
                for it in hist[:10]:
                    sym = it.get('asset') or it.get('symbol') or it.get('pair')
                    amt = float(it.get('amount', 0) or it.get('value', 0) or 0)
                    ts = float(it.get('time') or it.get('closeTimestamp') or 0)
                    if sym == asset and abs(amt - float(amount)) < 1e-3 and (now_ts - ts) <= 5.0:
                        matched_tid = it.get('ticket') or it.get('id')
                        break
            if matched_tid:
                # Upgrade to tracking path using matched ticket id
                try:
                    status_hist, item = await qx.get_result(matched_tid)
                except Exception:
                    status_hist, item = None, None
                if isinstance(item, dict):
                    st = (status_hist or item.get("status") or "").lower()
                    pa = float(item.get("profitAmount", item.get("profit", 0)) or 0)
                    if st in ("win","won"):
                        net = (pa - amount) if pa >= amount else pa
                        if abs(net) < 1e-9:
                            setattr(qx, "last_exec_status", "draw")
                            return False, 0.0
                        setattr(qx, "last_exec_status", "win")
                        return True, round(net, 2)
                    if st in ("loss","lost"):
                        if abs(pa) < 1e-9:
                            setattr(qx, "last_exec_status", "draw")
                            return False, 0.0
                        setattr(qx, "last_exec_status", "loss")
                        return False, -float(amount)
                    if st in ("equal","tie","draw"):
                        setattr(qx, "last_exec_status", "draw")
                        return False, 0.0
                # If still unknown, mark as fail_track rather than exec
                setattr(qx, "last_exec_status", "fail_track")
                setattr(qx, "last_exec_error_reason", "track_timeout")
                return False, 0.0
        except Exception:
            pass
        return False, 0.0

    # Capture operation identifiers (id and ticket) robustly
    op_id = None
    ticket_id = None
    bs = None
    pend = None
    try:
        op_id = getattr(qx.api, 'buy_id', None)
    except Exception:
        op_id = None
    try:
        ticket_id = getattr(qx.api, 'pending_id', None)
    except Exception:
        ticket_id = None
    # Fallback: try buy_successful payload
    try:
        bs = getattr(qx.api, 'buy_successful', None)
    except Exception:
        bs = None
    if not op_id and isinstance(bs, dict) and bs.get('id'):
        op_id = bs.get('id')
        try:
            setattr(qx.api, 'buy_id', op_id)
        except Exception:
            pass
    # Pending success flag
    try:
        pend = getattr(qx.api, 'pending_successful', None)
    except Exception:
        pend = None

    # Small wait loop to allow ws client to populate identifiers
    try:
        import asyncio as _aio
        for _ in range(20):  # ~2s
            if not op_id:
                try:
                    op_id = getattr(qx.api, 'buy_id', None)
                except Exception:
                    op_id = None
            if not ticket_id:
                try:
                    ticket_id = getattr(qx.api, 'pending_id', None)
                except Exception:
                    ticket_id = None
            if op_id or ticket_id:
                break
            await _aio.sleep(0.1)
    except Exception:
        pass

    # Last-resort fallback: pull latest history and infer ticket
    if not op_id and not ticket_id:
        try:
            hist = await qx.get_history()
            if isinstance(hist, list) and hist:
                # pick freshest by time
                def _ts(it):
                    return float(it.get('time') or it.get('closeTimestamp') or 0)
                latest = max(hist, key=_ts)
                tid = latest.get('ticket')
                if tid:
                    ticket_id = tid
        except Exception:
            pass

    # Debug trace for identifiers
    try:
        print(f"[S12][TRACE] ids post-buy: op_id={op_id} ticket={ticket_id} has_bs={bool(bs)} has_pend={bool(pend)}")
    except Exception:
        pass

    # Wait until broker marks result ready via check_win (bounded timeout)
    win_bool: bool | int | None = None
    if op_id:
        try:
            import asyncio as _aio
            # Timeout tuned: expiry duration plus headroom
            _tmo = max(30.0, min(120.0, float(duration_s) + 15.0))
            print(f"[S12][RESOLVE] check_win waiting (timeout={_tmo:.0f}s) op_id={op_id}")
            win_bool = await _aio.wait_for(qx.check_win(op_id), timeout=_tmo)
        except _aio.TimeoutError:
            # Do not hang indefinitely if WS never publishes listinfodata entry
            try:
                setattr(qx, "last_exec_status", "fail_track")
                setattr(qx, "last_exec_error_reason", "check_win_timeout")
                # Metrics: count timeout fallbacks
                globals()['S12_TIMEOUT_FALLBACKS'] = int(globals().get('S12_TIMEOUT_FALLBACKS', 0)) + 1
            except Exception:
                pass
            print("[S12][RESOLVE] check_win timeout; will fallback to history polling")
            win_bool = None
        except Exception as _e:
            print(f"[S12][RESOLVE] check_win exception: {_e!r}; fallback to history polling")
            win_bool = None

    # Seed fallback delta from payout if we got a definitive bool/int
    delta_seed: float | None = None
    if isinstance(win_bool, (bool, int)):
        try:
            p = qx.get_payout_by_asset(asset, timeframe="1")
            payoff = float(p) if p is not None else 0.0
        except Exception:
            payoff = 0.0
        if bool(win_bool):
            delta_seed = round(amount * (payoff / 100.0), 2)
            try:
                setattr(qx, "last_exec_status", "win")
            except Exception:
                pass
        else:
            delta_seed = -amount
            try:
                setattr(qx, "last_exec_status", "loss")
            except Exception:
                pass

    # Poll detailed result and map status robustly (prefer ticket when present)
    status_hist: str | None = None
    delta_hist: float | None = None
    op_for_poll = ticket_id or op_id
    for _ in range(60):  # up to ~60s polling window post-expiry
        if not op_for_poll:
            break
        try:
            status, item = await qx.get_result(op_for_poll)
        except Exception:
            status, item = None, None
        if isinstance(item, dict):
            try:
                status_hist = (status or item.get("status") or "").lower()
                pa = float(item.get("profitAmount", item.get("profit", 0)) or 0)
                if status_hist in ("win", "won"):
                    # ProfitAmount may be total return or net
                    net = (pa - amount) if pa >= amount else pa
                    # Some brokers mark equal as win with 0 profit
                    if abs(net) < 1e-9:
                        delta_hist = 0.0
                        setattr(qx, "last_exec_status", "draw")
                    else:
                        delta_hist = round(net, 2)
                        setattr(qx, "last_exec_status", "win")
                    break
                if status_hist in ("loss", "lost"):
                    # If profit is exactly 0, treat as draw (break-even), not loss
                    if abs(pa) < 1e-9:
                        delta_hist = 0.0
                        setattr(qx, "last_exec_status", "draw")
                    else:
                        # Some APIs return non-negative profitAmount on loss; normalize to -amount
                        delta_hist = round(-amount if pa >= 0 else -abs(pa), 2)
                        setattr(qx, "last_exec_status", "loss")
                    break
                if status_hist in ("equal", "tie", "draw"):
                    delta_hist = 0.0
                    setattr(qx, "last_exec_status", "draw")
                    break
            except Exception:
                pass
        await asyncio.sleep(1)

    # If broker status known but no delta, compute fallback from payout
    if delta_hist is None and status_hist in ("win", "won") and delta_seed is None:
        try:
            p = qx.get_payout_by_asset(asset, timeframe="1")
            payoff = float(p) if p is not None else 0.0
            delta_hist = round(amount * (payoff / 100.0), 2)
        except Exception:
            pass
    if delta_hist is None and status_hist in ("loss", "lost") and delta_seed is None:
        delta_hist = -amount

    # Balance-based fallback (unreliable if multiple trades overlap)
    try:
        bal_after = await qx.get_balance()
        delta_bal = round(bal_after - bal_before, 2)
    except Exception:
        delta_bal = 0.0

    # Decide final outcome with execution failure detection; avoid marking losses as FAIL
    if delta_hist is not None:
        won = delta_hist > 0
        delta = float(delta_hist)
    elif delta_seed is not None:
        won = delta_seed > 0
        delta = float(delta_seed)
    elif abs(delta_bal) > 0:
        won = delta_bal > 0
        delta = float(delta_bal)
    else:
        # Determine failure type based on exec signals
        saw_exec = bool(op_id or ticket_id or bs or pend)
        print(f"[WARN] Trade outcome unresolved on {asset} (id={op_id}, ticket={ticket_id}, buy_success={bool(bs)}, pending={bool(pend)})")
        try:
            setattr(qx, "last_exec_failed", True)
            if saw_exec:
                setattr(qx, "last_exec_status", "fail_track")
                setattr(qx, "last_exec_error_reason", "track_timeout")
            else:
                setattr(qx, "last_exec_status", "fail_exec")
                setattr(qx, "last_exec_error_reason", "no_op_id")
            setattr(qx, "last_exec_meta", {
                "saw_exec": saw_exec,
                "has_buy_success": bool(bs),
                "has_pending_success": bool(pend),
                "op_id": op_id,
                "ticket_id": ticket_id,
            })
        except Exception:
            pass
        won = False
        delta = 0.0

    return won, delta



def print_results_table(rows: List[List], headers: List[str]) -> None:
    try:
        table_str = tabulate(rows[-10:], headers=headers, tablefmt="pretty")
        print("\n" + table_str)
    except Exception as e:
        # Fallback simple print to ensure visibility
        print("\nLast trades (fallback):")
        for r in rows[-10:]:
            try:
                print(f"{r[0]:>3} | {r[1]:<12} | {r[2]:<4} | {r[3]:>8.2f} | {r[4]:<4} | {r[5]:>7.2f}")
            except Exception:
                print(str(r))
    try:
        import sys
        sys.stdout.flush()
    except Exception:
        pass


def read_float(prompt: str, default: float | None = None) -> float:
    while True:
        s = input(prompt).strip()
        if s == "" and default is not None:
            return float(default)
        try:
            return float(s)
        except ValueError:
            print("Please enter a number")


def prompt_credentials() -> tuple[str, str]:
    """Prompt user for email & password (masked)."""
    try:
        email = input("Enter your Quotex email: ").strip()
    except Exception:
        email = ""
    try:
        from getpass import getpass as _gp
        password = _gp("Enter your Quotex password: ")
    except Exception:
        password = input("Password (echo ON fallback): ")
    return email, password


def prompt_account_type() -> str:
    """Ask user demo/live; default DEMO."""
    try:
        ans = input("Account mode (demo/live) [demo]: ").strip().lower()
        if ans.startswith('l'):
            return "REAL"
    except Exception:
        pass
    return "PRACTICE"


async def show_balance(qx) -> float:
    """Fetch and print current balance."""
    bal = 0.0
    try:
        bal = await qx.get_balance()
    except Exception:
        pass
    try:
        print(f"Current balance: {bal}")
    except Exception:
        pass
    return float(bal or 0.0)


async def get_all_assets(qx) -> List[str]:
    names: List[str] = []
    try:
        instruments = await qx.get_instruments()
        for i in instruments:
            try:
                # i[1] = symbol
                names.append(i[1])
            except Exception:
                continue
    except Exception:
        pass
    return names


async def get_asset_payout(qx, asset: str, expiry_min: int) -> float:
    # Broker expects timeframe as string like '1' or '5'
    keys: List[str] = []
    if expiry_min <= 1:
        keys = ["1", "60"]
    elif expiry_min >= 5:
        keys = ["5", "300"]
    else:
        keys = [str(expiry_min)]
    for k in keys:
        try:
            val = qx.get_payout_by_asset(asset, timeframe=k)
            if val is not None:
                return float(val)
        except Exception:
            continue
    # final fallback: try without timeframe
    try:
        val = qx.get_payout_by_asset(asset)
        return float(val or 0)
    except Exception:
        return 0.0


# ---- Strategy 12 pre-trade confirmation and metrics helpers ----
S12_METRICS_CSV = os.path.join(os.path.dirname(__file__), "..", "s12_metrics.csv")
S12_METRICS_JSONL = os.path.join(os.path.dirname(__file__), "..", "s12_metrics.jsonl")
S12_FAIL_EXEC_COUNT = 0
S12_TIMEOUT_FALLBACKS = 0
S12_HARD_RECOVER_COUNT = 0
S12_ASSET_HEALTH: dict[str, dict] = {}


def _pct_rank(vals: List[float], v: float) -> float:
    try:
        if not vals:
            return 50.0
        s = sorted([float(x) for x in vals])
        # rank percentage of values <= v
        cnt = 0
        for x in s:
            if x <= v:
                cnt += 1
            else:
                break
        return 100.0 * cnt / max(1, len(s))
    except Exception:
        return 50.0


def _ensure_s12_metrics_header() -> None:
    try:
        if not os.path.exists(S12_METRICS_CSV):
            with open(S12_METRICS_CSV, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    "ts","asset","dir","payout","score","min_conf","passed",
                    "atr_pctile","body_ratio","ema60_align","ema30_align","ema15_slope",
                    "microtrend_ok","result","pnl","resolve_source","health_score"
                ])
    except Exception:
        pass


def _s12_log_metrics_row(data: dict) -> None:
    try:
        _ensure_s12_metrics_header()
        with open(S12_METRICS_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                datetime.now().isoformat(timespec="seconds"),
                data.get("asset"), data.get("dir"), data.get("payout"),
                round(float(data.get("score", 0.0)), 3), round(float(data.get("min_conf", 0.0)), 2),
                int(bool(data.get("passed", False))),
                round(float(data.get("atr_pctile", 0.0)), 1),
                round(float(data.get("body_ratio", 0.0)), 2),
                int(bool(data.get("ema60_align", False))),
                int(bool(data.get("ema30_align", False))),
                int(bool(data.get("ema15_slope", False))),
                int(bool(data.get("microtrend_ok", False))),
                data.get("result"), data.get("pnl"), data.get("resolve_source"),
                round(float(data.get("health_score", 0.0)), 3),
            ])
        with open(S12_METRICS_JSONL, "a", encoding="utf-8") as f:
            f.write(__import__("json").dumps(data, default=float) + "\n")
    except Exception:
        pass


async def s12_pretrade_confirm(qx, asset: str, direction: str) -> tuple[bool, float, dict]:
    """Compute multi-timeframe confirmations and return (passed, score01, details).
    Implements:
    - Primary TF 60s discovery
    - 30s EMA(11/55) alignment
    - 15s EMA11 slope agreement
    - 3-candle micro-trend (2 of last 3 bodies + last 2 closes relative to EMA7)
    - Body ratio >= 0.33 on 60s last candle
    - ATR percentile within [35, 80]
    Score in [0,1] using weighted confluence; dynamic thresholds applied by caller.
    """
    try:
        import time as _t
        # Fetch candles
        c60 = await qx.get_candles(asset, _t.time(), 60 * 240, 60)
        c30 = await qx.get_candles(asset, _t.time(), 30 * 240, 30)
        c15 = await qx.get_candles(asset, _t.time(), 15 * 240, 15)
        if not c60 or len(c60) < 120:
            return False, 0.0, {"reason": "insufficient_60s"}
        closes60 = [float(c["close"]) for c in c60]
        highs60 = [float(c["high"]) for c in c60]
        lows60 = [float(c["low"]) for c in c60]
        opens60 = [float(c["open"]) for c in c60]
        e11_60 = _ema(closes60, 11)
        e55_60 = _ema(closes60, 55)
        e7_60 = _ema(closes60, 7)
        if len(e11_60) < 2 or len(e55_60) < 2 or len(e7_60) < 3:
            return False, 0.0, {"reason": "insufficient_ema"}
        # Primary alignment
        align60 = (e11_60[-1] > e55_60[-1]) if direction == "call" else (e11_60[-1] < e55_60[-1])
        # 30s alignment
        align30 = False
        if c30 and len(c30) >= 120:
            closes30 = [float(x["close"]) for x in c30]
            e11_30 = _ema(closes30, 11)
            e55_30 = _ema(closes30, 55)
            if e11_30 and e55_30:
                align30 = (e11_30[-1] > e55_30[-1]) if direction == "call" else (e11_30[-1] < e55_30[-1])
        # 15s slope
        slope15 = False
        if c15 and len(c15) >= 120:
            closes15 = [float(x["close"]) for x in c15]
            e11_15 = _ema(closes15, 11)
            if len(e11_15) >= 2:
                slope15 = (e11_15[-1] > e11_15[-2]) if direction == "call" else (e11_15[-1] < e11_15[-2])
        # Micro-trend (60s): 2 of last 3 bodies in direction + last 2 closes beyond EMA7
        last3 = c60[-3:]
        bodies = [(float(k["close"]) - float(k["open"])) for k in last3]
        dir_bools = [(b > 0) if direction == "call" else (b < 0) for b in bodies]
        micro_bodies_ok = sum(1 for x in dir_bools if x) >= 2
        last2_close = [float(c60[-2]["close"]), float(c60[-1]["close"])]
        ema7_last2 = [float(e7_60[-2]), float(e7_60[-1])]
        micro_ema_ok = all((last2_close[i] > ema7_last2[i]) if direction == "call" else (last2_close[i] < ema7_last2[i]) for i in (0,1))
        microtrend_ok = micro_bodies_ok and micro_ema_ok
        # Body ratio on 60s last candle
        last = c60[-1]
        body = abs(float(last["close"]) - float(last["open"]))
        rng = max(1e-6, float(last["high"]) - float(last["low"]))
        body_ratio = (body / rng) if rng else 0.0
        body_ok = body_ratio >= 0.33
        # ATR percentile guard on 60s
        atr = _atr(highs60, lows60, closes60, 14)
        atr_ok = False
        atr_pctile = 0.0
        if atr and len(atr) >= 30:
            atr_pctile = _pct_rank(atr[-120:], float(atr[-1]))
            atr_ok = 35.0 <= atr_pctile <= 80.0
        # Build score
        w = {
            "align60": 0.30,
            "align30": 0.20,
            "slope15": 0.15,
            "micro": 0.20,
            "body": 0.10,
            "atr": 0.05,
        }
        score = 0.0
        if align60: score += w["align60"]
        if align30: score += w["align30"]
        if slope15: score += w["slope15"]
        if microtrend_ok: score += w["micro"]
        if body_ok: score += w["body"]
        if atr_ok: score += w["atr"]
        passed = align60 and body_ok and atr_ok  # hard must-haves
        details = {
            "ema60_align": align60,
            "ema30_align": align30,
            "ema15_slope": slope15,
            "microtrend_ok": microtrend_ok,
            "body_ratio": body_ratio,
            "atr_pctile": atr_pctile,
        }
        return passed, min(1.0, max(0.0, score)), details
    except Exception as e:
        return False, 0.0, {"reason": f"exception:{e}"}


async def _debug_strategy4_flags(qx, asset: str) -> tuple[bool, bool, str | None]:
    # returns (keltner_cross_ok, stoch_cross_ok, zigzag_dir)
    try:
        tf = 30
        candles = await _get_candles_safe(qx, asset, tf, 30, ctx="s4_dbg")
        if not candles or len(candles) < 30:
            _dbg(f"{asset} s4_dbg: insufficient 30s candles in debug path")
            return False, False, None
        closes = [float(c["close"]) for c in candles]
        highs = [float(c["high"]) for c in candles]
        lows = [float(c["low"]) for c in candles]
        opens = [float(c["open"]) for c in candles]
        mid = _ema(closes, 21)
        if len(mid) < 2:
            return False, False, None
        prev_mid, cur_mid = mid[-2], mid[-1]
        prev_close, cur_close = closes[-2], closes[-1]
        is_green = cur_close > opens[-1]
        is_red = cur_close < opens[-1]
        cross_up = prev_close < prev_mid and cur_close > cur_mid and is_green
        cross_down = prev_close > prev_mid and cur_close < cur_mid and is_red
        k_ok = cross_up or cross_down
        k_slow, d_slow = _stoch_slow(closes, highs, lows, 14, 3, 3)
        if len(k_slow) < 2 or len(d_slow) < 2:
            return k_ok, False, None
        prev_k, cur_k = k_slow[-2], k_slow[-1]
        prev_d, cur_d = d_slow[-2], d_slow[-1]
        bull = prev_k < prev_d and cur_k > cur_d and min(prev_k, cur_k) <= 20
        bear = prev_k > prev_d and cur_k < cur_d and max(prev_k, cur_k) >= 80
        zdir = _zigzag_last_direction(highs, lows, deviation=0.5, depth=13, backstep=3)
        return k_ok, (bull or bear), zdir
    except Exception:
        return False, False, None


async def _debug_strategy5_flags(qx, asset: str) -> tuple[bool, bool, float, bool, bool]:
    # returns (trend_up, trend_down, rsi7, pullback_up, pullback_down)
    try:
        # 30s context
        candles30 = await qx.get_candles(asset, time.time(), 30 * 200, 30)
        if not candles30 or len(candles30) < 60:
            return False, False, 0.0, False, False
        closes30 = [float(c["close"]) for c in candles30]
        opens30 = [float(c["open"]) for c in candles30]
        ema21_30 = _ema(closes30, 21)
        if len(ema21_30) < 3:
            return False, False, 0.0, False, False
        prev_close, cur_close = closes30[-2], closes30[-1]
        prev_mid, cur_mid = ema21_30[-2], ema21_30[-1]
        is_green = cur_close > opens30[-1]
        is_red = cur_close < opens30[-1]

        # 1m trend
        candles60 = await qx.get_candles(asset, time.time(), 60 * 300, 60)
        if not candles60 or len(candles60) < 60:
            return False, False, 0.0, False, False
        closes60 = [float(c["close"]) for c in candles60]
        ema50m = _ema(closes60, 50)
        if len(ema50m) < 2:
            return False, False, 0.0, False, False
        trend_up = closes60[-1] > ema50m[-1]
        trend_down = closes60[-1] < ema50m[-1]

        pullback_up = trend_up and prev_close <= prev_mid and cur_close > cur_mid and is_green
        pullback_down = trend_down and prev_close >= prev_mid and cur_close < cur_mid and is_red

        # RSI7 on 30s
        rsi30 = await qx.calculate_indicator(asset, "RSI", {"period": 7}, timeframe=30)
        rsi_vals = rsi30.get("rsi", [])
        rsi_cur = float(rsi_vals[-1]) if rsi_vals else 0.0

        return trend_up, trend_down, rsi_cur, pullback_up, pullback_down
    except Exception:
        return False, False, 0.0, False, False


async def find_first_signal(qx, strategy: int, min_payout: float, expiry_min: int, debug: bool = False) -> Tuple[str | None, str | None]:
    # Retry until instruments are ready
    for _ in range(5):
        assets = await get_all_assets(qx)
        if assets:
            break
        await asyncio.sleep(1)
    else:
        assets = []

    if debug:
        print(f"Scanning {len(assets)} assets...")

    # Single-asset isolation mode
    if SINGLE_ASSET_TEST:
        assets = [a for a in assets if a == SINGLE_ASSET_TEST]
        print(f"[DBG] SINGLE_ASSET_TEST={SINGLE_ASSET_TEST} -> {len(assets)} asset")

    # Session-based pause using historical win rate — relaxed/optional for Strategy 10
    if strategy == 10:
        # Strategy 10 keeps optional guard via S10_WR_GUARD_ENABLED
        if S10_WR_GUARD_ENABLED:
            ok_hist, hist_reason = should_trade_now(
                strategy,
                min_trades=S10_WR_MIN_TRADES,
                min_winrate=S10_WR_MIN_WINRATE,
                include_otc=True,
            )
            if not ok_hist:
                if debug:
                    print(f"Pausing due to weak recent performance: {hist_reason}")
                return None, None
    elif strategy == 11:
        # S11 hours filter removed
        pass
    else:
        ok_hist, hist_reason = should_trade_now(
            strategy,
            min_trades=10,
            min_winrate=0.60,
            include_otc=True,
        )
        if not ok_hist:
            if debug:
                print(f"Pausing due to weak recent performance: {hist_reason}")
            return None, None

    eligible = []
    for asset in assets:
        payout = await get_asset_payout(qx, asset, expiry_min)
        if payout >= min_payout:
            # Strategy 11: do not apply S11_EXCLUDE_ASSETS; keep general filtering only
            if strategy == 11:
                eligible.append(asset)
            else:
                disable_excl = os.environ.get("S11_EXCLUDE_DISABLE", "0").lower()
                if strategy == 11 and asset in S11_EXCLUDE_ASSETS and disable_excl in ("1", "true", "yes"):
                    eligible.append(asset)
                elif strategy == 11 and asset in S11_EXCLUDE_ASSETS:
                    pass
                else:
                    eligible.append(asset)
    if debug:
        print(f"Eligible by payout (>= {min_payout}%): {len(eligible)}")

    import time as _t
    for i, asset in enumerate(eligible):
        # Skip temporarily bad assets for S11
        if strategy == 11:
            until_ts = S11_BAD_ASSETS.get(asset, 0.0)
            if until_ts > _t.time():
                if debug:
                    print(f"Skipping {asset} (temp no-data, retry after {int(until_ts - _t.time())}s)")
                continue
        tradable = await ensure_asset_open(qx, asset)

        # Enforce asset-specific cooldown after losses
        # Note: For Strategy 11 (Sureshot), do not apply S11_EXCLUDE_ASSETS
        if strategy != 11 and tradable in S11_EXCLUDE_ASSETS:
            disable_excl = os.environ.get("S11_EXCLUDE_DISABLE", "0").lower()
            if disable_excl not in ("1", "true", "yes"):
                if debug:
                    print(f"Skipping {tradable} (S11_EXCLUDE_ASSETS)")
                continue

        import time as _t
        cooldown_until = _ASSET_COOLDOWN.get(tradable, 0)
        if cooldown_until > _t.time():
            if debug:
                print(f"Skipping {tradable} (cooldown active)")
            continue

        # Time-of-day filters removed for Strategy 11 per user request
        if strategy == 10:
            cur_hour_ist = int(datetime.now(ZoneInfo("Asia/Kolkata")).hour)
            if S10_ALLOWED_HOURS and cur_hour_ist not in S10_ALLOWED_HOURS:
                if debug:
                    print(f"Skipping {tradable} due to IST hour filter: {cur_hour_ist}")
                continue

        has_signal, direction = await decide_signal_and_direction(
            qx, tradable, strategy, timeframe=expiry_min * 60
        )
        if has_signal:
            if debug:
                print(f"Signal found on {tradable}: {direction}")
            # Capture S11 features for adaptive learning at signal time
            if strategy == 11 and S11_ADAPTIVE_ENABLE:
                try:
                    # Recompute quick indicators used in simplified logic for feature capture
                    tf = 60
                    candles = await _get_candles_safe(qx, tradable, tf, 40, ctx="s11_sig")
                    closes = [float(c["close"]) for c in candles] if candles else []
                    highs = [float(c["high"]) for c in candles] if candles else []
                    lows = [float(c["low"]) for c in candles] if candles else []
                    e2 = await qx.calculate_indicator(tradable, "EMA", {"period": 2}, timeframe=tf)
                    e7 = await qx.calculate_indicator(tradable, "EMA", {"period": 7}, timeframe=tf)
                    ema2 = e2.get("ema", []) if isinstance(e2, dict) else []
                    ema7 = e7.get("ema", []) if isinstance(e7, dict) else []
                    ks, ds = _stoch_slow(closes, highs, lows, 16, 3, 3) if closes and highs and lows else ([], [])
                    atr14 = _atr(highs, lows, closes, 14) if closes and highs and lows else []
                    rsi14 = _rsi(closes, 14) if closes else []
                    adx_vals = _adx(highs, lows, closes, 14) if closes and highs and lows else []
                    feat = _s11_capture_features(tradable, direction, candles or [], ema2, ema7, ks, ds, atr14, rsi14, adx_vals)
                    globals()["S11_LAST_FEATURES"] = feat
                except Exception as e:
                    _dbg(f"{tradable} s11 adaptive capture error: {e}")
            return tradable, direction
        if debug and strategy == 4 and i < 5:
            k_ok, s_ok, zdir = await _debug_strategy4_flags(qx, tradable)
            print(f"{tradable}: Keltner={k_ok}, Stoch={s_ok}, ZigZag={zdir}")
        if debug and strategy == 6 and i < 5:
            # Debug Strategy 6 conditions
            try:
                tf = 60
                candles = await qx.get_candles(tradable, time.time(), tf * 300, tf)
                if candles and len(candles) >= 60:
                    closes = [float(c["close"]) for c in candles]
                    highs = [float(c["high"]) for c in candles]
                    lows = [float(c["low"]) for c in candles]

                    # Check trend
                    ema50 = _ema(closes, 50)
                    trend_up = closes[-1] > ema50[-1] if ema50 else False
                    trend_down = closes[-1] < ema50[-1] if ema50 else False

                    # Check EMA cross
                    e5 = await qx.calculate_indicator(tradable, "EMA", {"period": 5}, timeframe=tf)
                    e20 = await qx.calculate_indicator(tradable, "EMA", {"period": 20}, timeframe=tf)
                    ema5 = e5.get("ema", [])
                    ema20 = e20.get("ema", [])
                    cross_up = cross_down = False
                    if len(ema5) >= 3 and len(ema20) >= 3:
                        prev_up = ema5[-2] > ema20[-2]
                        now_up = ema5[-1] > ema20[-1]
                        slope_ok_up = ema20[-1] > ema20[-2]
                        slope_ok_down = ema20[-1] < ema20[-2]
                        cross_up = not prev_up and now_up and trend_up and slope_ok_up
                        cross_down = prev_up and not now_up and trend_down and slope_ok_down

                    # Check RSI
                    rsi = await qx.calculate_indicator(tradable, "RSI", {"period": 14}, timeframe=tf)
                    rvals = rsi.get("rsi", [])
                    rsi_val = rvals[-1] if rvals else 0
                    rsi_ok_up = 45 <= rsi_val <= 70
                    rsi_ok_down = 30 <= rsi_val <= 55

                    # Check ATR distance
                    atr14 = _atr(highs, lows, closes, 14)
                    dist_ok = False
                    if atr14:
                        dist_up = (closes[-1] - ema20[-1]) if cross_up else 0.0
                        dist_down = (ema20[-1] - closes[-1]) if cross_down else 0.0
                        min_dist = 0.1 * atr14[-1]
                        dist_ok = (cross_up and dist_up >= min_dist) or (cross_down and dist_down >= min_dist)

                    # Check body ratio
                    last = candles[-1]
                    body = abs(float(last["close"]) - float(last["open"]))
                    rng = max(1e-6, float(last["high"]) - float(last["low"]))
                    body_ok = (body / rng) >= 0.4

                    print(f"{tradable}: Trend={trend_up}/{trend_down}, Cross={cross_up}/{cross_down}, RSI={rsi_val:.1f}({rsi_ok_up}/{rsi_ok_down}), Dist={dist_ok}, Body={body_ok}")
            except Exception as e:
                print(f"{tradable}: Debug failed: {e}")
        if debug and strategy == 7 and i < 5:
            # Debug Strategy 7 conditions
            try:
                tf = 30
                candles = await qx.get_candles(tradable, time.time(), tf * 200, tf)
                if candles and len(candles) >= 30:
                    closes = [float(c["close"]) for c in candles]
                    highs = [float(c["high"]) for c in candles]
                    lows = [float(c["low"]) for c in candles]
                    opens = [float(c["open"]) for c in candles]
                    mid = _ema(closes, 21)
                    if len(mid) >= 2:
                        prev_mid, cur_mid = mid[-2], mid[-1]
                        prev_close, cur_close = closes[-2], closes[-1]
                        is_green = cur_close > opens[-1]
                        is_red = cur_close < opens[-1]
                        cross_up = prev_close < prev_mid and cur_close > cur_mid and is_green
                        cross_down = prev_close > prev_mid and cur_close < cur_mid and is_red
                    else:
                        cross_up = cross_down = False
                    k_slow, d_slow = _stoch_slow(closes, highs, lows, 14, 3, 3)
                    bull = bear = False
                    if len(k_slow) >= 2 and len(d_slow) >= 2:
                        prev_k, cur_k = k_slow[-2], k_slow[-1]
                        prev_d, cur_d = d_slow[-2], d_slow[-1]
                        bull = prev_k < prev_d and cur_k > cur_d and min(prev_k, cur_k) <= 20
                        bear = prev_k > prev_d and cur_k < cur_d and max(prev_k, cur_k) >= 80
                    zz_dir = _zigzag_last_direction(highs, lows, deviation=0.5, depth=13, backstep=3)
                    last_pivot = _zigzag_last_pivot(highs, lows, deviation=0.5, depth=13, backstep=3)
                    touched = False
                    if last_pivot:
                        piv_idx, piv_price, piv_type = last_pivot
                        if piv_type == "H":
                            touched = highs[-1] >= piv_price or highs[-2] >= piv_price
                        else:
                            touched = lows[-1] <= piv_price or lows[-2] <= piv_price
                    print(f"{tradable}: S7 cross={cross_up}/{cross_down}, stoch={bull}/{bear}, zz={zz_dir}, touch={touched}")
            except Exception as e:
                print(f"{tradable}: Debug S7 failed: {e}")

    if debug:
        print("No signals this pass. Waiting...")

    return None, None


async def main():
    global S11_RECOVERY_PENDING
    colorama_init(autoreset=True)
    print(BANNER)

    try:
        from pyquotex.stable_api import Quotex
    except Exception as e:
        print(f"pyquotex not installed or import failed: {e}")
        sys.exit(1)

    # Use saved credentials from pyquotex settings/config.ini when available (prompts once on first run)
    try:
        from pyquotex.config import credentials as qx_credentials
    except Exception:
        qx_credentials = None

    if qx_credentials is not None:
        email, password = qx_credentials()
    else:
        email, password = prompt_credentials()

    account_mode = prompt_account_type()

    qx = Quotex(email=email, password=password, lang="en")
    qx.set_account_mode(account_mode)

    print("\nConnecting to Quotex...")
    ok, reason = await qx.connect()
    if not ok:
        print(Fore.YELLOW + f"Initial connect failed: {reason}. Attempting to re-authenticate..." + Style.RESET_ALL)
        try:
            await qx.reconnect()
            ok, reason = await qx.connect()
        except Exception as e:
            ok = False
            reason = f"Reconnect error: {e}"
    if not ok:
        print(Fore.RED + f"Connection failed: {reason}" + Style.RESET_ALL)
        sys.exit(2)

    start_balance = await show_balance(qx)

    # Recompute S11 allowed hours from recent logs (safe; falls back if no data)
    try:
        global S11_ALLOWED_HOURS
        S11_ALLOWED_HOURS = recompute_s11_allowed_hours_from_logs()
    except Exception as _e:
        print(f"[S11 Hours] Using default allowed hours (recompute failed): {_e}")


    # Optional candle data verification mode
    await run_verify_mode_if_requested(qx)

    # Config inputs per your process
    expiry_min = int(read_float("Enter expiry minutes (default 1): ", 1))
    # For Strategy 11, we'll use default 80% minimum payout unless user overrides later
    min_payout = read_float("Select assets with a minimum return percentage of (default 80): ", 80)

    profit_target = read_float("\nEnter your profit target: ")
    loss_target = read_float("Enter your Loss target: ")

    print("\nWhich Strategy You Want to Select")
    print("-> Strategy 18 (Confluence: EMA7/EMA3 + PSAR + Bears + Candle)")
    print("-> Strategy 17 (Signal Watcher: capture signalbot.exe logs)")

    print("-> Strategy 1")
    print("-> Strategy 2")
    print("-> Strategy 3")
    print("-> Strategy 4")
    print("-> Strategy 5 (Trend + Pullback + Momentum)")
    print("-> Strategy 6 (EMA 5/20 Pro: Trend+Slope+RSI+ATR+Body)")
    print("-> Strategy 7 (ZigZag+Keltner21+Stoch 14,3,3 on 30s)")
    print("-> Strategy 8 (Data-Discovered from Research)")
    print("-> Strategy 9 (Multi-indicator Alignment)")
    print("-> Strategy 10 (EMA11/55 + PSAR + Williams %R Confluence)")
    print("-> Strategy 11 (Sureshot: EMA5 Breakout + Retest + Error Candle)")
    print("-> Strategy 12 (AI/ML Auto-Strategy Generator - Phase 1 Rule Search)")
    print("-> Strategy 13 (S12 Strict95 Live Runner)")
    print("-> Strategy 14 (AI Live Market Analyzer - 95%+ Target)")
    print("-> Strategy 15 (Hybrid: Scheduled Signal Follower + Live Analyzer)")
    print("-> Strategy 16 (Ultra-Selective 98% WR Target)")
    print("-> Strategy 17 (Signal Watcher: capture signalbot.exe logs)")
    print("-> Strategy 18 (Confluence: EMA7/EMA3 + PSAR + Bears Power + Candle)")
    print("-> Strategy 19 (Telegram Signal Follower)")

    try:
        strategy = int(input("Enter Your Strategy Number(1-19): ").strip())
    except Exception:
        strategy = 1




    # Strategy 17: run capture-only mode (no trading)
    if strategy == 17:
        if s17_run_capture is None:
            print(Fore.RED + "S17 not available (import)." + Style.RESET_ALL)
            await qx.close()
            return
        print("\n[S17] Signal Watcher starting... Press Ctrl+C to stop.")
        try:
            await s17_run_capture(S17Config())
        finally:
            try:
                await qx.close()
            except Exception:
                pass
        return

    # Strategy 19: Telegram signal follower (auto multi-round)
    if strategy == 19:
        try:
            from telegram_signal_live import S19Config, Strategy19Follower
        except Exception:
            try:
                from .telegram_signal_live import S19Config, Strategy19Follower  # type: ignore
            except Exception:
                Strategy19Follower = None  # type: ignore
                S19Config = None  # type: ignore
        if not (Strategy19Follower and S19Config):
            print(Fore.RED + "[S19] Import failed." + Style.RESET_ALL)
            await qx.close(); return
        # Inputs
        try:
            min_forecast = read_float("Min forecast percentage (default 70): ", 70)
        except Exception:
            min_forecast = 70
        base_amount = read_float("Fixed stake amount (default 5): ", 5)
        api_id = os.environ.get("TELEGRAM_API_ID") or input("api_id: ").strip()
        api_hash = os.environ.get("TELEGRAM_API_HASH") or input("api_hash: ").strip()
        phone = os.environ.get("TELEGRAM_PHONE") or input("phone (+cc): ").strip()
        group = os.environ.get("TELEGRAM_GROUP") or input("group (name/id): ").strip()
        session_name = os.environ.get("TELEGRAM_SESSION", "s19_session")
        lead_s = int(os.environ.get("S19_LEAD_S", "5") or 5)
        cooldown_s = int(os.environ.get("S19_COOLDOWN_S", "90") or 90)
        grace_s = int(os.environ.get("S19_GRACE_S", "8") or 8)
        tz_offset_min = int(os.environ.get("S19_TZ_OFFSET_MIN", "-180") or -180)
        # Env toggles
        multi_round = os.environ.get("S19_MULTI_ROUND", "1").lower() in ("1","true","yes")
        auto_continue = os.environ.get("S19_AUTO_CONTINUE", "1").lower() in ("1","true","yes")
        stop_on_loss = os.environ.get("S19_STOP_ON_LOSS", "1").lower() in ("1","true","yes")
        max_attempts = int(os.environ.get("S19_MAX_ATTEMPTS", "3") or 3)
        recover_mode = os.environ.get("S19_RECOVER", "1").lower() in ("1","true","yes")
        recover_profit_factor = float(os.environ.get("S19_RECOVER_PROFIT_FACTOR", "0.8") or 0.8)
        exact_open = os.environ.get("S19_EXACT_OPEN", "1").lower() in ("1","true","yes")
        conf_2x = float(os.environ.get("S19_CONF_2X", "75") or 75)
        conf_3x = float(os.environ.get("S19_CONF_3X", "82") or 82)
        max_conf_mult = int(os.environ.get("S19_MAX_CONF_MULT", "3") or 3)
        conf_apply_attempts = os.environ.get("S19_CONF_APPLY_ATTEMPTS", "first").lower()
        conf_allow_recover = os.environ.get("S19_CONF_ALLOW_RECOVER", "0").lower() in ("1","true","yes")
        conf_cap_mult = float(os.environ.get("S19_CONF_CAP_MULT", "10") or 10)
        strict_recover = os.environ.get("S19_RECOVER_STRICT", "1").lower() in ("1","true","yes")
        strict_after = int(os.environ.get("S19_STRICT_AFTER", "3") or 3)
        fudge_ms = int(os.environ.get("S19_FUDGE_MS", "120") or 120)
        candle_fetch_retry = int(os.environ.get("S19_CANDLE_FETCH_RETRY", "3") or 3)
        max_stake_abs = float(os.environ.get("S19_MAX_STAKE", "0") or 0)  # hard ceiling per attempt
        bal_pct_cap = float(os.environ.get("S19_BAL_PCT_CAP", "0") or 0)   # % of current balance
        # New control flags (user customization)
        force_escalate = os.environ.get("S19_FORCE_ESCALATE", "1").lower() in ("1","true","yes")
        attempt_stake_cap = float(os.environ.get("S19_ATTEMPT_STAKE_CAP", "1000") or 1000)
        reset_recover_each_round = os.environ.get("S19_RESET_RECOVER_EACH_ROUND", "1").lower() in ("1","true","yes")
        # Risk distribution settings (simplified - removed risk_max_trades)
        risk_linear = os.environ.get("S19_RISK_LINEAR", "1").lower() in ("1","true","yes")  # legacy flag
        risk_mode = os.environ.get("S19_RISK_MODE", "remaining").lower()
        safe_recovery_trades = int(os.environ.get("S19_SAFE_RECOVERY_TRADES", "12") or 12)  # for risk_mode 'safe'
        # risk_mode options:
        #   linear    -> average distribution (divide remaining capacity by (trades_done+1) heuristic)
        #   remaining -> only cap by remaining loss capacity (allows 200,400,800 recovery ladder if within limit)
        #   off       -> no extra cap beyond loss_target hard stop

        # New recovery tuning (partial_recover removed – always full recover logic)
        recover_portion = float(os.environ.get("S19_RECOVER_PORTION", "0.55") or 0.55)  # retained for future but unused now
        outstanding_cap_mult = float(os.environ.get("S19_OUT_CAP_MULT", "6") or 6)  # cap first-attempt recovery stake to base * this
        attempt_mults_raw = os.environ.get("S19_ATTEMPT_MULTS", "")
        attempt_mults: list[float] = []
        if attempt_mults_raw.strip():
            try:
                attempt_mults = [float(x) for x in attempt_mults_raw.split(',') if x.strip()]
            except Exception:
                attempt_mults = []
        else:
            # Default progression for recovery ladder if user hasn't specified: 1x,2x,4x
            attempt_mults = [1,2,4]
        recover_style = os.environ.get("S19_RECOVER_STYLE", "formula").lower()  # formula | ladder

        try:
            from tabulate import tabulate as _tab
        except Exception:
            _tab = None  # type: ignore

        round_summaries: list[dict] = []
        total_agg = 0.0
        round_index = 0
        s19_outstanding_recover = 0.0
        s19_consec_loss_signals = 0
        # Strict mode tracking (streak counts signals while strict active)
        s19_strict_streak = 0
        s19_last_strict = False
        safe_countdown: int | None = None  # initialized when first net losing signal starts safe distribution
        # After a full 3-attempt loss on a signal, skip the next 2 signals entirely
        skip_next_signals: int = 0
        # Flag current round ended due to a triple-loss (all attempts of one signal failed)
        triple_loss_round_end: bool = False

        async def _run_round():
            nonlocal round_index, total_agg, s19_outstanding_recover, s19_consec_loss_signals, triple_loss_round_end, safe_countdown
            round_index += 1
            # Reset triple-loss flag at start of each round
            triple_loss_round_end = False
            # Optional reset of recovery pools each round (user requested behavior)
            if reset_recover_each_round:
                s19_outstanding_recover = 0.0
                s19_consec_loss_signals = 0
                safe_countdown = None
            # Round start banner
            print(
                Fore.CYAN
                + f"[S19] ===== Round {round_index} start (target {profit_target}) ====="
                + Style.RESET_ALL
            )
            rows: List[List] = []
            pnl = 0.0
            trade_no = 0
            headers = [
                "#",
                "Asset",
                "Dir",
                "Try",
                "Amt",
                "Res",
                "Δ",
                "Cum P/L",
            ]
            follower: Optional[Strategy19Follower] = None  # will assign below
            round_stop_triggered = False

            async def _exec_sig(sig):
                nonlocal pnl, trade_no, s19_outstanding_recover, s19_consec_loss_signals, rows, follower, round_stop_triggered, s19_strict_streak, s19_last_strict, safe_countdown, skip_next_signals, triple_loss_round_end
                base = float(base_amount)
                cumulative_loss = 0.0
                attempt = 0
                trade_epoch0 = sig.trade_epoch or int(time.time())

                # If round already marked to stop, ignore further signals
                if round_stop_triggered:
                    return pnl, trade_no, rows

                # Skip signals if instructed (post 3-try full loss)
                if skip_next_signals > 0:
                    # Yellow skip notification (post 3-try loss cool-off)
                    print(
                        Fore.YELLOW
                        + (
                            f"[S19][SAFE] Skip signal (cool-off after 3-try loss). "
                            f"Remaining skips={skip_next_signals}"
                        )
                        + Style.RESET_ALL
                    )
                    skip_next_signals -= 1
                    return pnl, trade_no, rows

                pnl_start_signal = pnl
                attempts_all_loss_for_signal = True

                def _next_minute_epoch(now: float | None = None) -> float:
                    t = time.time() if now is None else now
                    return (int(t // 60) * 60) + 60

                async def _resolve_with_candle(asset: str, direction: str):
                    # Try up to candle_fetch_retry * 3 (extended) to avoid FAIL_TRACK
                    for _ in range(candle_fetch_retry * 3):
                        try:
                            candles = await _get_candles_safe(qx, asset, 60, 3, ctx="s19_res")
                            if candles:
                                c = candles[-1]
                                o = float(c.get('open') or c.get('o') or 0)
                                cl = float(c.get('close') or c.get('c') or 0)
                                if abs(cl - o) < 1e-9:
                                    return "DRAW", 0.0
                                if direction == 'call':
                                    return ("WIN" if cl > o else "LOSS", cl - o)
                                return ("WIN" if cl < o else "LOSS", o - cl)
                        except Exception:
                            pass
                        await asyncio.sleep(0.25)
                    # Fallback: treat as DRAW (neutral) instead of FAIL_TRACK to prevent false recovery escalation
                    return "DRAW", 0.0

                async def _wait_until(ts: float):
                    loop = asyncio.get_running_loop()
                    target = ts - (fudge_ms / 1000)
                    while True:
                        now = time.time()
                        if now >= target:
                            break
                        rem = target - now
                        await asyncio.sleep(min(0.1, max(0.0, rem)))

                while attempt < max_attempts:
                    # Fixed schedule: attempts at base minute + n*60s (e.g. 0, +60, +120)
                    target_epoch = trade_epoch0 + attempt * 60
                    entry_ts = target_epoch if exact_open else (target_epoch - sig.entry_lead_s)
                    await _wait_until(entry_ts)
                    strict_active = strict_recover and (s19_consec_loss_signals >= strict_after)
                    # Stake logic
                    if attempt == 0:
                        if recover_style == 'ladder':
                            stake = base  # first attempt base
                        elif recover_mode and s19_outstanding_recover > 0:
                            try:
                                p_pct = await get_asset_payout(qx, sig.asset, 1)
                            except Exception:
                                p_pct = 80.0
                            eff = max(0.01, p_pct / 100.0)
                            # Always attempt full recovery + profit portion (partial_recover removed)
                            goal = s19_outstanding_recover if strict_active else (s19_outstanding_recover + base * recover_profit_factor)
                            stake = goal / eff
                            stake_cap = base * outstanding_cap_mult if outstanding_cap_mult > 0 else base * 10
                            stake = max(base, min(stake, stake_cap))
                        else:
                            stake = base
                    else:
                        if recover_style == 'ladder':
                            # Use attempt multipliers strictly for ladder (e.g., 1,2,4) ignoring recovery formula
                            m = attempt_mults[min(attempt, len(attempt_mults)-1)] if attempt_mults else (2 ** attempt)
                            stake = base * m
                        elif not recover_mode:
                            if attempt_mults:
                                m = attempt_mults[min(attempt, len(attempt_mults)-1)]
                                stake = base * m
                            else:
                                stake = base
                        else:
                            try:
                                p_pct = await get_asset_payout(qx, sig.asset, 1)
                            except Exception:
                                p_pct = 80.0
                            eff = max(0.01, p_pct / 100.0)
                            # Always attempt full recovery of cumulative loss + profit portion (partial_recover removed)
                            goal = cumulative_loss if strict_active else (cumulative_loss + base * recover_profit_factor)
                            stake = goal / eff
                            # Do not cap by attempt multiplier in formula recovery; need precise goal/eff stake
                            stake_cap = base * outstanding_cap_mult if outstanding_cap_mult > 0 else base * 10
                            stake = max(base, min(stake, stake_cap))
                    # Forced escalation override (after a losing first attempt) BEFORE confidence & risk caps
                    if force_escalate and attempt > 0 and cumulative_loss > 0:
                        try:
                            p_pct_force = await get_asset_payout(qx, sig.asset, 1)
                        except Exception:
                            p_pct_force = 80.0
                        eff_force = max(0.01, p_pct_force / 100.0)
                        # Goal: recover cumulative loss + one base profit portion
                        goal_force = cumulative_loss + (base * recover_profit_factor)
                        stake_needed = goal_force / eff_force
                        if attempt_stake_cap > 0:
                            stake_needed = min(stake_needed, attempt_stake_cap)
                        if stake < stake_needed:
                            stake = stake_needed
                    # confidence multiplier
                    conf_mult = 1
                    fpct = getattr(sig, 'forecast_pct', None)
                    if fpct is not None:
                        try:
                            v = float(fpct)
                            if max_conf_mult >= 3 and v >= conf_3x:
                                conf_mult = 3
                            elif max_conf_mult >= 2 and v >= conf_2x:
                                conf_mult = 2
                        except Exception:
                            pass
                    if conf_mult > 1 and not (strict_active and (s19_outstanding_recover > 0 or cumulative_loss > 0)):
                        if conf_apply_attempts == 'all' or (conf_apply_attempts == 'first' and attempt == 0):
                            if conf_allow_recover or s19_outstanding_recover == 0:
                                stake *= conf_mult
                    stake = min(stake, base * conf_cap_mult)
                    if max_stake_abs > 0:
                        stake = min(stake, max_stake_abs)
                    if bal_pct_cap > 0:
                        try:
                            bal_now = await qx.get_balance()
                            stake = min(stake, bal_now * bal_pct_cap / 100.0)
                        except Exception:
                            pass
                    # Risk distribution capping (skip safe reductions if force_escalate on higher attempts)
                    if loss_target > 0 and risk_mode != 'off':
                        remaining_loss_capacity = loss_target + pnl  # pnl negative when losing
                        if remaining_loss_capacity <= 0:
                            print(f"[S19][RISK] Remaining loss capacity exhausted ({remaining_loss_capacity:.2f}). Stop attempts.")
                            break
                        if not (force_escalate and attempt > 0):
                            # Determine if safe mode should be active:
                            safe_active = (risk_mode == 'safe') or (s19_consec_loss_signals >= 3) or (safe_countdown is not None and safe_countdown > 0)
                            if risk_mode == 'linear' and risk_linear and not safe_active:
                                trades_done = trade_no
                                heuristic_remaining_slots = max(1, 3 - (attempt))  # simple heuristic; prevents divide by zero
                                risk_cap_stake = remaining_loss_capacity / heuristic_remaining_slots
                                if risk_cap_stake < stake:
                                    orig = stake
                                    stake = max(0.01, risk_cap_stake)
                                    print(f"[S19][RISK] Linear cap {orig:.2f}->{stake:.2f} rem_loss={remaining_loss_capacity:.2f} slots={heuristic_remaining_slots}")
                            elif risk_mode == 'remaining' and not safe_active:
                                if stake > remaining_loss_capacity:
                                    orig = stake
                                    stake = max(0.01, remaining_loss_capacity)
                                    print(f"[S19][RISK] Rem-cap {orig:.2f}->{stake:.2f} rem_loss={remaining_loss_capacity:.2f}")
                            if safe_active:
                                if safe_countdown is None:
                                    safe_countdown = max(1, safe_recovery_trades)
                                if safe_countdown > 0:
                                    safe_cap = remaining_loss_capacity / safe_countdown
                                    if stake > safe_cap:
                                        orig = stake
                                        stake = max(0.01, safe_cap)
                                        print(f"[S19][RISK] Safe-cap {orig:.2f}->{stake:.2f} rem_loss={remaining_loss_capacity:.2f} slots={safe_countdown}")
                        else:
                            # Ensure not exceeding remaining loss capacity under force escalation
                            if stake > remaining_loss_capacity:
                                stake = max(0.01, remaining_loss_capacity)
                    # Final cap: per-attempt custom cap
                    if attempt_stake_cap > 0:
                        if stake > attempt_stake_cap:
                            stake = attempt_stake_cap
                    try:
                        payout_live = await get_asset_payout(qx, sig.asset, 1)
                    except Exception:
                        payout_live = 0.0
                    print(f"[S19] EXEC {sig.asset} {sig.direction} try={attempt+1}/{max_attempts} stake={stake:.2f} pay~{payout_live} fore={fpct}")
                    # Allow full lifecycle resolution (up to expiry + headroom) so we get definitive status
                    exec_timeout = 60 + 35  # 60s expiry + margin
                    try:
                        won, delta = await asyncio.wait_for(
                            place_and_wait(qx, float(stake), sig.asset, sig.direction, 60),
                            timeout=exec_timeout,
                        )
                    except asyncio.TimeoutError:
                        won, delta = False, 0.0

                    status = str(getattr(qx, 'last_exec_status', '')).lower()
                    op_id = None
                    try:
                        op_id = getattr(qx.api, 'buy_id', None)
                    except Exception:
                        op_id = None
                    # Post-expiry polling if still unknown
                    if status not in ("win", "loss", "draw"):
                        for _ in range(6):  # ~3s extra
                            await asyncio.sleep(0.5)
                            status = str(getattr(qx, 'last_exec_status', '')).lower()
                            if status in ("win","loss","draw"):
                                break
                    result: str | None = None
                    if status in ("win","loss","draw"):
                        result = "WIN" if status == "win" else ("LOSS" if status == "loss" else "DRAW")
                    else:
                        # Fallback history probe
                        try:
                            hist = await qx.get_history()
                            if isinstance(hist, list):
                                now_ts = time.time()
                                for it in hist[:12]:
                                    sym = it.get('asset') or it.get('symbol') or it.get('pair')
                                    amt = float(it.get('amount', it.get('value', 0)) or 0)
                                    ts = float(it.get('time') or it.get('closeTimestamp') or 0)
                                    if sym == sig.asset and abs(amt - float(stake)) < 1e-3 and (now_ts - ts) < 120:
                                        st = str(it.get('status') or it.get('result') or '').lower()
                                        if st in ("win","won"):
                                            result = "WIN"
                                        elif st in ("loss","lost"):
                                            result = "LOSS"
                                        elif st in ("draw","equal","tie"):
                                            result = "DRAW"
                                        if result:
                                            break
                        except Exception:
                            pass
                    # Candle fallback only if still unknown
                    if result is None:
                        result, _cd = await _resolve_with_candle(sig.asset, sig.direction)
                        if result == "DRAW":
                            # Try one more fetch if open==close
                            await asyncio.sleep(0.6)
                            r2, _cd2 = await _resolve_with_candle(sig.asset, sig.direction)
                            if r2 in ("WIN","LOSS"):
                                result = r2
                    # Normalize delta based on final result if ambiguous
                    if result == "WIN" and delta <= 0:
                        try:
                            eff = payout_live or await get_asset_payout(qx, sig.asset, 1)
                        except Exception:
                            eff = 80.0
                        delta = float(stake) * (eff / 100.0)
                    elif result == "LOSS" and delta >= 0:
                        delta = -float(stake)
                    elif result == "DRAW":
                        delta = 0.0
                    print(f"[S19][RESOLVE] status={status} op_id={op_id} final={result} delta={delta:.2f} stake={stake:.2f} pnl_after={pnl+delta:.2f}")
                    trade_no += 1
                    pnl += float(delta)
                    if result in ("WIN", "DRAW"):
                        attempts_all_loss_for_signal = False
                    if delta < 0:
                        cumulative_loss += -float(delta)
                    elif result == 'WIN':
                        cumulative_loss = 0.0
                    rows.append([trade_no, sig.asset, sig.direction.upper(), attempt+1, round(stake,2), result, round(delta,2), round(pnl,2)])
                    if _tab:
                        print(_tab(rows[-12:], headers=headers, tablefmt='github'))
                    else:
                        print("# Asset Dir Try Amt Res Δ CumPnl")
                        for r in rows[-5:]:
                            print(" ".join(str(x) for x in r))
                    try:
                        log_trade_row(sig.asset, 19, sig.direction, float(stake), result, float(delta), float(payout_live), 1, qx.get_account_mode() if hasattr(qx,'get_account_mode') else 'PRACTICE')
                    except Exception:
                        pass
                    # stop conditions
                    # Stop further attempts unless true LOSS and still under round targets
                    if pnl >= profit_target or -pnl >= loss_target or result != "LOSS":
                        break
                    attempt += 1
                # update recovery pools after signal using NET result across all attempts of this signal
                sig_net_total = pnl - pnl_start_signal
                if sig_net_total < 0:  # net losing signal
                    s19_outstanding_recover += (-sig_net_total)
                    s19_consec_loss_signals += 1
                    # Initialize safe mode automatically after threshold of consecutive losing signals
                    if ((risk_mode == 'safe') or (s19_consec_loss_signals >= 3) or attempts_all_loss_for_signal) and (safe_countdown is None or safe_countdown <= 0):
                        safe_countdown = max(1, safe_recovery_trades)
                    # If all attempts for this signal were losses, skip the next 2 signals to cool off before recovery
                    if attempts_all_loss_for_signal:
                        # Full 3-try loss on this signal -> end round immediately & schedule skips
                        print(
                            Fore.RED
                            + (
                                f"[S19][SAFE] 3-try full LOSS on {sig.asset} net={sig_net_total:.2f} "
                                f"pnl={pnl:.2f} -> ending round & skipping next 2 signals"
                            )
                            + Style.RESET_ALL
                        )
                        skip_next_signals = max(skip_next_signals, 2)
                        triple_loss_round_end = True
                        round_stop_triggered = True
                        try:
                            if follower and getattr(follower, 'client', None):
                                await follower.client.disconnect()  # type: ignore
                        except Exception:
                            pass
                elif sig_net_total > 0:  # net winning / recovering signal
                    s19_outstanding_recover = max(0.0, s19_outstanding_recover - sig_net_total)
                    if s19_outstanding_recover <= 0:
                        s19_consec_loss_signals = 0
                # Decrement safe_countdown after every trade in safe mode
                if (((risk_mode == 'safe') or (s19_consec_loss_signals >= 3)) or (safe_countdown is not None and safe_countdown > 0)) and safe_countdown is not None and safe_countdown > 0:
                    safe_countdown -= 1
                # One-time strict mode activation notice
                strict_active_now = strict_recover and (s19_consec_loss_signals >= strict_after)
                if strict_active_now and not getattr(globals(), 'S19_STRICT_ANNOUNCED', False):
                    print(Fore.MAGENTA + f"[S19] STRICT FULL RECOVERY ON (outstanding={s19_outstanding_recover:.2f})" + Style.RESET_ALL)
                    globals()['S19_STRICT_ANNOUNCED'] = True
                # Per-signal strict mode streak logging
                if strict_active_now:
                    if not s19_last_strict:
                        s19_strict_streak = 0
                    s19_strict_streak += 1
                    print(f"[S19] StrictFull outstanding={s19_outstanding_recover:.2f} streak={s19_strict_streak}")
                else:
                    if s19_last_strict:
                        s19_strict_streak = 0
                s19_last_strict = strict_active_now
                print(f"[S19] RecoverPool={s19_outstanding_recover:.2f} LossSignals={s19_consec_loss_signals}")
                # If round target hit, disconnect follower to end this round immediately
                if not round_stop_triggered and (pnl >= profit_target or -pnl >= loss_target):
                    round_stop_triggered = True
                    try:
                        bal = await show_balance(qx)
                    except Exception:
                        bal = None
                    if pnl >= profit_target:
                        print(Fore.GREEN + f"[S19] Profit target reached (PnL={pnl:.2f}). Balance={bal}" + Style.RESET_ALL)
                    else:
                        print(Fore.RED + f"[S19] Loss limit hit (PnL={pnl:.2f}). Balance={bal}" + Style.RESET_ALL)
                    try:
                        if follower and getattr(follower, 'client', None):
                            await follower.client.disconnect()  # type: ignore
                    except Exception:
                        pass
                return pnl, trade_no, rows

            cfg = S19Config(api_id=int(api_id), api_hash=api_hash, phone=phone, group=group,
                            session_name=session_name, min_forecast=float(min_forecast),
                            lead_s=lead_s, cooldown_same_asset_s=cooldown_s,
                            allow_past_grace_s=grace_s, trade=True, tz_offset_min=tz_offset_min)
            follower = Strategy19Follower(cfg, execute_cb=_exec_sig)
            pnl_before = 0.0
            try:
                await follower.start()
            except KeyboardInterrupt:
                raise
            finally:
                pass
            # Round finished when follower.stop triggered inside exec callback (disconnect) or manual stop
            return pnl, trade_no, rows

        try:
            while True:
                pnl_round, trades_round, rows_round = await _run_round()
                outcome = (
                    "LOSS_LIMIT"
                    if -pnl_round >= loss_target
                    else "PROFIT_TARGET"
                    if pnl_round >= profit_target
                    else "TRIPLE_LOSS"
                    if 'triple_loss_round_end' in locals() and triple_loss_round_end
                    else "COMPLETE"
                )
                round_summaries.append({"round": round_index, "trades": trades_round, "pnl": pnl_round, "outcome": outcome})
                total_agg += pnl_round
                # Outcome reason annotation for round end
                reason_msg = ""
                if outcome == "LOSS_LIMIT":
                    reason_msg = " (ended: loss limit)"
                elif outcome == "PROFIT_TARGET":
                    reason_msg = " (ended: profit target)"
                elif outcome == "TRIPLE_LOSS":
                    reason_msg = " (ended: 3-try loss)"
                else:
                    reason_msg = " (ended: manual/normal)"
                print(
                    Fore.MAGENTA
                    + (
                        f"[S19] Round {round_index} done pnl={pnl_round:.2f} "
                        f"total={total_agg:.2f}{reason_msg}"
                    )
                    + Style.RESET_ALL
                )
                if (-pnl_round >= loss_target and stop_on_loss) or not multi_round:
                    break
                if not auto_continue:
                    ans = input("[S19] Continue next round? (Y/n): ").strip().lower()
                    if ans == 'n':
                        break
        except KeyboardInterrupt:
            print(Fore.YELLOW + "\n[S19] Stopped by user." + Style.RESET_ALL)
        # Summary
        print(Fore.MAGENTA + "\n[S19] ===== Session Summary =====" + Style.RESET_ALL)
        if round_summaries:
            if _tab:
                print(
                    _tab(
                        [
                            [r['round'], r['trades'], f"{r['pnl']:.2f}", r['outcome']]
                            for r in round_summaries
                        ],
                        headers=["Round", "Trades", "PnL", "Outcome"],
                        tablefmt='github',
                    )
                )
            else:
                for r in round_summaries:
                    print(
                        f"Round {r['round']} Trades={r['trades']} PnL={r['pnl']:.2f} Outcome={r['outcome']}"
                    )
            print(
                Fore.CYAN
                + f"[S19] Total Rounds={len(round_summaries)} Aggregated PnL={total_agg:.2f}"
                + Style.RESET_ALL
            )
        else:
            print("No rounds executed.")
        return

    # Enforce 1-minute expiry for Strategy 11 as per design
    if strategy == 11 and expiry_min != 1:
        print(Fore.YELLOW + "Strategy 11 uses 1-minute expiry. Overriding your input to 1 minute." + Style.RESET_ALL)
        expiry_min = 1


    print("\nTrading type\n1. Compounding\n2. Martingale")
    t = input("Enter your trading type(1/2): ").strip()

    comp_amounts: List[float] = []
    base_amount = 5.0
    steps = 3
    mult = 2.0

    if t == "1":
        raw = input("Enter compounding amounts (comma-separated, e.g., 20,38,70): ").strip()
        if raw:
            comp_amounts = [float(x) for x in raw.split(',') if x.strip()]
        else:
            comp_amounts = [20, 38, 70]
    else:
        base_amount = read_float("Enter base amount: ", 5)
        steps = int(read_float("Enter Martingale steps: ", 3))
        mult = read_float("Enter Martingale multiplier: ", 2.0)


    # Strategy 12: run pipeline and paper-validate in PRACTICE before normal loop
    if strategy == 12:
        if s12_run_pipeline is None or S12Config is None or RuleVariant is None:
            print(Fore.RED + "S12 not available (import error)." + Style.RESET_ALL)
            await qx.close()
            return
        try:
            qx.set_account_mode("PRACTICE")
        except Exception:
            pass
        allowed_hours = set(S10_ALLOWED_HOURS) if 'S10_ALLOWED_HOURS' in globals() else None
        cfg = S12Config(
            timeframes_s=[15, 30, 60],
            min_payout=float(min_payout),
            target_accuracy=0.95,
            min_trades=10,
            ist_hours=allowed_hours,
            max_variants=32,
            bars_per_tf=2400,
            confluence_mode=False,
            # Diagnostics ON for multi-TF analysis
            diagnostic_mode=True,
            diag_disable_hours=True,
            diag_oos_frac=0.2,
            diag_show_all=True,
        )
        from strategy12.cli import interactive_select_variant as s12_select
        chosen = await s12_select(qx, cfg)
        if not chosen:
            await qx.close()
            return
        from strategy12.deploy import find_first_signal_s12
        print("\n[S12] PRACTICE validation running...")
        # Determine PRACTICE hour filter from selected variant (if provided)
        sel_hours = None
        try:
            sel_hours = chosen.get('params', {}).get('allowed_hours')
            if sel_hours and isinstance(sel_hours, (list, tuple)):
                sel_hours = set(int(h) for h in sel_hours)
            else:
                sel_hours = None
        except Exception:
            sel_hours = None
        # Hour filter visibility gated behind verbose flag
        verbose_practice_hours = bool(getattr(cfg, 'verbose_practice_hours', False))
        if sel_hours and verbose_practice_hours:
            print(f"[S12] PRACTICE hour filter: using allowed_hours={sorted(list(sel_hours))}")
        elif verbose_practice_hours:
            print("[S12] PRACTICE hour filter: OFF (no allowed_hours on variant)")

        idle = 0
        rows12: List[List] = []
        trade12 = 0
        headers12 = ["#", "Asset", "Dir", "Amt", "Result", "P/L"]
        target_trades = 20
        s12_loss_streak = 0  # pause after consecutive losses

        # Maintain a blacklist of assets with repeated execution failures
        exec_blacklist: set[str] = set()
        while True:
            # Pass cooldowns dict to scanning to avoid immediate reselection of failed assets
            try:
                exec_cooldowns
            except NameError:
                exec_cooldowns = {}
            # Live hour override: bypass variant hour restrictions in PRACTICE if enabled
            rp = dict(chosen.get('params', {}))
            if bool(getattr(cfg, 'live_hour_override', False)):
                rp['live_hour_override'] = True
                if getattr(cfg, 'current_ist_hour_force', None) is not None:
                    rp['current_ist_hour_force'] = int(getattr(cfg, 'current_ist_hour_force'))
                sel_hours = None

            asset, direction = await find_first_signal_s12(
                qx,
                rule=RuleVariant(name=chosen['variant'], params=rp),
                min_payout=85.0,
                timeframes_s=[60, 30, 15],
                allowed_hours=sel_hours,
                debug=True,
                blacklist=exec_blacklist,
                cooldowns=exec_cooldowns,
                strict95_mode=bool(getattr(cfg, 'strict95_mode', False)),
                confluence_mode=str(getattr(cfg, 'strict95_confluence_mode', 'off')),
                gate_body_min=float(getattr(cfg, 'strict95_pretrade_body_min', 0.33)),
                gate_atr_lo=float(getattr(cfg, 'strict95_pretrade_atr_band', (35.0, 80.0))[0] if hasattr(cfg, 'strict95_pretrade_atr_band') else 35.0),
                gate_atr_hi=float(getattr(cfg, 'strict95_pretrade_atr_band', (35.0, 80.0))[1] if hasattr(cfg, 'strict95_pretrade_atr_band') else 80.0),
                min_conf_override=float(getattr(cfg, 'strict95_min_conf', 0.0) or 0.0),
            )
            if not asset:
                idle += 1
                if idle % 5 == 0:
                    print("[S12] No signals yet; scanning continues...")
                await asyncio.sleep(1)
                continue
            idle = 0

            # Dynamic confidence threshold by payout tier
            payout = await get_asset_payout(qx, asset, expiry_min)
            if payout < 85.0:
                if True:
                    print(f"[S12] Skip {asset}: payout {payout:.0f}% < 85%")
                continue
            min_conf = 0.60 if payout >= 90.0 else 0.64

            # Multi-timeframe pretrade confirmation (60s primary)
            pre_ok, score01, details = await s12_pretrade_confirm(qx, asset, direction)
            if not pre_ok or score01 < min_conf:
                print(f"[S12] Skip {asset}: pretrade gate failed (score={score01:.2f} < min_conf={min_conf:.2f})")
                continue

            # Show we are placing a paper trade
            print(f"[S12] Paper trade on {asset} dir={direction} amt={comp_amounts[0] if t=='1' and comp_amounts else base_amount}")
            if t == "1":
                amount = comp_amounts[max(0, trade12 % len(comp_amounts))] if comp_amounts else 20.0
            else:
                amount = base_amount * (mult ** min(0, steps - 1)) if steps > 0 else base_amount
            # Availability snapshot before placing
            try:
                tradable_snapshot = await qx.is_tradable(asset) if hasattr(qx, 'is_tradable') else True
            except Exception:
                tradable_snapshot = True

            # Measure resolve latency and pre-timeout counters
            import time as _t
            t_start = _t.time()
            prev_timeouts = int(globals().get('S12_TIMEOUT_FALLBACKS', 0))

            won, delta = await place_and_wait(qx, amount, asset, direction, expiry_min * 60)
            latency = _t.time() - t_start

            # Classify result with execution checks
            last_status = str(getattr(qx, "last_exec_status", "")).lower()
            last_failed = bool(getattr(qx, "last_exec_failed", False))
            # Enhanced debug: op_id and delta source
            op_id = None
            try:
                op_id = getattr(qx.api, 'buy_id', None)
            except Exception:
                op_id = None
            delta_source = (
                "hist" if last_status in ("win", "loss", "won", "lost", "draw", "equal", "tie") and delta != 0.0 else
                "seed" if delta != 0.0 and last_status in ("win", "loss") else
                "balance" if delta != 0.0 else
                "none"
            )
            # Track per-asset execution health and log metrics
            try:
                h = S12_ASSET_HEALTH.get(asset, {"trades": 0, "fails": 0, "timeouts": 0, "lat": []})
                h["trades"] += 1
                if last_status in ("fail_exec", "fail_track"):
                    h["fails"] += 1
                new_timeouts = int(globals().get('S12_TIMEOUT_FALLBACKS', 0)) - prev_timeouts
                if new_timeouts > 0:
                    h["timeouts"] += 1
                try:
                    h["lat"].append(float(latency))
                    if len(h["lat"]) > 50:
                        del h["lat"][:-50]
                except Exception:
                    pass
                # Compute health score
                fr = h["fails"] / max(1, h["trades"])
                tr = h["timeouts"] / max(1, h["trades"])
                p95 = sorted(h["lat"])[:max(1, len(h["lat"]))][-1] if h["lat"] else latency
                lat_score = max(0.0, min(1.0, (p95 - 45.0) / 45.0))
                health = max(0.0, min(1.0, 1.0 - (0.5*fr + 0.3*tr + 0.2*lat_score)))
                S12_ASSET_HEALTH[asset] = h
                # Stash metrics to log after result classification
                globals()['S12_LAST_METRICS'] = {
                    "asset": asset,
                    "dir": direction,
                    "payout": payout,
                    "score": score01,
                    "min_conf": min_conf,
                    "passed": True,
                    "atr_pctile": details.get("atr_pctile"),
                    "body_ratio": details.get("body_ratio"),
                    "ema60_align": details.get("ema60_align"),
                    "ema30_align": details.get("ema30_align"),
                    "ema15_slope": details.get("ema15_slope"),
                    "microtrend_ok": details.get("microtrend_ok"),
                    "health_score": health,
                }
            except Exception:
                pass
            # Use granular failure classes from place_and_wait
            fstatus = last_status
            if fstatus in ("equal", "tie", "draw"):
                result = "DRAW"
            elif fstatus in ("fail_exec", "fail_track"):
                result = "FAIL_EXEC" if fstatus == "fail_exec" else "FAIL_TRACK"
            elif last_failed or (delta == 0.0 and not won):
                result = "FAIL_TRACK" if bool(getattr(qx, "last_exec_meta", {}).get("saw_exec")) else "FAIL_EXEC"
            else:
                result = "WIN" if won else "LOSS"

            # Track per-asset execution health and log metrics
            try:
                h = S12_ASSET_HEALTH.get(asset, {"trades": 0, "fails": 0, "timeouts": 0, "lat": []})
                h["trades"] += 1
                if result in ("FAIL_EXEC", "FAIL_TRACK"):
                    h["fails"] += 1
                new_timeouts = int(globals().get('S12_TIMEOUT_FALLBACKS', 0)) - prev_timeouts
                if new_timeouts > 0:
                    h["timeouts"] += 1
                try:
                    h["lat"].append(float(latency))
                    if len(h["lat"]) > 50:
                        del h["lat"][:-50]
                except Exception:
                    pass
                fr = h["fails"] / max(1, h["trades"])
                tr = h["timeouts"] / max(1, h["trades"])
                p95 = sorted(h["lat"])[:max(1, len(h["lat"]))][-1] if h["lat"] else float(latency)
                lat_score = max(0.0, min(1.0, (p95 - 45.0) / 45.0))
                health = max(0.0, min(1.0, 1.0 - (0.5*fr + 0.3*tr + 0.2*lat_score)))
                S12_ASSET_HEALTH[asset] = h
                # Combine with pretrade metrics if available
                try:
                    m = dict(globals().get('S12_LAST_METRICS', {}))
                except Exception:
                    m = {}
                if not m:
                    m = {
                        "asset": asset,
                        "dir": direction,
                        "payout": payout,
                        "score": score01,
                        "min_conf": min_conf,
                        "passed": pre_ok,
                        "atr_pctile": details.get("atr_pctile"),
                        "body_ratio": details.get("body_ratio"),
                        "ema60_align": details.get("ema60_align"),
                        "ema30_align": details.get("ema30_align"),
                        "ema15_slope": details.get("ema15_slope"),
                        "microtrend_ok": details.get("microtrend_ok"),
                        "health_score": health,
                    }
                m.update({
                    "result": result,
                    "pnl": float(delta),
                    "resolve_source": delta_source,
                })
                _s12_log_metrics_row(m)
            except Exception:
                pass

            opid_dbg = None
            try:
                opid_dbg = getattr(qx.api, 'buy_id', None)
            except Exception:
                opid_dbg = None
            print(
                f"[S12][DEBUG] op_id={opid_dbg} status={last_status} result={result} "
                f"delta={delta} source={delta_source} tradable_before={tradable_snapshot}"
            )

            # Consecutive loss pause guard
            try:
                if result == "LOSS":
                    s12_loss_streak += 1
                else:
                    s12_loss_streak = 0
                if s12_loss_streak >= 3:
                    print("[S12] Pause: 3 consecutive losses. Cooling for 60s...")
                    await asyncio.sleep(60.0)
                    s12_loss_streak = 0
            except Exception:
                pass


            # Track failures and blacklist on repeat + short cooldown
            if result in ("FAIL_EXEC", "FAIL_TRACK"):
                try:
                    exec_fail_counts
                except NameError:
                    exec_fail_counts = {}
                cnt = exec_fail_counts.get(asset, 0) + 1
                exec_fail_counts[asset] = cnt
                # First failure: mark cooldown 2.5s to avoid immediate reselection
                try:
                    from time import time as _now
                    if cnt == 1:
                        if 'exec_cooldowns' not in globals():
                            globals()['exec_cooldowns'] = {}
                        globals()['exec_cooldowns'][asset] = _now() + 2.5
                except Exception:
                    pass
                if cnt >= 2 and asset not in exec_blacklist:
                    exec_blacklist.add(asset)
                    print(f"[S12] Blacklisting asset due to repeated execution failures: {asset}")
                    # Extend blacklist duration to 12 hours for problematic assets
                    try:
                        if 'exec_cooldowns' not in globals():
                            globals()['exec_cooldowns'] = {}
                        from time import time as _now
                        globals()['exec_cooldowns'][asset] = _now() + (12 * 3600)
                    except Exception:
                        pass
            else:
                # Reset failure count on non-fail
                try:
                    exec_fail_counts[asset] = 0
                except Exception:
                    pass
            trade12 += 1
            # Inter-trade throttle (rate-limit guard)
            try:
                await asyncio.sleep(0.9)
            except Exception:
                pass

            rows12.append([trade12, asset, direction, amount, result, delta])
            print(tabulate(rows12[-10:], headers=headers12, tablefmt="github"))

            # Circuit breaker: if last two are FAIL_EXEC, pause and reconnect
            try:
                if len(rows12) >= 2 and rows12[-1][4].startswith("FAIL_") and rows12[-2][4].startswith("FAIL_"):
                    print("[S12] Circuit breaker: consecutive failures detected. Pausing and reconnecting...")
                    try:
                        await asyncio.sleep(6.0)
                        await _hard_recover_broker(qx)
                        await asyncio.sleep(1.0)
                    except Exception:
                        pass
            except Exception:
                pass

            if trade12 >= target_trades:
                wins = sum(1 for r in rows12 if r[4] == "WIN")
                # Treat FAIL and DRAW as non-wins, exclude FAIL from loss-rate calc if you prefer
                acc = wins / max(1, len(rows12))
                print(f"[S12] Paper validation completed: trades={len(rows12)} wins={wins} acc={acc:.3f}")
                await qx.close()
                return

    headers = ["#", "Currency", "Direction", "Amount", "Result", "P/L"]
    rows: List[List] = []
    trade_no = 0

    print("\nBot start......")

    idx_comp = 0
    loss_streak = 0
    pnl_total = 0.0

    # Infinite-loop safeguards
    idle_passes = 0
    max_idle_passes = 300  # ~10 minutes at 2s/pass

    while True:
        # Stop checks based on cumulative realized P/L
        if pnl_total >= profit_target:
            print(Fore.GREEN + f"Target reached. Profit: {pnl_total}" + Style.RESET_ALL)
            await qx.close()
            return
        if -pnl_total >= loss_target:
            print(Fore.RED + f"Loss limit reached. P/L: {pnl_total}" + Style.RESET_ALL)
            await qx.close()
            return

        # Strategy 14: AI analyzer path – use before generic find_first_signal
        if strategy == 14:
            try:
                from strategy14_ai.deploy import find_first_signal_ai
            except Exception:
                try:
                    from .strategy14_ai.deploy import find_first_signal_ai
                except Exception:
                    find_first_signal_ai = None
            if find_first_signal_ai is None:
                print(Fore.RED + "S14 not available (import error)." + Style.RESET_ALL)
                await qx.close()
                return
            # Allowed IST hours from S10 filter by default
            allowed_hours = set(S10_ALLOWED_HOURS) if 'S10_ALLOWED_HOURS' in globals() else None
            asset, direction = await find_first_signal_ai(
                qx,
                min_confidence=0.90,
                min_payout=float(max(90.0, min_payout)),
                quality_cfg={
                    "mtf_confluence": True,
                    "liq_min": 0.5,
                    "atr_pctile_min": 0.2,
                    "atr_pctile_max": 0.9,
                },
                allowed_hours=allowed_hours,
                debug=True,
                live_hour_override=True,
            )
        elif strategy == 18:
            # Strategy 18: Strict 4-way confluence on 60s with 60s expiry
            if find_first_signal_s18 is None or S18Config is None:
                print(Fore.RED + "S18 not available (import error)." + Style.RESET_ALL)
                await qx.close()
                return
            # Force 1-minute expiry per spec
            if expiry_min != 1:
                print(Fore.YELLOW + "Strategy 18 uses 1-minute expiry. Overriding your input to 1 minute." + Style.RESET_ALL)
                expiry_min = 1
            # Strategy 18: ignore IST hour filter (execute-anytime per user request)
            s18_cfg = S18Config(
                allowed_hours_ist=None,
                timeframe_s=60,
                expiry_s=60,
            )
            asset, direction = await find_first_signal_s18(
                qx,
                cfg=s18_cfg,
                min_payout=float(min_payout),
                debug=True,
            )

        elif strategy == 15:
            # Strategy 15: Hybrid scheduled follower + live technical analyzer
            if find_first_signal_s15 is None or S15Config is None:
                print(Fore.RED + "S15 not available (import error)." + Style.RESET_ALL)
                await qx.close()
                return
            # Build default S15 config; allow user-provided trades file if exists
            trades_file = os.path.join(os.path.dirname(__file__), "..", "trades.txt")
            if not os.path.exists(trades_file):
                trades_file = None
            # Allowed IST hours inherit from S10 default if present
            allowed_hours = set(S10_ALLOWED_HOURS) if 'S10_ALLOWED_HOURS' in globals() else None
            s15_cfg = S15Config(
                mode="hybrid",
                trades_file=trades_file,
                schedule_tz="IST",
                match_window_sec=25,
                allowed_hours_ist=allowed_hours,
            )
            asset, direction = await find_first_signal_s15(
                qx,
                cfg=s15_cfg,
                min_payout=float(min_payout),
                expiry_min=int(expiry_min),
                debug=True,
            )
        else:
            # Strategy 16: ultra-selective scanner path
            if strategy == 16:
                if find_first_signal_s16 is None or S16Config is None:
                    print(Fore.RED + "S16 not available (import error)." + Style.RESET_ALL)
                    await qx.close()
                    return
                allowed_hours = set(S10_ALLOWED_HOURS) if 'S10_ALLOWED_HOURS' in globals() else None
                s16_cfg = S16Config(
                    timeframes_s=[15, 30, 60],
                    min_payout=float(max(92.0, min_payout)),
                    allowed_hours_ist=allowed_hours,
                    expiry_s=60,
                    confluence_mode="3of3",
                    min_confidence=0.98,
                )
                asset, direction = await find_first_signal_s16(
                    qx,
                    cfg=s16_cfg,
                    min_payout=float(max(92.0, min_payout)),
                    debug=True,
                )
            else:
                # Scan assets in background and pick the first with a valid signal and payout >= threshold
                scan_min_payout = 80 if strategy == 11 else min_payout
                asset, direction = await find_first_signal(qx, strategy, scan_min_payout, expiry_min, debug=True)

        if not asset:
            idle_passes += 1
            if idle_passes >= max_idle_passes:
                print(Fore.YELLOW + "No signals for an extended period. Pausing for 60s to avoid looping..." + Style.RESET_ALL)
                await asyncio.sleep(60)
                idle_passes = 0
            else:
                print("No signals this pass. Waiting...\n")
                await asyncio.sleep(2)
            continue
        else:
            idle_passes = 0

        # Before placing next trade, consult session profiler (day-of-week/hour bucket)
        # Strategy 18: execute immediately on signal without historical WR guard
        if strategy in (18,):
            ok_to_trade, reason = True, "s18-immediate"
        elif strategy == 11 and not S11_WR_GUARD_ENABLED:
            ok_to_trade, reason = True, "s11-bypass"
        elif strategy == 15:
            ok_to_trade, reason = True, "s15-bypass"
        else:
            ok_to_trade, reason = should_trade_now(strategy, min_trades=5, min_winrate=0.55, include_otc=True)
        if not ok_to_trade:
            print(Fore.YELLOW + f"Skipping this minute due to low historical winrate: {reason}" + Style.RESET_ALL)
            await asyncio.sleep(30)
            continue

        # Money management
        if strategy == 11:
            # Strategy 11: 2% base, 1.5x on first loss, recover after 2 losses and switch asset
            bal = await qx.get_balance()
            base_pct = max(0.02 * bal, 1.0)
            ls = S11_LOSS_STREAK.get(asset, 0)
            if S11_RECOVERY_PENDING and S11_RECOVERY_PENDING > 0:
                amount = round(S11_RECOVERY_PENDING + base_pct, 2)
                S11_RECOVERY_PENDING = 0.0  # use once
            elif ls == 0:
                amount = base_pct
            elif ls == 1:
                amount = round(1.5 * base_pct, 2)
            else:
                # Fallback: if somehow still on same asset with 2+ losses, use recovery
                last_losses = S11_LAST_LOSSES.get(asset, [])
                rec = sum(last_losses[-2:]) if last_losses else 0.0
                amount = round(rec + base_pct, 2)
        else:
            if t == "1":
                amount = comp_amounts[max(0, idx_comp % len(comp_amounts))]
            else:
                amount = base_amount * (mult ** min(loss_streak, steps - 1))

        # Debug visibility for Strategy 15 execution stage
        if strategy == 15:
            # Optional timing alignment using learned preferred second
            try:
                pref_sec = get_preferred_second(asset) if get_preferred_second else None
            except Exception:
                pref_sec = None
            if isinstance(pref_sec, int):
                try:
                    now = time.time()
                    cur_sec = int(now % 60)
                    wait_s = 0.0
                    if cur_sec <= pref_sec:
                        wait_s = float(pref_sec - cur_sec)
                    else:
                        wait_s = float(60 - (cur_sec - pref_sec))
                    # Safety margin: avoid waiting if less than 0.8s or too close to expiry framing
                    if 0.8 <= wait_s <= 15.0:
                        print(f"[S15] Aligning entry to sec={pref_sec} (learned), waiting {wait_s:.1f}s")
                        await asyncio.sleep(wait_s)
                except Exception:
                    pass
            try:
                print(f"[S15] Placing trade: {asset} {direction} amount={amount}")
            except Exception:
                pass
            # Execute trade for Strategy 15 immediately
            start_ts = time.time()
            won, delta = await place_and_wait(qx, float(amount), asset, direction, int(expiry_min) * 60)
            sec_in_min = int(start_ts % 60)
            # Update session P/L only (compounding index will be adjusted after result classification)
            try:
                pnl_total += float(delta)
            except Exception:
                pass
            # Outcome print and quick metrics
            try:
                # Classify result with granular exec checks (align with S12)
                last_status = str(getattr(qx, "last_exec_status", "")).lower()
                last_failed = bool(getattr(qx, "last_exec_failed", False))
                if last_status in ("equal", "tie", "draw"):
                    result = "DRAW"
                elif last_status in ("fail_exec", "fail_track"):
                    result = "FAIL_EXEC" if last_status == "fail_exec" else "FAIL_TRACK"
                elif last_failed or (float(delta) == 0.0 and not bool(won)):
                    saw_exec = bool(getattr(qx, "last_exec_meta", {}).get("saw_exec"))
                    result = "FAIL_TRACK" if saw_exec else "FAIL_EXEC"
                else:
                    result = "WIN" if bool(won) else "LOSS"

                # Update streaks only on true LOSS
                try:
                    if result == "LOSS":
                        loss_streak = loss_streak + 1
                    elif result in ("WIN", "DRAW"):
                        loss_streak = 0
                except Exception:
                    pass

                print(f"[S15] Result: {result} PnL={float(delta):+.2f} Total={float(pnl_total):+.2f}")
                # Broker exec status echo for diagnosis
                try:
                    st = str(getattr(qx, "last_exec_status", ""))
                    rsn = str(getattr(qx, "last_exec_error_reason", ""))
                    if st or rsn:
                        print(f"[S15] Exec status: {st} reason={rsn}")
                except Exception:
                    pass
                # Append to live results table
                try:
                    row_no = len(rows) + 1
                    rows.append([row_no, asset, direction, amount, result, delta])
                    print(tabulate(rows[-10:], headers=headers, tablefmt="github"))
                    # Register cooldown on loss for S15
                    if result == "LOSS":
                        try:
                            from strategy15 import s15_register_loss
                            s15_register_loss(asset, when_ts=start_ts)
                        except Exception:
                            pass
                except Exception:
                    pass
                # Session CSV log (optional)
                try:
                    payout = await get_asset_payout(qx, asset, expiry_min)
                    acc_mode = qx.get_account_mode() if hasattr(qx, 'get_account_mode') else "PRACTICE"
                    log_trade_row(
                        asset,
                        15,
                        direction,
                        float(amount),
                        result,
                        float(delta),
                        float(payout),
                        int(expiry_min),
                        acc_mode,
                    )
                except Exception:
                    pass
                # Strategy 15 timing memory + execution log + quick post-eval
                try:
                    from strategy15 import (
                        log_s15_execution,
                        update_timing_memory,
                        post_trade_quick_eval,
                        S15_LAST_TF,
                        S15_LAST_MODE,
                    )
                except Exception:
                    log_s15_execution = None
                    update_timing_memory = None
                    post_trade_quick_eval = None
                    S15_LAST_TF = None
                    S15_LAST_MODE = None
                try:
                    if log_s15_execution:
                        log_s15_execution(
                            entry_ts=start_ts,
                            asset=asset,
                            direction=direction,
                            amount=float(amount),
                            result=result,
                            delta=float(delta),
                            expiry_s=int(expiry_min) * 60,
                            sec_in_minute=int(sec_in_min),
                            tf_hint=int(S15_LAST_TF) if S15_LAST_TF else None,
                            payout=float(payout) if 'payout' in locals() else None,
                            mode=str(S15_LAST_MODE) if S15_LAST_MODE else None,
                        )
                    if update_timing_memory:
                        update_timing_memory(asset, sec_in_min, result)
                    if post_trade_quick_eval and result in ("WIN", "LOSS", "DRAW"):
                        _ = await post_trade_quick_eval(
                            qx,
                            asset,
                            start_ts,
                            direction,
                            eval_tf_s=15,
                            bars=2,
                        )
                except Exception:
                    pass
                # S15 compounding ladder update (WIN->advance, LOSS->reset, DRAW/FAIL->hold)
                try:
                    if t == "1" and 'result' in locals():
                        if result == "WIN":
                            idx_comp = (idx_comp + 1) % (len(comp_amounts) if comp_amounts else 1)
                        elif result == "LOSS":
                            idx_comp = 0
                        # DRAW/FAIL_EXEC/FAIL_TRACK -> hold (no change)
                except Exception:
                    pass
            except Exception:
                pass

        else:
            # General execution for strategies other than 15 (e.g., S18)
            try:
                print(f"[S{strategy}] Placing trade: {asset} {direction} amount={amount}")
            except Exception:
                pass
            import time as _t
            start_ts = _t.time()
            won, delta = await place_and_wait(qx, float(amount), asset, direction, int(expiry_min) * 60)
            # Update PnL
            try:
                pnl_total += float(delta)
            except Exception:
                pass
            # Classify result with execution failure detection (align with S15)
            try:
                last_status = str(getattr(qx, "last_exec_status", "")).lower()
                last_failed = bool(getattr(qx, "last_exec_failed", False))
                if last_status in ("equal", "tie", "draw"):
                    result = "DRAW"
                elif last_status in ("fail_exec", "fail_track"):
                    result = "FAIL_EXEC" if last_status == "fail_exec" else "FAIL_TRACK"
                else:
                    result = "WIN" if bool(won) else "LOSS"
                # Optional hint for execution failures
                if last_failed and result.startswith("FAIL"):
                    try:
                        reason = str(getattr(qx, "last_exec_error_reason", "")).lower()
                        print(f"[WARN] Execution failed for {asset}: {result} ({reason})")
                    except Exception:
                        pass
                print(f"[S{strategy}] Result: {result} PnL={float(delta):+.2f} Total={float(pnl_total):+.2f}")
                # Append to live results table (generic strategies)
                try:
                    row_no = len(rows) + 1
                    rows.append([row_no, asset, direction, amount, result, delta])
                    print(tabulate(rows[-10:], headers=headers, tablefmt="github"))
                except Exception:
                    pass
                # Simple compounding index update if enabled
                if t == "1":
                    try:
                        if result == "WIN":
                            idx_comp = (idx_comp + 1) % (len(comp_amounts) if comp_amounts else 1)
                        elif result == "LOSS":
                            idx_comp = 0
                    except Exception:
                        pass
            except Exception:
                pass

        trade_no += 1
        # Attach captured features to this trade number (Strategy 11)
        if strategy == 11 and S11_ADAPTIVE_ENABLE:
            try:
                feat = globals().get("S11_LAST_FEATURES")
                if feat:
                    feat["asset"] = asset
                    S11_OPEN_TRADES[trade_no] = feat
                    globals()["S11_LAST_FEATURES"] = None
            except Exception as e:
                _dbg(f"{asset} s11 adaptive attach error: {e}")

    # Strategy 12: Phase 1 paper validation branch (run standalone and return)
    if strategy == 12:
        if s12_run_pipeline is None or S12Config is None or RuleVariant is None:
            print(Fore.RED + "S12 not available (import error)." + Style.RESET_ALL)
            await qx.close()
            return
        try:
            qx.set_account_mode("PRACTICE")
        except Exception:
            pass
        allowed_hours = set(S10_ALLOWED_HOURS) if 'S10_ALLOWED_HOURS' in globals() else None
        cfg = S12Config(
            timeframes_s=[15, 30, 60],
            min_payout=float(min_payout),
            target_accuracy=0.95,
            min_trades=50,
            ist_hours=allowed_hours,
            bars_per_tf=5400,
        )
        from strategy12.cli import interactive_select_variant as s12_select
        chosen = await s12_select(qx, cfg)
        if not chosen:
            await qx.close()
            return
        from strategy12.deploy import find_first_signal_s12
        print("\n[S12] PRACTICE validation running...")
        idle = 0
        rows = []
        trade_no = 0
        while True:
            asset, direction = await find_first_signal_s12(
                qx,
                rule=RuleVariant(name=chosen['variant'], params=chosen['params']),
                min_payout=float(min_payout),
                timeframes_s=[15, 30, 60],
                allowed_hours=allowed_hours,
                debug=True,
            )
            if not asset:
                idle += 1
                if idle % 30 == 0:
                    print("[S12] No signals yet...")
                await asyncio.sleep(2)
                continue
            idle = 0
            if t == "1":
                amount = comp_amounts[max(0, idx_comp % len(comp_amounts))] if comp_amounts else 20.0
                idx_comp += 1
            else:
                amount = base_amount * (mult ** min(loss_streak, steps - 1))
            won, delta = await place_and_wait(qx, amount, asset, direction, expiry_min * 60)
            result = "WIN" if won else "LOSS"
            trade_no += 1
            rows.append([trade_no, asset, direction, amount, result, delta])
            if trade_no >= 50:
                wins = sum(1 for r in rows if r[4] == "WIN")
                acc = wins / max(1, len(rows))
                print(f"[S12] Paper validation: trades={len(rows)} wins={wins} acc={acc:.3f}")
                if acc >= 0.95:
                    print(Fore.GREEN + "[S12] Target met. Eligible for live promotion." + Style.RESET_ALL)
                    await qx.close()
                    return
                else:
                    print(Fore.YELLOW + "[S12] Below target; continuing..." + Style.RESET_ALL)



if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
