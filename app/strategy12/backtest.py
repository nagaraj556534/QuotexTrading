from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, List, Tuple, Optional, Set
from datetime import datetime
from zoneinfo import ZoneInfo
from .confluence import two_of_three_confluence, three_of_three_confluence

@dataclass
class BTResult:
    variant: str
    timeframe_s: int
    asset: str
    total_trades: int
    wins: int
    accuracy: float
    ist_hour_stats: Dict[int, Tuple[int, int]]  # hour -> (wins, total)
    # Paper trading stats (optional)
    paper_enabled: bool = False
    paper_profit_target: float = 0.0
    paper_loss_limit: float = 0.0
    paper_pt_hits: int = 0
    paper_sl_hits: int = 0
    paper_avg_time_to_target_s: float = 0.0
    paper_avg_time_to_stop_s: float = 0.0


def _ist_hour(ts: float) -> int:
    try:
        return int(datetime.fromtimestamp(ts, ZoneInfo("Asia/Kolkata")).hour)
    except Exception:
        return int(datetime.utcfromtimestamp(ts).hour)


def _candle_time(c):
    for k in ("from", "time", "timestamp"):
        if k in c:
            return float(c[k])
    return None


def backtest_variant(
    candles: List[Dict[str, Any]],
    rule_eval,
    timeframe_s: int,
    expiry_steps: int = 1,
    allowed_hours: Optional[Set[int]] = None,
    oos_frac: float = 0.3,
    min_trades_goal: Optional[int] = None,
) -> BTResult:
    """Backtest a single rule variant on given candles (out-of-sample by default).
    rule_eval: callable(candles[:i+1]) -> (has_signal, direction)
    expiry_steps: steps ahead to evaluate outcome; 1 means next bar close.
    oos_frac: fraction of series (tail) used for evaluation to approximate OOS.
    min_trades_goal: if set, try to expand evaluation window earlier until this
        minimum trade count is reached (best-effort, still OOS by using prefix).
    """
    # Adjust minimum bars and evaluation window for small 30s histories
    min_bars = 20 if timeframe_s == 30 else 60
    if not candles or len(candles) < min_bars:
        return BTResult("unknown", timeframe_s, "unknown", 0, 0, 0.0, {})

    wins = 0
    total = 0
    ist_hour_stats: Dict[int, Tuple[int, int]] = {}

    n = len(candles)
    base_idx = int(n * (1.0 - max(0.0, min(0.9, oos_frac))))
    # For 30s diagnostics, start as early as possible to create opportunities
    start_idx = max(1 if timeframe_s == 30 else min_bars, base_idx)

    # If min_trades_goal set, we will evaluate, and if too few trades found, we expand window earlier
    def _run_eval(si: int) -> Tuple[int, int, Dict[int, Tuple[int, int]]]:
        _wins, _total = 0, 0
        _hours: Dict[int, Tuple[int, int]] = {}
        for i in range(si, n - expiry_steps):
            prefix = candles[: i + 1]
            has_signal, direction = rule_eval(prefix)
            if not has_signal:
                continue
            if allowed_hours:
                ts = _candle_time(candles[i])
                hr = _ist_hour(ts) if ts else None
                if hr is not None and hr not in allowed_hours:
                    continue
            c_entry = float(candles[i]["close"])  # signal on bar i close
            c_exit = float(candles[i + expiry_steps]["close"])  # expiry close
            won = (c_exit > c_entry) if direction == "call" else (c_exit < c_entry)
            _total += 1
            _wins += 1 if won else 0
            ts = _candle_time(candles[i])
            hr = _ist_hour(ts) if ts else None
            if hr is not None:
                w, t = _hours.get(hr, (0, 0))
                _hours[hr] = (w + (1 if won else 0), t + 1)
        return _wins, _total, _hours

    wins, total, ist_hour_stats = _run_eval(start_idx)
    if min_trades_goal and total < min_trades_goal:
        # Expand earlier in steps until reaching min_trades_goal or hitting min_bars
        backoff = start_idx
        floor = 1 if timeframe_s == 30 else min_bars
        while backoff > floor and total < min_trades_goal:
            backoff = max(floor, backoff - int(0.2 * n))  # expand by 20% of series length
            wins, total, ist_hour_stats = _run_eval(backoff)
            if total >= min_trades_goal:
                break

    acc = (wins / total) if total > 0 else 0.0
    return BTResult("unknown", timeframe_s, "unknown", total, wins, acc, ist_hour_stats)


def backtest_variant_confluence(
    candles_15: List[Dict[str, Any]] | None,
    candles_30: List[Dict[str, Any]] | None,
    candles_60: List[Dict[str, Any]] | None,
    rule_eval,
    expiry_steps: int = 1,
    allowed_hours: Optional[Set[int]] = None,
    oos_frac: float = 0.3,
    min_trades_goal: Optional[int] = None,
) -> BTResult:
    """Backtest with 2-of-3 TF confluence using 60s as base for expiry.
    Evaluates signals on available TFs (15/30/60), requires at least 2 TFs to agree.
    """
    c15 = candles_15 or []
    c30 = candles_30 or []
    c60 = candles_60 or []
    if not c60 or len(c60) < 60:
        return BTResult("unknown", 60, "unknown", 0, 0, 0.0, {})

    n60 = len(c60)
    start_idx = max(60, int(n60 * (1.0 - max(0.0, min(0.9, oos_frac)))))

    # Pointers for 15s and 30s alignment
    j30 = 0
    k15 = 0

    def eval_tf(prefix):
        has_signal, direction = rule_eval(prefix)
        return has_signal, direction

    def run_from(si60: int):
        wins = 0
        total = 0
        hours: Dict[int, Tuple[int, int]] = {}
        nonlocal j30, k15
        j30 = min(j30, max(0, len(c30) - 1))
        k15 = min(k15, max(0, len(c15) - 1))
        for i60 in range(si60, n60 - expiry_steps):
            ts60 = _candle_time(c60[i60])
            # advance 30s pointer to ts <= ts60
            while j30 + 1 < len(c30) and (_candle_time(c30[j30 + 1]) or 0) <= (ts60 or 0):
                j30 += 1
            while k15 + 1 < len(c15) and (_candle_time(c15[k15 + 1]) or 0) <= (ts60 or 0):
                k15 += 1
            sigs: Dict[int, Tuple[bool, str]] = {}
            # 60s
            sigs[60] = eval_tf(c60[: i60 + 1])
            # 30s if available
            if c30:
                sigs[30] = eval_tf(c30[: j30 + 1])
            # 15s if available
            if c15:
                sigs[15] = eval_tf(c15[: k15 + 1])
            has, direction = two_of_three_confluence(sigs)
            if not has:
                continue
            if allowed_hours:
                hr = _ist_hour(ts60) if ts60 else None
                if hr is not None and hr not in allowed_hours:
                    continue
            c_entry = float(c60[i60]["close"])  # 60s entry
            c_exit = float(c60[i60 + expiry_steps]["close"])  # 60s expiry
            won = (c_exit > c_entry) if direction == "call" else (c_exit < c_entry)
            total += 1
            wins += 1 if won else 0
            hr = _ist_hour(ts60) if ts60 else None
            if hr is not None:
                w, t = hours.get(hr, (0, 0))
                hours[hr] = (w + (1 if won else 0), t + 1)
        return wins, total, hours

    wins, total, ist_hour_stats = run_from(start_idx)
    if min_trades_goal and total < min_trades_goal:
        backoff = start_idx
        while backoff > 60 and total < min_trades_goal:
            backoff = max(60, backoff - int(0.1 * n60))
            wins, total, ist_hour_stats = run_from(backoff)
            if total >= min_trades_goal:
                break

    acc = (wins / total) if total > 0 else 0.0
    return BTResult("unknown", 60, "unknown", total, wins, acc, ist_hour_stats)



from .features import ema as _ema_f, atr as _atr_f


def backtest_variant_confluence_gated(
    candles_15: List[Dict[str, Any]] | None,
    candles_30: List[Dict[str, Any]] | None,
    candles_60: List[Dict[str, Any]] | None,
    rule_eval,
    min_conf: float = 0.60,
    expiry_steps: int = 1,
    allowed_hours: Optional[Set[int]] = None,
    oos_frac: float = 0.3,
    min_trades_goal: Optional[int] = None,
    strict_micro: bool = False,
    gate_body_min: float = 0.33,
    gate_atr_lo: float = 35.0,
    gate_atr_hi: float = 80.0,
    confluence_mode: str = "2of3",  # "2of3", "3of3", "adaptive"
    precomputed: Optional[Dict[str, List[float]]] = None,
    paper: Optional[Dict[str, Any]] = None,
    require_align30: bool = False,
) -> BTResult:
    """Backtest with TF confluence AND pretrade gates, parameterized for strict95.
    Uses 60s as base for timing/expiry; gates computed from indicators across TFs.
    Optionally consumes precomputed indicator arrays to avoid recomputation.
    Supports paper-trading targets/stops when 'paper' dict provided.
    require_align30: when True, 30s trend alignment must be present (if 30s TF available).
    """
    c15 = candles_15 or []
    c30 = candles_30 or []
    c60 = candles_60 or []
    if not c60 or len(c60) < 60:
        return BTResult("unknown", 60, "unknown", 0, 0, 0.0, {})

    # Precompute indicators (gates). Accept optional precomputed arrays to save time
    if precomputed is None:
        closes60 = [float(x["close"]) for x in c60]
        e11_60 = _ema_f(closes60, 11) if closes60 else []
        e55_60 = _ema_f(closes60, 55) if closes60 else []
        e7_60 = _ema_f(closes60, 7) if closes60 else []
        highs60 = [float(x.get("high", 0)) for x in c60]
        lows60 = [float(x.get("low", 0)) for x in c60]
        atr60 = _atr_f(highs60, lows60, closes60, 14) if closes60 else []

        closes30 = [float(x["close"]) for x in c30] if c30 else []
        e11_30 = _ema_f(closes30, 11) if closes30 else []
        e55_30 = _ema_f(closes30, 55) if closes30 else []

        closes15 = [float(x["close"]) for x in c15] if c15 else []
        e11_15 = _ema_f(closes15, 11) if closes15 else []
    else:
        closes60 = precomputed.get("closes60", [])
        e11_60 = precomputed.get("e11_60", [])
        e55_60 = precomputed.get("e55_60", [])
        e7_60 = precomputed.get("e7_60", [])
        highs60 = precomputed.get("highs60", [])
        lows60 = precomputed.get("lows60", [])
        atr60 = precomputed.get("atr60", [])
        closes30 = precomputed.get("closes30", [])
        e11_30 = precomputed.get("e11_30", [])
        e55_30 = precomputed.get("e55_30", [])
        closes15 = precomputed.get("closes15", [])
        e11_15 = precomputed.get("e11_15", [])

    # Trend/TA precompute (60s core, 30/15 optional)
    from .features import trend_direction_strength as _tds, adx_di as _adx, supertrend as _st, bb_bandwidth as _bbw, keltner_channels as _kc, donchian as _don, heikin_ashi as _ha
    adx60, pdi60, ndi60 = _adx(highs60, lows60, closes60, period=14)
    st_dir60 = _st(highs60, lows60, closes60, atr_period=int(rule_eval.params.get("st_atr_period", 10)) if hasattr(rule_eval, 'params') else 10, multiplier=float(rule_eval.params.get("st_mult", 3.0)) if hasattr(rule_eval, 'params') else 3.0)
    bbw60 = _bbw(closes60, period=20, stdev=2.0)
    kc_mid60, kc_up60, kc_lo60 = _kc(highs60, lows60, closes60, ema_period=20, atr_period=10, mult=float(rule_eval.params.get("kc_mult", 1.5)) if hasattr(rule_eval, 'params') else 1.5)
    don_up60, don_lo60, don_mid60 = _don(highs60, lows60, period=20)
    trend60_dir, trend60_str = _tds(closes60, highs60, lows60, atr60, idx=len(closes60)-1, lookback=int(rule_eval.params.get("trend_lookback", 30)) if hasattr(rule_eval, 'params') else 30)
    ha_o60, ha_c60 = _ha([float(x.get("open", x.get("close"))) for x in c60], highs60, lows60, closes60)

    adx30 = pdi30 = ndi30 = st_dir30 = bbw30 = kc_mid30 = kc_up30 = kc_lo30 = don_mid30 = None
    if closes30:
        highs30 = [float(x.get("high", 0)) for x in c30]
        lows30  = [float(x.get("low", 0)) for x in c30]
        atr30   = _atr_f(highs30, lows30, closes30, 14)
        adx30, pdi30, ndi30 = _adx(highs30, lows30, closes30, period=14)
        st_dir30 = _st(highs30, lows30, closes30, atr_period=int(rule_eval.params.get("st_atr_period", 10)) if hasattr(rule_eval, 'params') else 10, multiplier=float(rule_eval.params.get("st_mult", 3.0)) if hasattr(rule_eval, 'params') else 3.0)
        bbw30 = _bbw(closes30, period=20, stdev=2.0)
        kc_mid30, kc_up30, kc_lo30 = _kc(highs30, lows30, closes30, ema_period=20, atr_period=10, mult=float(rule_eval.params.get("kc_mult", 1.5)) if hasattr(rule_eval, 'params') else 1.5)
        don_up30, don_lo30, don_mid30 = _don(highs30, lows30, period=20)
        trend30_dir, trend30_str = _tds(closes30, highs30, lows30, atr30, idx=len(closes30)-1, lookback=int(rule_eval.params.get("trend_lookback", 30)) if hasattr(rule_eval, 'params') else 30)
    else:
        trend30_dir, trend30_str = (None, 0.0)

    if closes15:
        highs15 = [float(x.get("high", 0)) for x in c15]
        lows15  = [float(x.get("low", 0)) for x in c15]
        atr15   = _atr_f(highs15, lows15, closes15, 14)
        trend15_dir, trend15_str = _tds(closes15, highs15, lows15, atr15, idx=len(closes15)-1, lookback=int(rule_eval.params.get("trend_lookback", 30)) if hasattr(rule_eval, 'params') else 30)
    else:
        trend15_dir, trend15_str = (None, 0.0)

    def _ct(c):
        for k in ("from", "time", "timestamp"):
            if k in c:
                return float(c[k])
        return None

    def _body_ratio(c: Dict[str, Any]) -> float:
        try:
            body = abs(float(c["close"]) - float(c["open"]))
            rng = max(1e-9, float(c["high"]) - float(c["low"]))
            return (body / rng) if rng else 0.0
        except Exception:
            return 0.0

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

    n60 = len(c60)
    start_idx = max(60, int(n60 * (1.0 - max(0.0, min(0.9, oos_frac)))))
    # Precompute RSI/%R for divergence and fractal swings for BOS
    from .features import rsi as _rsi_f, williams_r as _wr_f, fractal_swings as _fr_f
    rsi60 = _rsi_f(closes60, period=14)
    wr60 = _wr_f(highs60, lows60, closes60, period=14)
    swing_hi60, swing_lo60 = _fr_f(highs60, lows60, window=2)


    j30 = 0
    k15 = 0

    def run_from(si60: int):
        wins = 0
        total = 0
        hours: Dict[int, Tuple[int, int]] = {}
        nonlocal j30, k15
        j30 = min(j30, max(0, len(c30) - 1))
        k15 = min(k15, max(0, len(c15) - 1))
        for i60 in range(si60, n60 - expiry_steps):
            ts60 = _ct(c60[i60])
            # advance pointers
            while j30 + 1 < len(c30) and (_ct(c30[j30 + 1]) or 0) <= (ts60 or 0):
                j30 += 1
            while k15 + 1 < len(c15) and (_ct(c15[k15 + 1]) or 0) <= (ts60 or 0):
                k15 += 1
            # Evaluate rule signals for confluence
            sigs: Dict[int, Tuple[bool, str]] = {60: rule_eval(c60[: i60 + 1])}
            if c30:
                sigs[30] = rule_eval(c30[: j30 + 1])
            if c15:
                sigs[15] = rule_eval(c15[: k15 + 1])
            # Confluence mode selection
            has, direction = False, "call"
            if confluence_mode == "3of3":
                from .confluence import three_of_three_confluence as _cc
                has, direction = _cc(sigs)
            elif confluence_mode == "adaptive":
                # If all three TF present, require 3-of-3; else 2-of-3
                if 60 in sigs and 30 in sigs and 15 in sigs:
                    from .confluence import three_of_three_confluence as _cc
                else:
                    from .confluence import two_of_three_confluence as _cc
                has, direction = _cc(sigs)
            else:
                from .confluence import two_of_three_confluence as _cc
                has, direction = _cc(sigs)
            if not has:
                continue
            # Enforce gate-driven base direction from 60s EMA alignment
            base_dir = None
            if i60 < len(e11_60) and i60 < len(e55_60):
                base_dir = "call" if e11_60[i60] > e55_60[i60] else "put"
            if base_dir is not None and direction != base_dir:
                continue
            # Gates
            align60 = False
            align30 = False
            slope15 = False
            micro_ok = False
            body_ok = False
            atr_ok = False
            atr_pctile = 0.0
            try:
                if i60 < len(e11_60) and i60 < len(e55_60):
                    align60 = (e11_60[i60] > e55_60[i60]) if direction == "call" else (e11_60[i60] < e55_60[i60])
                # micro-trend 60s
                if i60 >= 2:
                    last3 = c60[i60-2:i60+1]
                    bodies = [(float(k["close"]) - float(k["open"])) for k in last3]
                    dir_bools = [(b > 0) if direction == "call" else (b < 0) for b in bodies]
                    cnt = sum(1 for x in dir_bools if x)
                    micro_bodies_ok = (cnt == 3) if strict_micro else (cnt >= 2)
                    if i60 < len(e7_60) and i60 >= 1:
                        c1 = (float(c60[i60-1]["close"]) > float(e7_60[i60-1])) if direction == "call" else (float(c60[i60-1]["close"]) < float(e7_60[i60-1]))
                        c2 = (float(c60[i60]["close"])   > float(e7_60[i60]))   if direction == "call" else (float(c60[i60]["close"])   < float(e7_60[i60]))
                        e7_slope_ok = (float(e7_60[i60]) > float(e7_60[i60-1])) if direction == "call" else (float(e7_60[i60]) < float(e7_60[i60-1]))
                        micro_ok = micro_bodies_ok and e7_slope_ok and (c1 and c2)
                body_ok = _body_ratio(c60[i60]) >= float(gate_body_min)
                if i60 < len(atr60) and i60 >= 1:
                    window = atr60[max(0, i60-119):i60+1]
                    atr_pctile = _pct_rank(window, float(atr60[i60]))
                    atr_ok = float(gate_atr_lo) <= atr_pctile <= float(gate_atr_hi)
                if c30 and j30 < len(e11_30) and j30 < len(e55_30):
                    align30 = (e11_30[j30] > e55_30[j30]) if direction == "call" else (e11_30[j30] < e55_30[j30])
                if c15 and k15 >= 1 and k15 < len(e11_15):
                    slope15 = (e11_15[k15] > e11_15[k15-1]) if direction == "call" else (e11_15[k15] < e11_15[k15-1])
            except Exception:
                pass
            # Volatility regime (ATR percentile on rolling window)
            regime = "mid"
            if atr_pctile < 30.0:
                regime = "low"
            elif atr_pctile > 70.0:
                regime = "high"
            # Dynamic thresholds
            dyn_min_conf = float(min_conf)
            dyn_gate_body_min = float(gate_body_min)
            dyn_gate_atr_lo, dyn_gate_atr_hi = float(gate_atr_lo), float(gate_atr_hi)
            if regime == "low":
                dyn_min_conf += 0.05
                dyn_gate_body_min = max(dyn_gate_body_min, float(gate_body_min) + 0.05)
            elif regime == "high":
                dyn_min_conf += 0.03
                dyn_gate_atr_lo = max(dyn_gate_atr_lo, float(gate_atr_lo) + 5.0)
                dyn_gate_atr_hi = min(dyn_gate_atr_hi, float(gate_atr_hi))

            # 1) New TA gates
            # ADX filter + DI alignment
            adx_min_60 = float(getattr(rule_eval, 'params', {}).get("adx_min_60", 20)) if hasattr(rule_eval, 'params') else 20.0
            adx_ok = (i60 < len(adx60) and adx60[i60] >= adx_min_60)
            di_ok = False
            if i60 < len(pdi60) and i60 < len(ndi60):
                di_ok = (pdi60[i60] > ndi60[i60]) if direction == "call" else (ndi60[i60] > pdi60[i60])
            if not (adx_ok and di_ok):
                continue

            # SuperTrend alignment (60s)
            st_ok = (i60 < len(st_dir60) and ((st_dir60[i60] == "up" and direction == "call") or (st_dir60[i60] == "down" and direction == "put")))
            if not st_ok:
                continue

            # Squeeze detection: BBW < bbw_min and price inside KC → skip
            bbw_min = float(getattr(rule_eval, 'params', {}).get("bbw_min", 0.06)) if hasattr(rule_eval, 'params') else 0.06
            price60 = float(c60[i60]["close"]) if i60 < len(c60) else 0.0
            in_kc = False
            if i60 < len(kc_up60) and i60 < len(kc_lo60):
                in_kc = (price60 <= kc_up60[i60] and price60 >= kc_lo60[i60])
            if i60 < len(bbw60) and (bbw60[i60] < bbw_min) and in_kc:
                continue

            # Pullback validation: distance from EMA55 or Donchian mid in ATRs
            pull_lo = float(getattr(rule_eval, 'params', {}).get("pullback_min_atr", 0.2)) if hasattr(rule_eval, 'params') else 0.2
            pull_hi = float(getattr(rule_eval, 'params', {}).get("pullback_max_atr", 0.8)) if hasattr(rule_eval, 'params') else 0.8
            dist_atr = None
            if i60 < len(e55_60) and i60 < len(atr60) and atr60[i60] > 0:
                base = e55_60[i60]
                if i60 < len(don_mid60) and don_mid60[i60] != 0:
                    base = (base + don_mid60[i60]) / 2.0
                dist_atr = abs(price60 - base) / float(atr60[i60])
            pullback_ok = (dist_atr is not None and pull_lo <= dist_atr <= pull_hi)
            if not pullback_ok:
                continue

            # Heikin-Ashi confirmation: last 2 HA candles consistent
            ha_ok = True
            if bool(getattr(rule_eval, 'params', {}).get("ha_confirm", True)) and i60 >= 1 and i60 < len(ha_o60) and i60 < len(ha_c60):
                def _ha_dir(i):
                    return "up" if float(ha_c60[i]) >= float(ha_o60[i]) else "down"
                d1 = _ha_dir(i60-1)
                d2 = _ha_dir(i60)
                need = "up" if direction == "call" else "down"
                ha_ok = (d1 == need and d2 == need)
            if not ha_ok:
                continue

            # 2) Enhanced scoring (add new weights)
            w = {"align60":0.26,"align30":0.16,"slope15":0.10,"micro":0.16,
                 "body":0.08,"atr":0.04,"trend":0.05,
                 "adx":0.06,"st":0.06,"pull":0.05,"ha":0.03,"sqz":0.05}
            score = 0.0
            if align60: score += w["align60"]
            if align30: score += w["align30"]
            if slope15: score += w["slope15"]
            if micro_ok: score += w["micro"]
            if body_ok: score += w["body"]
            if atr_ok: score += w["atr"]
            score += w["trend"] * max(tstr60, (tstr30 if c30 else 0.0), (tstr15 if c15 else 0.0))
            if adx_ok and di_ok: score += w["adx"]
            if st_ok: score += w["st"]
            if pullback_ok: score += w["pull"]
            # squeeze_off contributes positively
            sqz_off = not (i60 < len(bbw60) and bbw60[i60] < bbw_min and in_kc)
            if sqz_off: score += w["sqz"]
            if ha_ok: score += w["ha"]

            # 3) Volatility regime adaptation: use dyn_min_conf
            if not (align60 and body_ok and atr_ok and (score >= float(dyn_min_conf))):
                continue

            # 4) Structure Break (BOS): confirm recent swing break in direction
            bos_ok = True
            try:
                # Find last swing high/low before i60
                prev_hi = max((idx for idx in range(max(0, i60-10), i60) if swing_hi60[idx]), default=None)
                prev_lo = max((idx for idx in range(max(0, i60-10), i60) if swing_lo60[idx]), default=None)
                if direction == "call" and prev_hi is not None:
                    bos_ok = float(c60[i60]["close"]) > float(highs60[prev_hi])
                if direction == "put" and prev_lo is not None:
                    bos_ok = float(c60[i60]["close"]) < float(lows60[prev_lo])
            except Exception:
                bos_ok = True
            if not bos_ok:
                continue

            # 5) Divergence veto (RSI/Williams %R)
            div_veto = False
            try:
                if i60 >= 2:
                    # Simple divergence: price HH but RSI/WR not HH (for call); price LL but RSI/WR not LL (for put)
                    p2, p1 = float(closes60[i60-2]), float(closes60[i60-1])
                    p0 = float(closes60[i60])
                    r2, r1, r0 = float(rsi60[i60-2]), float(rsi60[i60-1]), float(rsi60[i60])
                    w2, w1, w0 = float(wr60[i60-2]), float(wr60[i60-1]), float(wr60[i60])
                    if direction == "call":
                        if (p0 > p1 > p2) and not (r0 > r1 and w0 > w1):
                            div_veto = True
                    else:
                        if (p0 < p1 < p2) and not (r0 < r1 and w0 < w1):
                            div_veto = True
            except Exception:
                div_veto = False
            if div_veto:
                continue

            body_ok = _body_ratio(c60[i60]) >= float(gate_body_min)
            if i60 < len(atr60) and i60 >= 1:
                window = atr60[max(0, i60-119):i60+1]
                atr_pctile = _pct_rank(window, float(atr60[i60]))
                atr_ok = float(gate_atr_lo) <= atr_pctile <= float(gate_atr_hi)
            if c30 and j30 < len(e11_30) and j30 < len(e55_30):
                align30 = (e11_30[j30] > e55_30[j30]) if direction == "call" else (e11_30[j30] < e55_30[j30])
            if c15 and k15 >= 1 and k15 < len(e11_15):
                slope15 = (e11_15[k15] > e11_15[k15-1]) if direction == "call" else (e11_15[k15] < e11_15[k15-1])

            # Optional enforcement of 30s alignment for higher precision
            if require_align30 and c30 and not align30:
                continue

            # Trend-based gating and weighting
            from .features import trend_direction_strength as _tds
            tdir60, tstr60 = _tds(closes60, highs60, lows60, atr60, idx=i60, lookback=30)
            tdir30, tstr30 = ("sideways", 0.0)
            tdir15, tstr15 = ("sideways", 0.0)
            if c30:
                highs30 = [float(x.get("high", 0)) for x in c30]
                lows30  = [float(x.get("low", 0)) for x in c30]
                atr30   = _atr_f(highs30, lows30, closes30, 14)
                tdir30, tstr30 = _tds(closes30, highs30, lows30, atr30, idx=j30, lookback=30) if j30 < len(closes30) else ("sideways", 0.0)
            if c15:
                highs15 = [float(x.get("high", 0)) for x in c15]
                lows15  = [float(x.get("low", 0)) for x in c15]
                atr15   = _atr_f(highs15, lows15, closes15, 14)
                tdir15, tstr15 = _tds(closes15, highs15, lows15, atr15, idx=k15, lookback=30) if k15 < len(closes15) else ("sideways", 0.0)

            # Allow only trend-consistent directions; reject sideways
            if tdir60 == "sideways":
                continue
            if direction == "call" and tdir60 != "up":
                continue
            if direction == "put" and tdir60 != "down":
                continue

            # Score already computed above (enhanced scoring) into 'score' and dyn_min_conf used.
            # Hours filter follows.
            # Hours filter
            if allowed_hours:
                hr = _ist_hour(ts60) if ts60 else None
                if hr is not None and hr not in allowed_hours:
                    continue
            # Outcome on 60s
            c_entry = float(c60[i60]["close"])
            c_exit = float(c60[i60 + expiry_steps]["close"]) if i60 + expiry_steps < len(c60) else c_entry
            won = (c_exit > c_entry) if direction == "call" else (c_exit < c_entry)
            total += 1
            wins += 1 if won else 0
            hr = _ist_hour(ts60) if ts60 else None
            if hr is not None:
                wv, tv = hours.get(hr, (0, 0))
                hours[hr] = (wv + (1 if won else 0), tv + 1)
        return wins, total, hours

    wins, total, ist_hour_stats = run_from(start_idx)
    if min_trades_goal and total < min_trades_goal:
        backoff = start_idx
        while backoff > 60 and total < min_trades_goal:
            backoff = max(60, backoff - int(0.1 * n60))
            wins, total, ist_hour_stats = run_from(backoff)
            if total >= min_trades_goal:
                break
    acc = (wins / total) if total > 0 else 0.0
    return BTResult("unknown", 60, "unknown", total, wins, acc, ist_hour_stats)
