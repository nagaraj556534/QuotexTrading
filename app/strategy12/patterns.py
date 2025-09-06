from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple, Any, Optional, Set

from .features import ema, williams_r, body_ratio
try:
    from strategy10_confluence import compute_psar  # type: ignore
except Exception:
    try:
        from . import compute_psar  # type: ignore
    except Exception:
        compute_psar = None  # type: ignore


# Utilities

def _ts(c: Dict[str, Any]) -> Optional[float]:
    for k in ("from", "time", "timestamp"):
        if k in c:
            try:
                return float(c[k])
            except Exception:
                return None
    return None


def _ist_hour(ts: Optional[float]) -> Optional[int]:
    if ts is None:
        return None
    try:
        import datetime as _dt
        import zoneinfo as _zi
        dt = _dt.datetime.utcfromtimestamp(ts).replace(tzinfo=_zi.ZoneInfo("UTC")).astimezone(_zi.ZoneInfo("Asia/Kolkata"))
        return int(dt.hour)
    except Exception:
        return None


@dataclass
class Pattern:
    direction: str  # "call" or "put"
    predicates: Tuple[str, ...]
    global_trades: int
    global_wins: int
    win_rate: float
    per_asset: Dict[str, Tuple[int, int]]  # asset -> (wins, trades)


# Feature state extraction for 60s timeframe

def build_feature_states_60s(candles: List[Dict[str, Any]], ema_fast_p: int = 11, ema_slow_p: int = 55,
                             wpr_period: int = 14, psar_step: float = 0.02, psar_max: float = 0.3) -> Dict[str, List[bool]]:
    if not candles or len(candles) < max(60, ema_slow_p + 2):
        return {}
    opens = [float(x["open"]) for x in candles]
    highs = [float(x["high"]) for x in candles]
    lows = [float(x["low"]) for x in candles]
    closes = [float(x["close"]) for x in candles]

    e_fast = ema(closes, ema_fast_p)
    e_slow = ema(closes, ema_slow_p)
    wr = williams_r(highs, lows, closes, period=wpr_period)
    br = body_ratio(opens, closes, highs, lows)
    ps = compute_psar(highs, lows, step=psar_step, max_step=psar_max) if compute_psar else [float('inf')] * len(closes)

# Feature state extraction for 30s timeframe (lower min bars)

def build_feature_states_30s(candles: List[Dict[str, Any]], ema_fast_p: int = 11, ema_slow_p: int = 55,
                             wpr_period: int = 14, psar_step: float = 0.02, psar_max: float = 0.3) -> Dict[str, List[bool]]:
    if not candles or len(candles) < 20:
        return {}
    opens = [float(x["open"]) for x in candles]
    highs = [float(x["high"]) for x in candles]
    lows = [float(x["low"]) for x in candles]
    closes = [float(x["close"]) for x in candles]

    e_fast = ema(closes, ema_fast_p)
    e_slow = ema(closes, ema_slow_p)
    wr = williams_r(highs, lows, closes, period=wpr_period)
    br = body_ratio(opens, closes, highs, lows)
    ps = compute_psar(highs, lows, step=psar_step, max_step=psar_max) if compute_psar else [float('inf')] * len(closes)

    n = len(closes)
    # Initialize all predicates with False
    states: Dict[str, List[bool]] = {}
    def _alloc(name: str):
        if name not in states:
            states[name] = [False] * n

    # Predicate builders
    thresholds_body = [0.20, 0.30, 0.35, 0.40]
    thresholds_ema_dist = [0.00, 0.01, 0.02]

    for i in range(1, n):
        # EMA relations
        ema_above = e_fast[i] > e_slow[i]
        ema_below = e_fast[i] < e_slow[i]
        ema_cross_up = (e_fast[i-1] <= e_slow[i-1]) and ema_above
        ema_cross_dn = (e_fast[i-1] >= e_slow[i-1]) and ema_below
        # PSAR state
        psar_bull = closes[i] > ps[i]
        psar_bear = closes[i] < ps[i]
        # PSAR flip recent (<=3 bars)
        psar_flip = False
        if i >= 2:
            prev_bull = closes[i-1] > ps[i-1]
            if prev_bull != psar_bull:
                psar_flip = True
        # Williams %R crossings
        w_up = False; w_dn = False
        try:
            w_up = (wr[i-1] <= -80.0) and (wr[i] > -80.0)
            w_dn = (wr[i-1] >= -20.0) and (wr[i] < -20.0)
        except Exception:
            pass
        # Body thresholds
        for th in thresholds_body:
            name = f"body_ge_{str(th).replace('.', 'p')}"
            _alloc(name)
            states[name][i] = br[i] >= th
        # EMA distance thresholds
        dist = abs(e_fast[i] - e_slow[i])
        for th in thresholds_ema_dist:
            name = f"ema_dist_ge_{str(th).replace('.', 'p')}"
            _alloc(name)
            states[name][i] = dist >= th
        # Basic predicates
        for name, val in [
            ("ema_above", ema_above),
            ("ema_below", ema_below),
            ("ema_cross_up", ema_cross_up),
            ("ema_cross_dn", ema_cross_dn),
            ("psar_bull", psar_bull),
            ("psar_bear", psar_bear),
            ("psar_flip_recent", psar_flip),
            ("wpr_cross_up_from_oversold", w_up),
            ("wpr_cross_dn_from_overbought", w_dn),
        ]:
            _alloc(name)
            states[name][i] = bool(val)
        # Trend run lengths (using close diffs)
        up = closes[i] > closes[i-1]
        down = closes[i] < closes[i-1]
        _alloc("run_up_ge2"); _alloc("run_down_ge2")
        if i >= 2:
            states["run_up_ge2"][i] = up and (closes[i-1] > closes[i-2])
            states["run_down_ge2"][i] = down and (closes[i-1] < closes[i-2])

    return states


def label_outcomes_60s(candles: List[Dict[str, Any]], expiry_steps: int = 1) -> Tuple[List[bool], List[bool]]:
    n = len(candles)
    call_win = [False] * n
    put_win = [False] * n
    if n < expiry_steps + 1:
        return call_win, put_win
    closes = [float(x["close"]) for x in candles]
    for i in range(n - expiry_steps):
        entry = closes[i]
        exitp = closes[i + expiry_steps]
        call_win[i] = exitp > entry
        put_win[i] = exitp < entry
    return call_win, put_win


def _bars_in_hours(candles: List[Dict[str, Any]], allowed_hours: Optional[Set[int]]) -> List[bool]:
    if not allowed_hours:
        return [True] * len(candles)
    mask: List[bool] = []
    for c in candles:
        hr = _ist_hour(_ts(c))
        mask.append(hr in allowed_hours if hr is not None else False)
    return mask


def _select_indices(mask_lists: List[List[bool]]) -> List[int]:
    # Return indices where all masks are True
    if not mask_lists:
        return []
    n = len(mask_lists[0])
    out: List[int] = []
    for i in range(n):
        ok = True
        for m in mask_lists:
            if i >= len(m) or not m[i]:
                ok = False
                break
        if ok:
            out.append(i)
    return out


def _score_predicates_for_direction(states: Dict[str, List[bool]], labels: List[bool], eligible_idx: List[int]) -> List[Tuple[Tuple[str, ...], int, int, float]]:
    # Return list of (predicates, wins, trades, wr)
    results: List[Tuple[Tuple[str, ...], int, int, float]] = []
    keys = [k for k in states.keys()]
    # Evaluate size-1
    for k in keys:
        wins = trades = 0
        s = states[k]
        for i in eligible_idx:
            if i < len(s) and s[i]:
                trades += 1
                wins += 1 if labels[i] else 0
        wr = (wins / trades) if trades > 0 else 0.0
        results.append(((k,), wins, trades, wr))
    # Greedy build size-2 from best size-1
    results.sort(key=lambda x: (x[3], x[2]), reverse=True)
    top1 = [r for r in results[:10] if r[2] >= 10]  # reduce breadth for speed
    combos2: List[Tuple[Tuple[str, ...], int, int, float]] = []
    for a in top1:
        for b in top1:
            if a[0] >= b[0]:
                continue
            preds = a[0] + b[0]
            wins = trades = 0
            s1, s2 = states[preds[0]], states[preds[1]]
            for i in eligible_idx:
                if i < len(s1) and i < len(s2) and s1[i] and s2[i]:
                    trades += 1
                    wins += 1 if labels[i] else 0
            wr = (wins / trades) if trades > 0 else 0.0
            combos2.append((preds, wins, trades, wr))
    combos2.sort(key=lambda x: (x[3], x[2]), reverse=True)
    # Build size-3 from best size-2 (top 10 only)
    top2 = [r for r in combos2[:10] if r[2] >= 10]
    combos3: List[Tuple[Tuple[str, ...], int, int, float]] = []
    for a in top2:
        for k in keys:
            if k in a[0]:
                continue
            preds = a[0] + (k,)
            wins = trades = 0
            s_list = [states[p] for p in preds]
            for i in eligible_idx:
                if all(i < len(s) and s[i] for s in s_list):
                    trades += 1
                    wins += 1 if labels[i] else 0
            wr = (wins / trades) if trades > 0 else 0.0
            combos3.append((preds, wins, trades, wr))
    combos3.sort(key=lambda x: (x[3], x[2]), reverse=True)

    return results + combos2 + combos3


def mine_patterns_60s(per_asset_candles: Dict[str, List[Dict[str, Any]]],
                      allowed_hours: Optional[Set[int]] = None,
                      min_global_trades: int = 40,
                      min_asset_trades: int = 10,
                      min_asset_wr: float = 0.75) -> List[Pattern]:
    """
    Discover high-precision conjunctive patterns across assets (60s timeframe).
    Returns patterns with per-asset and global stats. Patterns are separate for call and put.
    """
    # Build feature states and labels per asset
    asset_states: Dict[str, Dict[str, List[bool]]] = {}
    asset_lbl_call: Dict[str, List[bool]] = {}
    asset_lbl_put: Dict[str, List[bool]] = {}
    asset_hour_mask: Dict[str, List[bool]] = {}

    for a, candles in per_asset_candles.items():
        if not isinstance(candles, list) or len(candles) < 120:
            continue
        st = build_feature_states_60s(candles)
        if not st:
            continue
        call_win, put_win = label_outcomes_60s(candles, expiry_steps=1)
        asset_states[a] = st
        asset_lbl_call[a] = call_win
        asset_lbl_put[a] = put_win
        asset_hour_mask[a] = _bars_in_hours(candles, allowed_hours)

    # Mine per direction on concatenated indices while maintaining per-asset breakdown
    def mine_for_direction(direction: str) -> List[Pattern]:
        patterns: List[Pattern] = []
        assets = list(asset_states.keys())
        if not assets:
            return patterns
        seed_asset = assets[0]
        states = asset_states[seed_asset]
        labels = asset_lbl_call[seed_asset] if direction == "call" else asset_lbl_put[seed_asset]
        eligible_idx = _select_indices([asset_hour_mask[seed_asset]])
        candidates = _score_predicates_for_direction(states, labels, eligible_idx)
        top = [c for c in candidates if c[2] >= 10]
        top = sorted(top, key=lambda x: (x[3], x[2]), reverse=True)[:30]
        for preds, _w, _t, _wr in top:
            global_wins = 0
            global_trades = 0
            per_asset: Dict[str, Tuple[int, int]] = {}
            for a in assets:
                st = asset_states[a]
                if any(p not in st for p in preds):
                    continue
                lbl = asset_lbl_call[a] if direction == "call" else asset_lbl_put[a]
                hour_mask = asset_hour_mask[a]
                idxs = _select_indices([hour_mask] + [st[p] for p in preds])
                wins = sum(1 for i in idxs if lbl[i])
                trades = len(idxs)
                if trades > 0:
                    per_asset[a] = (wins, trades)
                    global_wins += wins
                    global_trades += trades
            wr = (global_wins / global_trades) if global_trades > 0 else 0.0
            ok_assets = [a for a, (w, t) in per_asset.items() if t >= min_asset_trades and (w / t if t > 0 else 0.0) >= min_asset_wr]
            if global_trades >= min_global_trades and ok_assets:
                patterns.append(Pattern(direction=direction, predicates=preds, global_trades=global_trades,
                                        global_wins=global_wins, win_rate=wr, per_asset=per_asset))
        patterns.sort(key=lambda p: (p.win_rate, p.global_trades), reverse=True)
        seen = set()
        uniq: List[Pattern] = []
        for p in patterns:
            if p.predicates in seen:
                continue
            uniq.append(p)
            seen.add(p.predicates)
        return uniq[:20]

    out: List[Pattern] = []
    out.extend(mine_for_direction("call"))
    out.extend(mine_for_direction("put"))
    out.sort(key=lambda p: (p.win_rate, p.global_trades), reverse=True)
    return out


def mine_patterns_30s(per_asset_candles: Dict[str, List[Dict[str, Any]]],
                      allowed_hours: Optional[Set[int]] = None,
                      min_global_trades: int = 20,
                      min_asset_trades: int = 5,
                      min_asset_wr: float = 0.70) -> List[Pattern]:
    """
    Discover high-precision conjunctive patterns across assets (30s timeframe).
    Accepts shorter histories; uses build_feature_states_30s.
    """
    asset_states: Dict[str, Dict[str, List[bool]]] = {}
    asset_lbl_call: Dict[str, List[bool]] = {}
    asset_lbl_put: Dict[str, List[bool]] = {}
    asset_hour_mask: Dict[str, List[bool]] = {}

    for a, candles in per_asset_candles.items():
        if not isinstance(candles, list) or len(candles) < 20:
            continue
        st = build_feature_states_30s(candles)
        if not st:
            continue
        call_win, put_win = label_outcomes_60s(candles, expiry_steps=1)
        asset_states[a] = st
        asset_lbl_call[a] = call_win
        asset_lbl_put[a] = put_win
        asset_hour_mask[a] = _bars_in_hours(candles, allowed_hours)

    def mine_for_direction(direction: str) -> List[Pattern]:
        patterns: List[Pattern] = []
        assets = list(asset_states.keys())
        if not assets:
            return patterns
        seed_asset = assets[0]
        states = asset_states[seed_asset]
        labels = asset_lbl_call[seed_asset] if direction == "call" else asset_lbl_put[seed_asset]
        eligible_idx = _select_indices([asset_hour_mask[seed_asset]])
        candidates = _score_predicates_for_direction(states, labels, eligible_idx)
        top = [c for c in candidates if c[2] >= 5]
        top = sorted(top, key=lambda x: (x[3], x[2]), reverse=True)[:30]
        for preds, _w, _t, _wr in top:
            global_wins = 0
            global_trades = 0
            per_asset: Dict[str, Tuple[int, int]] = {}
            for a in assets:
                st = asset_states[a]
                if any(p not in st for p in preds):
                    continue
                lbl = asset_lbl_call[a] if direction == "call" else asset_lbl_put[a]
                hour_mask = asset_hour_mask[a]
                idxs = _select_indices([hour_mask] + [st[p] for p in preds])
                wins = sum(1 for i in idxs if lbl[i])
                trades = len(idxs)
                if trades > 0:
                    per_asset[a] = (wins, trades)
                    global_wins += wins
                    global_trades += trades
            wr = (global_wins / global_trades) if global_trades > 0 else 0.0
            ok_assets = [a for a, (w, t) in per_asset.items() if t >= min_asset_trades and (w / t if t > 0 else 0.0) >= min_asset_wr]
            if global_trades >= min_global_trades and ok_assets:
                patterns.append(Pattern(direction=direction, predicates=preds, global_trades=global_trades,
                                        global_wins=global_wins, win_rate=wr, per_asset=per_asset))
        patterns.sort(key=lambda p: (p.win_rate, p.global_trades), reverse=True)
        seen = set()
        uniq: List[Pattern] = []
        for p in patterns:
            if p.predicates in seen:
                continue
            uniq.append(p)
            seen.add(p.predicates)
        return uniq[:20]

    out: List[Pattern] = []
    out.extend(mine_for_direction("call"))
    out.extend(mine_for_direction("put"))
    out.sort(key=lambda p: (p.win_rate, p.global_trades), reverse=True)
    return out

