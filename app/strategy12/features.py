from typing import List, Tuple

# Lightweight indicator helpers (pure python) — reuse ideas from strategy10_confluence where relevant

def ema(series: List[float], period: int) -> List[float]:
    if not series or period <= 1:
        return series[:]
    k = 2 / (period + 1)
    out: List[float] = []
    avg = series[0]
    for v in series:
        avg = v * k + avg * (1 - k)
        out.append(avg)
    return out


def sma(series: List[float], period: int) -> List[float]:
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


def williams_r(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> List[float]:
    n = len(closes)
    if n == 0 or not highs or not lows or len(highs) != n or len(lows) != n:
        return []
    out: List[float] = []
    for i in range(n):
        start = max(0, i - period + 1)
        hh = max(highs[start:i+1])
        ll = min(lows[start:i+1])
        d = hh - ll if hh != ll else 1e-9
        wr = -100.0 * (hh - closes[i]) / d
        out.append(wr)
    return out


def body_ratio(opens: List[float], closes: List[float], highs: List[float], lows: List[float]) -> List[float]:
    out: List[float] = []
    for o, c, h, l in zip(opens, closes, highs, lows):
        body = abs(c - o)
        rng = max(1e-9, h - l)
        out.append(body / rng)
    return out


def rsi(closes: List[float], period: int = 14) -> List[float]:
    n = len(closes)
    if n == 0:
        return []
    gains: List[float] = [0.0] * n
    losses: List[float] = [0.0] * n
    for i in range(1, n):
        chg = float(closes[i]) - float(closes[i - 1])
        gains[i] = max(0.0, chg)
        losses[i] = max(0.0, -chg)
    # Wilder smoothing
    rsis: List[float] = [50.0] * n
    if period < 1:
        return rsis
    avg_gain = sum(gains[1 : min(n, period + 1)]) / float(period)
    avg_loss = sum(losses[1 : min(n, period + 1)]) / float(period)
    rsis[period] = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + (avg_gain / (avg_loss if avg_loss != 0 else 1e-9))))
    for i in range(period + 1, n):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / float(period)
        avg_loss = (avg_loss * (period - 1) + losses[i]) / float(period)
        rs = avg_gain / (avg_loss if avg_loss != 0 else 1e-9)
        rsis[i] = 100.0 - (100.0 / (1.0 + rs))
    # Fill initial values
    for i in range(period):
        rsis[i] = rsis[period]
    return rsis


def macd(closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[List[float], List[float], List[float]]:
    if not closes:
        return [], [], []
    emf = ema(closes, fast)
    ems = ema(closes, slow)
    macd_line = [f - s for f, s in zip(emf, ems)]
    signal_line = ema(macd_line, signal)
    hist = [m - s for m, s in zip(macd_line, signal_line)]
    return macd_line, signal_line, hist


def bollinger_bands(closes: List[float], period: int = 20, stdev: float = 2.0) -> Tuple[List[float], List[float], List[float]]:
    if not closes:
        return [], [], []
    mid = sma(closes, period)
    n = len(closes)
    up: List[float] = []
    lo: List[float] = []
    for i in range(n):
        start = max(0, i - period + 1)
        window = [float(x) for x in closes[start : i + 1]]
        mean = sum(window) / float(len(window))
        var = sum((x - mean) ** 2 for x in window) / float(len(window))
        sd = var ** 0.5
        up.append(mid[i] + stdev * sd)
        lo.append(mid[i] - stdev * sd)
    return mid, up, lo


def stochastic(highs: List[float], lows: List[float], closes: List[float], k_period: int = 14, d_period: int = 3, smooth_k: int = 3) -> Tuple[List[float], List[float]]:
    n = min(len(highs), len(lows), len(closes))
    if n == 0:
        return [], []
    raw_k: List[float] = []
    for i in range(n):
        start = max(0, i - k_period + 1)
        hh = max(highs[start : i + 1])
        ll = min(lows[start : i + 1])
        rng = max(1e-9, hh - ll)
        k = 100.0 * (closes[i] - ll) / rng
        raw_k.append(k)
    # Smooth %K
    if smooth_k > 1:
        k_s = sma(raw_k, smooth_k)
    else:
        k_s = raw_k
    d = sma(k_s, d_period)
    return k_s, d


def atr(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> List[float]:
    """Average True Range (Wilder) using simple initialization.
    Returns a list aligned to inputs. If insufficient data, returns partial ATRs.
    """
    n = min(len(highs), len(lows), len(closes))
    if n == 0:
        return []
    # True Range series
    tr: List[float] = []
    prev_close = closes[0]
    for i in range(n):
        h = float(highs[i])
        l = float(lows[i])
        c_prev = float(prev_close) if i > 0 else float(closes[0])
        tr_val = max(h - l, abs(h - c_prev), abs(l - c_prev))
        tr.append(tr_val)
        prev_close = float(closes[i])
    if period <= 1:
        return tr
    out: List[float] = []
    # Seed ATR with simple average of first 'period' TRs (or all available if shorter)
    seed_len = min(period, len(tr))
    if seed_len == 0:
        return []
    seed = sum(tr[:seed_len]) / float(seed_len)
    out.extend([seed] * seed_len)
    atr_prev = seed
    for i in range(seed_len, len(tr)):
        atr_cur = atr_prev + (tr[i] - atr_prev) / float(period)
        out.append(atr_cur)
        atr_prev = atr_cur
    return out

# --- Trend helpers ---

def hh_hl_trend(highs: List[float], lows: List[float], idx: int, lookback: int = 20) -> str:
    """Simple HH/HL vs LH/LL detection over two halves of a window ending at idx."""
    n = min(len(highs), len(lows))
    if n == 0 or idx <= 0:
        return "sideways"
    start = max(0, idx - lookback + 1)
    win_h = highs[start:idx+1]
    win_l = lows[start:idx+1]
    if len(win_h) < 4:
        return "sideways"
    mid = len(win_h) // 2
    prev_h, prev_l = max(win_h[:mid]), min(win_l[:mid])
    last_h, last_l = max(win_h[mid:]), min(win_l[mid:])
    if last_h > prev_h and last_l > prev_l:
        return "up"
    if last_h < prev_h and last_l < prev_l:
        return "down"
    return "sideways"


def trend_direction_strength(closes: List[float], highs: List[float], lows: List[float], atr_series: List[float], idx: int, lookback: int = 30, ema_period: int = 11) -> Tuple[str, float]:
    """Combine ATR-normalized price drift and HH/HL pattern to assess trend.
    Returns (dir: 'up'|'down'|'sideways', strength [0..1])."""
    n = len(closes)
    if n == 0 or idx <= 0:
        return "sideways", 0.0
    i0 = max(0, idx - lookback)
    drift = float(closes[idx]) - float(closes[i0])
    atrv = float(atr_series[idx]) if idx < len(atr_series) and atr_series[idx] else 0.0
    norm = abs(drift) / (max(1e-9, atrv) * (1.0 + (lookback ** 0.5)))
    norm = max(0.0, min(1.0, norm))
    drift_dir = "up" if drift > 0 else ("down" if drift < 0 else "sideways")
    hhhl = hh_hl_trend(highs, lows, idx, lookback=max(8, lookback // 2))
    # Combine: both signals agreeing → strong, else weak/sideways
    if drift_dir == "up" and hhhl == "up":
        return "up", min(1.0, 0.6 + 0.4 * norm)
    if drift_dir == "down" and hhhl == "down":
        return "down", min(1.0, 0.6 + 0.4 * norm)
    if drift_dir in ("up","down"):
        return drift_dir, 0.3 * norm
    return "sideways", 0.0


# --- Additional indicators ---

def adx_di(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> Tuple[List[float], List[float], List[float]]:
    n = min(len(highs), len(lows), len(closes))
    if n == 0:
        return [], [], []
    # True range and directional movement
    tr: List[float] = [0.0] * n
    plus_dm: List[float] = [0.0] * n
    minus_dm: List[float] = [0.0] * n
    for i in range(1, n):
        up = float(highs[i]) - float(highs[i - 1])
        dn = float(lows[i - 1]) - float(lows[i])
        plus_dm[i] = up if (up > dn and up > 0) else 0.0
        minus_dm[i] = dn if (dn > up and dn > 0) else 0.0
        tr[i] = max(
            float(highs[i]) - float(lows[i]),
            abs(float(highs[i]) - float(closes[i - 1])),
            abs(float(lows[i]) - float(closes[i - 1])),
        )
    def _smooth(xs: List[float], p: int) -> List[float]:
        out: List[float] = [0.0] * n
        s = sum(xs[1 : min(n, p + 1)])
        out[p] = s
        for i in range(p + 1, n):
            s = s - (s / p) + xs[i]
            out[i] = s
        for i in range(p):
            out[i] = out[p] if p < n else 0.0
        return out
    atr_s = _smooth(tr, period)
    pdi = [0.0] * n
    ndi = [0.0] * n
    for i in range(n):
        d = atr_s[i] if atr_s[i] != 0 else 1e-9
        pdi[i] = 100.0 * _smooth(plus_dm, period)[i] / d
        ndi[i] = 100.0 * _smooth(minus_dm, period)[i] / d
    dx: List[float] = [0.0] * n
    for i in range(n):
        denom = max(1e-9, (pdi[i] + ndi[i]))
        dx[i] = 100.0 * abs(pdi[i] - ndi[i]) / denom
    # ADX is smoothed DX
    adx: List[float] = [0.0] * n
    s = sum(dx[1 : min(n, period + 1)]) / float(period) if n > period else 0.0
    if n > period:
        adx[period] = s
        for i in range(period + 1, n):
            s = (s * (period - 1) + dx[i]) / float(period)
            adx[i] = s
        for i in range(period):
            adx[i] = adx[period]
    return adx, pdi, ndi


def supertrend(highs: List[float], lows: List[float], closes: List[float], atr_period: int = 10, multiplier: float = 3.0) -> List[str]:
    n = min(len(highs), len(lows), len(closes))
    if n == 0:
        return []
    from typing import cast
    # Compute ATR (Wilder-like)
    # Reuse atr() above
    atr_vals = atr(highs, lows, closes, period=atr_period)
    basic_upper = []
    basic_lower = []
    for i in range(n):
        hl2 = (float(highs[i]) + float(lows[i])) / 2.0
        a = float(atr_vals[i]) if i < len(atr_vals) else 0.0
        basic_upper.append(hl2 + multiplier * a)
        basic_lower.append(hl2 - multiplier * a)
    final_upper = [0.0] * n
    final_lower = [0.0] * n
    st: List[float] = [0.0] * n
    dirn: List[str] = ["sideways"] * n
    for i in range(n):
        if i == 0:
            final_upper[i] = basic_upper[i]
            final_lower[i] = basic_lower[i]
            st[i] = final_upper[i]
            dirn[i] = "down"
        else:
            final_upper[i] = basic_upper[i] if (basic_upper[i] < final_upper[i-1] or float(closes[i-1]) > final_upper[i-1]) else final_upper[i-1]
            final_lower[i] = basic_lower[i] if (basic_lower[i] > final_lower[i-1] or float(closes[i-1]) < final_lower[i-1]) else final_lower[i-1]
            if st[i-1] == final_upper[i-1]:
                st[i] = final_upper[i] if float(closes[i]) <= final_upper[i] else final_lower[i]
            else:
                st[i] = final_lower[i] if float(closes[i]) >= final_lower[i] else final_upper[i]
            dirn[i] = "up" if float(closes[i]) >= st[i] else "down"
    return dirn


def bb_bandwidth(closes: List[float], period: int = 20, stdev: float = 2.0) -> List[float]:
    mid, up, lo = bollinger_bands(closes, period, stdev)
    out: List[float] = []
    for m, u, l in zip(mid, up, lo):
        rng = abs(u - l)
        denom = abs(m) if m != 0 else 1.0
        out.append(rng / (abs(denom) + 1e-9))
    return out


def keltner_channels(highs: List[float], lows: List[float], closes: List[float], ema_period: int = 20, atr_period: int = 10, mult: float = 1.5) -> Tuple[List[float], List[float], List[float]]:
    if not closes:
        return [], [], []
    mid = ema(closes, ema_period)
    atr_vals = atr(highs, lows, closes, atr_period)
    up = [m + mult * a for m, a in zip(mid, atr_vals)]
    lo = [m - mult * a for m, a in zip(mid, atr_vals)]
    return mid, up, lo


def donchian(highs: List[float], lows: List[float], period: int = 20) -> Tuple[List[float], List[float], List[float]]:
    n = min(len(highs), len(lows))
    if n == 0:
        return [], [], []
    up: List[float] = []
    lo: List[float] = []
    mid: List[float] = []
    for i in range(n):
        s = max(0, i - period + 1)
        u = max(highs[s : i + 1])
        l = min(lows[s : i + 1])
        up.append(float(u))
        lo.append(float(l))
        mid.append((float(u) + float(l)) / 2.0)
    return up, lo, mid


def heikin_ashi(opens: List[float], highs: List[float], lows: List[float], closes: List[float]) -> Tuple[List[float], List[float]]:
    n = min(len(opens), len(highs), len(lows), len(closes))
    if n == 0:
        return [], []
    ha_o: List[float] = [0.0] * n
    ha_c: List[float] = [0.0] * n
    for i in range(n):
        ha_c[i] = (float(opens[i]) + float(highs[i]) + float(lows[i]) + float(closes[i])) / 4.0
        if i == 0:
            ha_o[i] = (float(opens[i]) + float(closes[i])) / 2.0
        else:
            ha_o[i] = (ha_o[i-1] + ha_c[i-1]) / 2.0
    return ha_o, ha_c


def fractal_swings(highs: List[float], lows: List[float], window: int = 2) -> Tuple[List[bool], List[bool]]:
    """Detect simple swing highs/lows using fractals (window=2 means 5-bar fractal)."""
    n = min(len(highs), len(lows))
    sh = [False] * n
    sl = [False] * n
    for i in range(window, n - window):
        if highs[i] == max(highs[i - window : i + window + 1]):
            sh[i] = True
        if lows[i] == min(lows[i - window : i + window + 1]):
            sl[i] = True
    return sh, sl
