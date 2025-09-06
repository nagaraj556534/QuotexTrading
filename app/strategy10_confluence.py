from typing import List, Tuple


def compute_williams_r(highs: List[float], lows: List[float], closes: List[float], period: int = 7) -> List[float]:
    """
    Compute Williams %R over the given OHLC series.
    Returns a list aligned to input length where the first (period-1) values are None placeholders removed.
    Values are in range [-100, 0], where higher (towards 0) indicates stronger buying pressure.
    """
    n = len(closes)
    if n == 0 or len(highs) != n or len(lows) != n or n < period:
        return []
    result: List[float] = []
    for i in range(period - 1, n):
        hh = max(highs[i - period + 1:i + 1])
        ll = min(lows[i - period + 1:i + 1])
        c = closes[i]
        denom = (hh - ll) if hh != ll else 1e-12
        wr = -100.0 * (hh - c) / denom
        result.append(wr)
    return result


def compute_psar(highs: List[float], lows: List[float], step: float = 0.02, max_step: float = 0.3) -> List[float]:
    """
    Compute Parabolic SAR (PSAR) series.
    Minimal implementation returning a list aligned to input length (first value starts with prior extreme point).
    Uses standard Wilder's method with given acceleration step and maximum.
    """
    n = len(highs)
    if n == 0 or len(lows) != n:
        return []
    # Initialize trend: assume uptrend if last swing up is greater than previous low
    psar: List[float] = [0.0] * n
    uptrend = True
    af = step

    # Initialize EPs using first two bars
    ep_high = max(highs[0], highs[1] if n > 1 else highs[0])
    ep_low = min(lows[0], lows[1] if n > 1 else lows[0])
    psar[0] = ep_low  # initial psar below price for uptrend

    # Start from second bar
    for i in range(1, n):
        prev = i - 1
        prev_psar = psar[prev]

        if uptrend:
            # Update extreme point
            if highs[i] > ep_high:
                ep_high = highs[i]
                af = min(max_step, af + step)
            # Next PSAR prediction
            cur_psar = prev_psar + af * (ep_high - prev_psar)
            # PSAR cannot be above last two lows in uptrend
            cur_psar = min(cur_psar, lows[prev])
            if i >= 2:
                cur_psar = min(cur_psar, lows[prev - 1])
            # Check for reversal
            if lows[i] < cur_psar:
                # switch to downtrend
                uptrend = False
                psar[i] = ep_high  # on reversal, PSAR is set to prior EP
                ep_low = lows[i]
                af = step
            else:
                psar[i] = cur_psar
        else:
            # downtrend
            if lows[i] < ep_low:
                ep_low = lows[i]
                af = min(max_step, af + step)
            cur_psar = prev_psar + af * (ep_low - prev_psar)
            # PSAR cannot be below last two highs in downtrend
            cur_psar = max(cur_psar, highs[prev])
            if i >= 2:
                cur_psar = max(cur_psar, highs[prev - 1])
            # reversal?
            if highs[i] > cur_psar:
                uptrend = True
                psar[i] = ep_low
                ep_high = highs[i]
                af = step
            else:
                psar[i] = cur_psar

    return psar

