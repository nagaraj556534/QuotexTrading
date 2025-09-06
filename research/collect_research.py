import asyncio
import time
import csv
import os
from typing import List, Dict

from pyquotex.stable_api import Quotex

# Minimal, independent collector to avoid touching trading loop
# Usage: python -m research.collect_research (will prompt credentials)

LOG_PATH = os.path.join(os.path.dirname(__file__), "research_dataset.csv")


def ensure_headers():
    if not os.path.exists(LOG_PATH):
        with open(LOG_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "ts","asset","tf","open","high","low","close",
                "ema5","ema21","ema50","rsi7","rsi14","stoch_k","stoch_d",
                "adx14","atr14","body_ratio","dist_ema21","zz_dir","is_otc",
                "weekday","hour","payout","future_ret_60s"
            ])


def ema(values: List[float], period: int) -> List[float]:
    if len(values) < period:
        return []
    k = 2/(period+1)
    out = [sum(values[:period])/period]
    for v in values[period:]:
        out.append((v - out[-1])*k + out[-1])
    return out


def rma(values: List[float], period: int) -> List[float]:
    if len(values) < period:
        return []
    out = [sum(values[:period])/period]
    for v in values[period:]:
        out.append((out[-1]*(period-1) + v)/period)
    return out


def atr(highs: List[float], lows: List[float], closes: List[float], period: int=14) -> List[float]:
    if len(highs) < period+1:
        return []
    trs: List[float] = []
    for i in range(1, len(highs)):
        tr = max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1]))
        trs.append(tr)
    return rma(trs, period)


def stoch_slow(closes: List[float], highs: List[float], lows: List[float], k_period:int=14, smooth_k:int=3, smooth_d:int=3):
    if len(closes) < k_period+smooth_k+smooth_d:
        return [], []
    fast_k: List[float] = []
    for i in range(k_period-1, len(closes)):
        hh = max(highs[i-k_period+1:i+1]); ll = min(lows[i-k_period+1:i+1])
        k = 50.0 if hh==ll else (closes[i]-ll)/(hh-ll)*100
        fast_k.append(k)
    def sma(vals: List[float], p: int) -> List[float]:
        if len(vals) < p: return []
        out = []
        s = sum(vals[:p]); out.append(s/p)
        for i in range(p, len(vals)):
            s += vals[i] - vals[i-1]
            out.append(s/p)
        return out
    slow_k = sma(fast_k, smooth_k)
    slow_d = sma(slow_k, smooth_d) if slow_k else []
    return slow_k, slow_d


def adx(highs: List[float], lows: List[float], closes: List[float], period:int=14) -> List[float]:
    if len(highs) < period+1:
        return []
    plus_dm = [0.0]; minus_dm=[0.0]; trs=[0.0]
    for i in range(1, len(highs)):
        up = highs[i]-highs[i-1]; dn = lows[i-1]-lows[i]
        plus_dm.append(up if up>dn and up>0 else 0.0)
        minus_dm.append(dn if dn>up and dn>0 else 0.0)
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    atr_vals = rma(trs[1:], period)
    if len(atr_vals) < period: return []
    def _r(vals): return rma(vals, period)
    smp=_r(plus_dm[1:]); smm=_r(minus_dm[1:])
    plus_di=[]; minus_di=[]
    for i in range(len(atr_vals)):
        d=atr_vals[i] if atr_vals[i]!=0 else 1e-9
        plus_di.append(100*(smp[i]/d)); minus_di.append(100*(smm[i]/d))
    dx=[]
    for p,m in zip(plus_di, minus_di):
        s=p+m if p+m!=0 else 1e-9
        dx.append(100*abs(p-m)/s)
    return rma(dx, period)


async def get_all_assets(qx) -> List[str]:
    names: List[str] = []
    instruments = await qx.get_instruments()
    for i in instruments:
        try:
            names.append(i[1])
        except Exception:
            continue
    return names


def is_otc(sym: str) -> int:
    return int(sym.lower().endswith("_otc"))


def get_payout(qx: Quotex, asset: str, expiry_min: int=1) -> float:
    for k in ("1","60"):
        try:
            v = qx.get_payout_by_asset(asset, timeframe=k)
            if v is not None:
                return float(v)
        except Exception:
            continue
    try:
        return float(qx.get_payout_by_asset(asset) or 0)
    except Exception:
        return 0.0


async def collect_loop(qx: Quotex, tf_list: List[int], payout_floor: float=0.0):
    ensure_headers()
    assets = await get_all_assets(qx)
    while True:
        for asset in assets:
            p = get_payout(qx, asset, 1)
            if p < payout_floor:
                continue
            for tf in tf_list:
                try:
                    candles = await qx.get_candles(asset, time.time(), tf*210, tf)
                    if not candles or len(candles) < 70:
                        continue
                    closes=[float(c['close']) for c in candles]
                    highs=[float(c['high']) for c in candles]
                    lows=[float(c['low']) for c in candles]
                    opens=[float(c['open']) for c in candles]
                    e5=ema(closes,5); e21=ema(closes,21); e50=ema(closes,50)
                    r7_vals=[]; r14_vals=[]
                    try:
                        # If pyquotex exposes RSI, you can call it, else compute custom
                        from math import isnan
                    except Exception:
                        pass
                    # Simple RSI(14) substitute via price change smoothing can be added later
                    k,d = stoch_slow(closes, highs, lows, 14,3,3)
                    ax = adx(highs, lows, closes, 14)
                    at = atr(highs, lows, closes, 14)
                    if not e21 or not k or not d or not ax or not at:
                        continue
                    last = candles[-1]
                    body = abs(float(last['close']) - float(last['open']))
                    rng = max(1e-9, float(last['high']) - float(last['low']))
                    body_ratio = body / rng
                    dist_ema21 = float(last['close']) - e21[-1]
                    from datetime import datetime
                    now = datetime.now()
                    future_close = float(candles[-1]['close'])
                    # Grab one more minute forward if available
                    # Note: in live streaming, next loop will give us future; here we approximate using last two closes
                    if len(candles) >= 72:
                        future_close = float(candles[-1]['close'])
                    future_ret_60s = 0.0  # placeholder, will be filled by post-process if needed
                    row = [
                        int(time.time()), asset, tf,
                        float(last['open']), float(last['high']), float(last['low']), float(last['close']),
                        e5[-1], e21[-1], e50[-1], 0.0, 0.0, k[-1], d[-1],
                        ax[-1], at[-1], body_ratio, dist_ema21, "", is_otc(asset),
                        now.strftime('%a'), int(now.strftime('%H')), p, future_ret_60s
                    ]
                    with open(LOG_PATH, 'a', newline='', encoding='utf-8') as f:
                        csv.writer(f).writerow(row)
                except Exception:
                    continue
        await asyncio.sleep(5)


async def main():
    email = input("Enter your email: ").strip()
    password = input("Enter your password: ").strip()
    account_mode = input("Account Type(D/L): ").strip().lower()
    qx = Quotex(email=email, password=password, lang="en")
    qx.set_account_mode("demo" if account_mode.startswith('d') else "real")

    # Robust connect with a few retries
    for attempt in range(1, 4):
        try:
            ok, reason = await qx.connect()
            if ok:
                print(f"Connected on attempt {attempt}. Starting research collector...")
                break
            else:
                print(f"Connect failed (attempt {attempt}): {reason}")
        except Exception as e:
            print(f"Connect exception (attempt {attempt}): {e}")
        await asyncio.sleep(2)
    else:
        print("Unable to connect after retries. Exiting.")
        return

    try:
        await collect_loop(qx, tf_list=[30,60], payout_floor=0.0)
    finally:
        try:
            await qx.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())

