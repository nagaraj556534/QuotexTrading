import asyncio
import time
from typing import Tuple
from pyquotex.stable_api import Quotex

# This small probe uses Strategy 9 conditions from main (re-implemented lightweight)
# to scan assets and print first few with a signal (no orders placed)

ASSETS = [
    'EURUSD_otc','USDJPY_otc','BTCUSD_otc','AUDJPY_otc','EURUSD','USDJPY','XAUUSD_otc'
]
TF = 60  # recommended timeframe for Strategy 9

# Minimal helpers

def ema(vals, p):
    if len(vals) < p: return []
    k=2/(p+1); out=[sum(vals[:p])/p]
    for v in vals[p:]: out.append((v-out[-1])*k+out[-1])
    return out

def rma(vals, p):
    if len(vals) < p: return []
    out=[sum(vals[:p])/p]
    for v in vals[p:]: out.append((out[-1]*(p-1)+v)/p)
    return out

def atr(h,l,c,p=14):
    if len(h) < p+1: return []
    trs=[]
    for i in range(1,len(h)):
        trs.append(max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])))
    return rma(trs,p)

def adx(h,l,c,p=14):
    if len(h) < p+1: return []
    plus_dm=[0.0]; minus_dm=[0.0]; trs=[0.0]
    for i in range(1,len(h)):
        up=h[i]-h[i-1]; dn=l[i-1]-l[i]
        plus_dm.append(up if up>dn and up>0 else 0.0)
        minus_dm.append(dn if dn>up and dn>0 else 0.0)
        trs.append(max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])))
    atr_vals=rma(trs[1:],p)
    if len(atr_vals)<p: return []
    smp=rma(plus_dm[1:],p); smm=rma(minus_dm[1:],p)
    plus_di=[]; minus_di=[]
    for i in range(len(atr_vals)):
        d=atr_vals[i] if atr_vals[i]!=0 else 1e-9
        plus_di.append(100*(smp[i]/d)); minus_di.append(100*(smm[i]/d))
    dx=[]
    for pv,mv in zip(plus_di,minus_di):
        s=pv+mv if pv+mv!=0 else 1e-9
        dx.append(100*abs(pv-mv)/s)
    return rma(dx,p)

def stoch_slow(c,h,l,kp=14,sk=3,sd=3):
    if len(c) < kp+sk+sd: return [],[]
    fk=[]
    for i in range(kp-1, len(c)):
        hh=max(h[i-kp+1:i+1]); ll=min(l[i-kp+1:i+1])
        fk.append(50.0 if hh==ll else (c[i]-ll)/(hh-ll)*100)
    def sma(v,p):
        if len(v)<p: return []
        out=[]; s=sum(v[:p]); out.append(s/p)
        for i in range(p, len(v)):
            s+=v[i]-v[i-1]; out.append(s/p)
        return out
    skv=sma(fk,sk); sdv=sma(skv,sd) if skv else []
    return skv,sdv

async def main():
    email=input('Enter your email: ').strip(); password=input('Enter your password: ').strip(); acct=input('Account Type(D/L): ').strip().lower()
    qx=Quotex(email=email,password=password,lang='en'); qx.set_account_mode('PRACTICE' if acct.startswith('d') else 'REAL')
    ok,reason=await qx.connect(); print('connect:', ok, reason)
    if not ok: return
    for a in ASSETS:
        try:
            chosen_tf = None
            candles = None
            for _tf in (60, 30, 10):
                try:
                    _candles = await qx.get_candles(a, time.time(), _tf * 1200, _tf)
                except Exception:
                    _candles = None
                if _candles and len(_candles) >= 60:
                    chosen_tf = _tf
                    candles = _candles
                    break
            if not candles:
                print(f'{a}: insufficient candles')
                continue
            c=[float(x['close']) for x in candles]
            h=[float(x['high']) for x in candles]
            l=[float(x['low']) for x in candles]
            o=[float(x['open']) for x in candles]
            e50=ema(c,50); e21=ema(c,21)
            if not e50 or not e21:
                continue
            trend_up = c[-1]>e50[-1] and e50[-1]>=e50[-2]
            trend_down = c[-1]<e50[-1] and e50[-1]<=e50[-2]
            prev_close, cur_close = c[-2], c[-1]
            prev_open, cur_open = o[-2], o[-1]
            prev_mid, cur_mid = e21[-2], e21[-1]
            is_green = cur_close>cur_open; is_red = cur_close<cur_open
            cross_now_up = prev_close<prev_mid and cur_close>cur_mid and is_green
            cross_now_down = prev_close>prev_mid and cur_close<cur_mid and is_red
            prev2_close, prev2_mid = c[-3], e21[-3]
            prev_is_green = prev_close>prev_open; prev_is_red = prev_close<prev_open
            cross_prev_up = prev2_close<prev2_mid and prev_close>prev_mid and prev_is_green
            cross_prev_down = prev2_close>prev2_mid and prev_close<prev_mid and prev_is_red
            cross_up = cross_now_up or cross_prev_up
            cross_down = cross_now_down or cross_prev_down
            # RSI
            rsi = await qx.calculate_indicator(a, 'RSI', {'period':14}, timeframe=TF)
            rvals=rsi.get('rsi', [])
            rcur=float(rvals[-1]) if rvals else 50.0
            rsi_ok_up = 55<=rcur<=70; rsi_ok_down = 30<=rcur<=45
            # Stoch
            k,d = stoch_slow(c,h,l,14,3,3)
            if not k or not d:
                continue
            prev_k, cur_k = k[-2], k[-1]
            prev_d, cur_d = d[-2], d[-1]
            stoch_buy = prev_k<prev_d and cur_k>cur_d and min(prev_k,cur_k)<=40
            stoch_sell = prev_k>prev_d and cur_k<cur_d and max(prev_k,cur_k)>=60
            # ADX & ATR
            ax=adx(h,l,c,14); at=atr(h,l,c,14)
            if not ax or not at: continue
            adx_ok = ax[-1]>=18
            dist_ok = abs(cur_close-cur_mid) >= 0.03*at[-1]
            buy = trend_up and cross_up and rsi_ok_up and stoch_buy and adx_ok and dist_ok
            sell = trend_down and cross_down and rsi_ok_down and stoch_sell and adx_ok and dist_ok
            if buy or sell:
                side = 'CALL' if buy else 'PUT'
                print(f'{a}: SIGNAL {side} rsi={rcur:.1f} adx={ax[-1]:.1f} dist={abs(cur_close-cur_mid)/at[-1]:.2f} tf={TF}s')
        except Exception as e:
            print(f'{a}: error {e!r}')
    await qx.close()

if __name__ == '__main__':
    asyncio.run(main())

