import asyncio
import time
from pyquotex.stable_api import Quotex

ASSETS = ['EURUSD_otc','BTCUSD_otc','AUDJPY_otc','EURUSD','USDJPY']
TFS = [10, 15, 30, 60, 120, 300]

async def main():
    email = input('Enter your email: ').strip()
    password = input('Enter your password: ').strip()
    acct = input('Account Type(D/L): ').strip().lower()
    qx = Quotex(email=email, password=password, lang='en')
    qx.set_account_mode('PRACTICE' if acct.startswith('d') else 'REAL')
    ok, reason = await qx.connect()
    print('connect:', ok, reason)
    if not ok:
        return
    for a in ASSETS:
        for tf in TFS:
            try:
                candles = await qx.get_candles(a, time.time(), tf*210, tf)
                n = 0 if not candles else len(candles)
                sample = None
                if candles:
                    c = candles[-1]
                    sample = {k: c.get(k) for k in ('open','high','low','close')}
                print(f'asset={a} tf={tf}s -> len={n} sample={sample}')
            except Exception as e:
                print(f'asset={a} tf={tf}s -> ERROR {e!r}')
    await qx.close()

if __name__ == '__main__':
    asyncio.run(main())

