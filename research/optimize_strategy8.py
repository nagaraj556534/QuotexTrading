import csv
import json
import os
from typing import List, Dict, Any

# Simple grid search optimizer over threshold rules to maximize expected value (EV)
# Input: research/research_dataset.csv
# Output: research/strategy8_config.json

DATA_PATH = os.path.join(os.path.dirname(__file__), "research_dataset.csv")
OUT_PATH = os.path.join(os.path.dirname(__file__), "strategy8_config.json")


def load_rows() -> List[Dict[str, Any]]:
    rows = []
    if not os.path.exists(DATA_PATH):
        print("No dataset found:", DATA_PATH)
        return rows
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        cr = csv.DictReader(f)
        for r in cr:
            rows.append(r)
    return rows


def compute_ev(wins: int, losses: int, avg_payout: float) -> float:
    total = wins + losses
    if total == 0:
        return -1.0
    win_rate = wins / total
    # EV per 1 unit risk
    return win_rate * (avg_payout/100.0) - (1 - win_rate)


def grid_search(rows: List[Dict[str, Any]]):
    # Buckets by weekday-hour optional later
    best = {"ev": -2.0}
    # Basic thresholds to try
    body_opts = [0.25, 0.3, 0.35]
    adx_opts = [15, 18, 20]
    stoch_zone = [(20,80), (25,75)]
    dist_opts = [0.0, 0.05]

    # Labeling proxy: if close>open on next bar, count as win for CALL; this is placeholder.
    # In data collection, future_ret_60s should be filled; for now, we simulate with last row (improve later).

    for body_min in body_opts:
        for adx_min in adx_opts:
            for oslv, obuv in stoch_zone:
                for dist_frac in dist_opts:
                    wins=losses=0
                    payout_sum=0.0
                    cnt=0
                    for r in rows:
                        try:
                            body=float(r["body_ratio"]) >= body_min
                            adx=float(r["adx14"]) >= adx_min
                            k=float(r["stoch_k"]); d=float(r["stoch_d"]) 
                            stoch_buy = (k> d) and min(k,d) <= oslv
                            stoch_sell = (k< d) and max(k,d) >= obuv
                            dist_ok = abs(float(r["dist_ema21"])) >= dist_frac * float(r["atr14"]) if float(r["atr14"]) else False
                            if not (body and adx and dist_ok and (stoch_buy or stoch_sell)):
                                continue
                            # Placeholder label: assume win if close>open for buy-like setup
                            o=float(r["open"]); c=float(r["close"])
                            win = 1 if c>o else 0
                            if win: wins+=1
                            else: losses+=1
                            payout_sum += float(r["payout"]) if r["payout"] else 0.0
                            cnt+=1
                        except Exception:
                            continue
                    avg_payout = (payout_sum/cnt) if cnt>0 else 0.0
                    ev = compute_ev(wins, losses, avg_payout)
                    if ev > best.get("ev", -2.0):
                        best = {
                            "ev": ev,
                            "params": {
                                "body_min": body_min,
                                "adx_min": adx_min,
                                "stoch_os": oslv,
                                "stoch_ob": obuv,
                                "dist_frac_atr": dist_frac
                            },
                            "wins": wins,
                            "losses": losses,
                            "avg_payout": avg_payout,
                            "count": cnt
                        }
    return best


def main():
    rows = load_rows()
    if len(rows) < 5000:
        print(f"Not enough rows ({len(rows)}) to optimize. Need >= 5000.")
        return
    best = grid_search(rows)
    print("Best:", best)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(best, f, indent=2)
    print("Saved config to", OUT_PATH)


if __name__ == "__main__":
    main()

