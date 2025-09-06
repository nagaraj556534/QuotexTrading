# NOTE: This module is imported by app/main.py. Keep line lengths modest to appease linters.

from __future__ import annotations
import asyncio
import time
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple, Optional, Set

# Safe tabulate import with fallback (no external dependency required)
try:
    from tabulate import tabulate  # type: ignore
except Exception:
    def tabulate(rows, headers=None, tablefmt="github"):
        # Minimal fallback: render a simple markdown-like table
        rows = rows or []
        headers = headers or []
        data = [headers] + rows if headers else rows
        if not data:
            return ""
        # Compute column widths
        widths = [max(len(str(x)) for x in col) for col in zip(*data)]
        def fmt_row(r):
            return "| " + " | ".join(str(x).ljust(w) for x, w in zip(r, widths)) + " |"
        out = []
        if headers:
            out.append(fmt_row(headers))
            out.append("|" + "|".join("-" * (w + 2) for w in widths) + "|")
        for r in rows:
            out.append(fmt_row(r))
        return "\n".join(out)

# Simple progress helper
def _pct(done: int, total: int) -> str:
    total = max(1, total)
    return f"{int(100*done/total)}%"

def _print_phase(msg: str):
    print(f"[S12] {msg}")

def _print_progress(prefix: str, done: int, total: int, extra: str = ""):
    bar = _pct(done, total)
    if extra:
        print(f"[S12] {prefix}: {done}/{total} ({bar}) | {extra}")
    else:
        print(f"[S12] {prefix}: {done}/{total} ({bar})")

# Candle ts extractor
def _ts(c: Dict[str, Any]) -> Optional[float]:
    for k in ("from", "time", "timestamp"):
        if k in c:
            try:
                return float(c[k])
            except Exception:
                return None
    return None

# Heartbeat helper (prints every ~10s during long waits)
class _Heartbeat:
    def __init__(self, phase: str):
        self.phase = phase
        self.t0 = time.time()
        self.last = 0.0
    def tick(self):
        now = time.time()
        if now - self.last >= 10.0:
            elapsed = int(now - self.t0)
            print(f"[S12] Working... t={elapsed}s (Phase: {self.phase})")
            self.last = now

from .search import generate_rule_variants, refine_variant, generate_broader_variants
from .search_wide import generate_rule_variants_wide_60s
from .rules import RuleVariant
from .backtest import backtest_variant, backtest_variant_confluence, backtest_variant_confluence_gated
from .runlog import RunLogger

@dataclass
class S12Config:
    timeframes_s: List[int]
    min_payout: float
    target_accuracy: float
    min_trades: int
    ist_hours: Optional[Set[int]]
    max_variants: int = 10
    bars_per_tf: int = 3600  # Base bars; adaptive per TF
    oos_frac: float = 0.5  # Use 50% for evaluation (more trades)
    show_per_asset_breakdown: bool = True
    asset_min_wr: float = 0.75  # Per-asset minimum win rate
    asset_min_trades: int = 15  # Per-asset minimum trades
    display_min_acc: float = 0.80  # Only show >= this WR in table
    selection_min_acc: float = 0.80  # Only return >= this WR unless fallback
    adaptive_bars: bool = True  # Use different bars per timeframe
    early_stop_refinement: bool = True  # Stop when enough >=80% variants
    max_parallel_assets: int = 8  # Parallel asset processing limit
    confluence_mode: bool = True  # 2-of-3 multi-TF agreement
    # Optional: restrict analysis to only these assets (single-asset mode supported)
    only_assets: Optional[Set[str]] = None
    # Manual IST hour selection (overrides auto-optimization when enabled)
    manual_ist_hours: bool = False
    manual_selected_hours: Optional[Set[int]] = None
    # Paper trading simulation (optional)
    paper_trading: bool = False
    paper_mode: str = "price"  # "price" or "pct"
    paper_profit_target: float = 0.0
    paper_loss_limit: float = 0.0
    # Fast mode (speed-first preset)
    fast_mode: bool = False
    fast_assets_whitelist: Optional[Set[str]] = None  # if None, use default top-8 when fast_mode
    # Live trading hour override (bypass hour optimization for immediate testing)
    live_hour_override: bool = True  # If True, ignore variant hour restrictions in PRACTICE
    current_ist_hour_force: Optional[int] = None  # Force this IST hour for testing (e.g., 15)
    # Diagnostic toggles
    diagnostic_mode: bool = False  # If True, relax filters and show full stats
    diag_oos_frac: Optional[float] = None  # Override oos_frac in diagnosis (e.g., 0.2)
    diag_disable_hours: bool = False  # If True, ignore IST hour filter during diagnosis
    diag_show_all: bool = True  # Show all variants table before strict filter

    # Enhanced Strategy 12 - 95%+ Win Rate Discovery System
    strict95_mode: bool = True  # Enable 95%+ win rate discovery system
    strict95_target_wr: float = 0.95  # Target win rate for strict mode
    strict95_fallback_targets: List[float] = None  # Progressive fallback [0.90, 0.85, 0.80]
    strict95_min_trades: int = 50  # Minimum trades for 95% mode
    strict95_oos_frac: float = 0.6  # Larger OOS split to prevent overfitting
    strict95_confluence_mode: str = "3of3"  # "2of3", "3of3", or "adaptive"
    strict95_max_refinement_iterations: int = 5  # Max refinement loops
    strict95_pretrade_body_min: float = 0.48  # Stricter body ratio
    strict95_pretrade_atr_band: Tuple[float, float] = (48.0, 62.0)  # Tighter ATR band
    strict95_min_conf: float = 0.88  # Higher confidence threshold
    strict95_max_ist_hours: int = 2  # Limit to 1-2 most profitable hours
    strict95_cross_asset_validation: bool = True  # Require cross-asset performance
    strict95_pattern_mining_depth: int = 3  # Enhanced pattern discovery depth
    strict95_enable_loss_analysis: bool = True  # Analyze failed trades for refinement
    # Hour analysis controls
    hour_analysis_min_trades: int = 20
    hour_analysis_top_k: int = 3
    # Focused hour filter for backtests/selection (PRACTICE can still override)
    focus_hours: Optional[Set[int]] = None


async def _load_candles_map(qx, assets: List[str], tfs: List[int], bars_per_tf: int, show_progress: bool = False, max_concurrent: int = 8, cache_enable: bool = True, live_collect_secs: int = 0) -> Dict[Tuple[str, int], List[Dict[str, Any]]]:
    m: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
    now = time.time()
    import asyncio as _aio

    total = len(assets) * len(tfs)
    done = 0

    # Optional stricter TA overrides applied to each variant at runtime
    override_params: Optional[Dict[str, Any]] = None

    hb = _Heartbeat("Loading candles") if show_progress else None
    if show_progress:
        _print_phase(f"Phase 2: Loading candles (bars={bars_per_tf})...")

    # On-disk cache folder for candle snapshots (helps when OTC lacks history)
    import os, json
    CACHE_DIR = os.path.join(os.getcwd(), 'artifacts', 'strategy12', 'cache')
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
    except Exception:
        pass

    def _cache_key(asset: str, tf: int) -> str:
        safe = asset.replace('/', '_')
        return os.path.join(CACHE_DIR, f"{safe}_{tf}s.json")

    def _cache_load(asset: str, tf: int) -> List[Dict[str, Any]]:
        try:
            p = _cache_key(asset, tf)
            if not os.path.exists(p):
                return []
            with open(p, 'r', encoding='utf-8') as f:
                arr = json.load(f)
            # sanity: ensure dicts with time
            return [x for x in arr if isinstance(x, dict) and 'time' in x]
        except Exception:
            return []

    def _cache_save(asset: str, tf: int, candles: List[Dict[str, Any]]):
        try:
            p = _cache_key(asset, tf)
            with open(p, 'w', encoding='utf-8') as f:
                json.dump(candles[-5000:], f)  # cap file size
        except Exception:
            pass

    # Concurrency controls
    task_sem = _aio.Semaphore(max(1, max_concurrent))
    api_lock = _aio.Lock()

    async def fetch_one(a: str, tf: int):
        async with task_sem:
            # Adaptive target bars per TF
            target_map = {
                60: max(1200, int(bars_per_tf)),
                30: max(2400, int(bars_per_tf * 2)),
                15: max(4800, int(bars_per_tf * 4)),
            }
            target = target_map.get(tf, bars_per_tf)
            # Per-TF chunk sizes to avoid rate limits
            chunk_map = {15: 600, 30: 300, 60: 200}
            chunk = chunk_map.get(tf, min(900, target))
            acc: Dict[float, Dict[str, Any]] = {}
            # Seed from cache if enabled
            if cache_enable:
                try:
                    cached = _cache_load(a, tf)
                    for ci in cached:
                        ts = _ts(ci)
                        if ts is not None:
                            acc[ts] = ci
                    if show_progress and cached:
                        print(f"[S12][CACHE] Seed {a}@{tf}s from cache: {len(cached)} bars")
                except Exception:
                    pass
            end_ts = now
            last_min_ts = None
            iters = 0
            max_iters = 60
            while len(acc) < target and iters < max_iters:
                # Timeout + retries (API calls must be serialized)
                c: List[Dict[str, Any]] = []
                for attempt in range(3):
                    try:
                        async with api_lock:
                            c = await _aio.wait_for(qx.get_candles(a, end_ts, tf * chunk, tf), timeout=10.0)
                        break
                    except Exception:
                        c = []
                        await _aio.sleep(0.3 * (attempt + 1))
                        continue
                if not c:
                    # Fallback attempts for sub-minute TFs (OTC often lacks history)
                    if tf in (15, 30):
                        try:
                            if show_progress:
                                print(f"[S12][DIAG] Empty response for {a}@{tf}s; trying fallbacks...")
                            # Align end_ts to tf boundary
                            try:
                                import math, time as _time
                                now_ts = _time.time()
                                end_ts_aligned = math.floor(now_ts / tf) * tf
                            except Exception:
                                end_ts_aligned = end_ts
                            # Try multiple offsets and progressive flag
                            fb_offsets = [tf * 210, tf * 1200, tf * 2400]
                            fb_prog = [False, True]
                            got = False
                            for off in fb_offsets:
                                for prog in fb_prog:
                                    try:
                                        async with api_lock:
                                            c2 = await _aio.wait_for(
                                                qx.get_candles(a, end_ts_aligned, off, tf, prog), timeout=10.0
                                            )
                                        n2 = 0 if not c2 else len(c2)
                                        if show_progress:
                                            print(f"[S12][DIAG] try {a}@{tf}s off={off} prog={prog} -> {n2}")
                                        if c2 and n2 > 0:
                                            for ci in c2:
                                                ts = _ts(ci)
                                                if ts is not None:
                                                    acc[ts] = ci
                                            got = True
                                            break
                                    except Exception as _e2:
                                        if show_progress:
                                            print(f"[S12][DIAG] fallback error {a}@{tf}s off={off} prog={prog}: {_e2}")
                                        await _aio.sleep(0.1)
                                        continue
                                if got:
                                    break
                            if not got:
                                # One last attempt with tiny window
                                try:
                                    async with api_lock:
                                        c3 = await _aio.wait_for(qx.get_candles(a, end_ts_aligned, tf * 120, tf), timeout=8.0)
                                    if c3:
                                        for ci in c3:
                                            ts = _ts(ci)
                                            if ts is not None:
                                                acc[ts] = ci
                                        if show_progress:
                                            print(f"[S12][DIAG] tiny window worked {a}@{tf}s -> {len(c3)}")
                                        # do not break here; continue loop to backfill further
                                        continue
                                except Exception as _e3:
                                    if show_progress:
                                        print(f"[S12][DIAG] tiny window error {a}@{tf}s: {_e3}")
                                    pass
                        except Exception:
                            pass
                    # If still nothing, break
                    if len(acc) == 0 and live_collect_secs > 0 and tf in (15, 30):
                        # Live collection fallback (short window)
                        try:
                            if show_progress:
                                print(f"[S12][LIVE] Collecting realtime {a}@{tf}s for up to {live_collect_secs}s...")
                            import time as _time
                            t_end = _time.time() + float(live_collect_secs)
                            while _time.time() < t_end:
                                try:
                                    async with api_lock:
                                        c4 = await _aio.wait_for(qx.get_candle_v2(a, tf), timeout=10.0)
                                except Exception:
                                    c4 = []
                                if c4:
                                    for ci in c4:
                                        ts = _ts(ci)
                                        if ts is not None:
                                            acc[ts] = ci
                                if show_progress:
                                    print(f"[S12][LIVE] {a}@{tf}s collected={len(acc)}")
                                await _aio.sleep(max(0.5, tf / 5.0))
                        except Exception:
                            pass
                    if len(acc) == 0:
                        break
                # merge
                for ci in c:
                    ts = _ts(ci)
                    if ts is not None:
                        acc[ts] = ci
                loaded = len(acc)
                if show_progress:
                    print(f"[S12] Loading {a}@{tf}s: loaded={loaded}/{target}")
                    if hb:
                        hb.tick()
                # move window backward
                min_ts = min((_ts(ci) for ci in c if _ts(ci) is not None), default=None)
                if min_ts is None:
                    break
                if last_min_ts is not None and min_ts >= last_min_ts:
                    # no progress; shrink chunk or break
                    if chunk > 50:
                        chunk = max(50, chunk // 2)
                        end_ts = min_ts - tf
                        iters += 1
                        await _aio.sleep(0.15)
                        continue
                    break
                last_min_ts = min_ts
                end_ts = min_ts - tf
                iters += 1
                # pacing
                await _aio.sleep(0.15)
            candles = [acc[k] for k in sorted(acc.keys())]
            # Save to cache
            if cache_enable and candles:
                try:
                    _cache_save(a, tf, candles)
                except Exception:
                    pass
            return (a, tf, candles)

    tasks = [fetch_one(a, tf) for a in assets for tf in tfs]
    for coro in _aio.as_completed(tasks):
        a, tf, candles = await coro
        done += 1
        # Store whatever we got; 'OK' readiness is computed later by thresholds
        if candles:
            m[(a, tf)] = candles
        if show_progress:
            loaded = len(candles) if candles else 0
            extra = f"{a}@{tf}s loaded={loaded}"
            _print_progress("Candles", done, total, extra=extra)
            hb.tick()
    return m


    t0 = time.time()
    _print_phase("Phase 1: Asset discovery + payout filter...")


async def run_pipeline(qx, cfg: S12Config) -> List[dict]:
    """End-to-end: generate → backtest → refine (iterative) → rank.
    Artifacts are logged to artifacts/strategy12/runs/<timestamp> for transparency.
    """
    runlog = RunLogger()
    runlog.write_json("config.json", {
        "timeframes_s": cfg.timeframes_s,
        "min_payout": cfg.min_payout,
        "target_accuracy": cfg.target_accuracy,
        "min_trades": cfg.min_trades,
        "ist_hours": list(cfg.ist_hours) if cfg.ist_hours else None,
        "bars_per_tf": cfg.bars_per_tf,
        "oos_frac": cfg.oos_frac,
    })
    # 1) Assets: reuse get_instruments and filter by payout >= min_payout
    try:
        instruments = await qx.get_instruments()
    except Exception:
        instruments = []
    assets = [i[1] for i in instruments] if instruments else []
    # Filter by payout at 1m (proxy); in live we gate per expiry
    elig = []
    for a in assets:
        try:
            p = qx.get_payout_by_asset(a, timeframe="1")
            if p and float(p) >= cfg.min_payout:
                elig.append(a)
        except Exception:
            continue

    # Fast mode whitelist of assets (top-8) if enabled
    if getattr(cfg, "fast_mode", False):
        default_top8 = {"TONUSD_otc","EURUSD_otc","WIFUSD_otc","ZECUSD_otc","MELUSD_otc","LINUSD_otc","GBPAUD_otc","AUDUSD_otc"}
        wl = getattr(cfg, "fast_assets_whitelist", None) or default_top8
        elig = [a for a in elig if a in wl]

    # Eligible assets summary (Tanglish)
    names_preview = ", ".join(elig[:12]) + ("..." if len(elig) > 12 else "")
    _print_phase(
        f"Eligible assets: {len(elig)}/{len(assets)} (min_payout={int(cfg.min_payout)}): {names_preview}"
    )

    if not elig:
        print("No eligible assets found by payout filter; proceeding with all.")
        elig = assets

    # 2) Load candles for required TFs (adaptive bars per TF)
    tf_bias = sorted(cfg.timeframes_s, key=lambda x: (x != 15, x))  # prefer 15s first
    candles_map: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
    if cfg.adaptive_bars:
        # Reduced targets for better OTC compatibility
        bars_map = {15: 1800, 30: 2400, 60: 5400}
        _print_phase("Phase 2: Loading candles (adaptive bars per TF)...")
        for tf in tf_bias:
            live_secs = 15 if tf in (15, 30) else 0
            part = await _load_candles_map(
                qx, elig, [tf], bars_map.get(tf, cfg.bars_per_tf), show_progress=True, live_collect_secs=live_secs
            )
            candles_map.update(part)
    else:
        candles_map = await _load_candles_map(qx, elig, tf_bias, cfg.bars_per_tf, show_progress=True, live_collect_secs=15)

    # Per-asset/TF loaded bars summary table
    rows = []
    assets_set = sorted(set(a for (a, _tf) in candles_map.keys()))
    for a in assets_set:
        c15_list = candles_map.get((a, 15), [])
        c30_list = candles_map.get((a, 30), [])
        c60_list = candles_map.get((a, 60), [])
        c15 = len(c15_list) if isinstance(c15_list, list) else 0
        c30 = len(c30_list) if isinstance(c30_list, list) else 0
        c60 = len(c60_list) if isinstance(c60_list, list) else 0
        status = "OK" if (c15 or c30 or c60) else "NO DATA"
        rows.append([a, c15, c30, c60, status])
    print(tabulate(rows, headers=["Asset", "15s bars", "30s bars", "60s bars", "Status"], tablefmt="github"))
    print(f"[S12] Total loaded pairs: {len(candles_map)}")

    # Multi-TF readiness summary (30s has lower bar requirement now)
    ok15 = sum(1 for a in assets_set if isinstance(candles_map.get((a, 15), []), list) and len(candles_map.get((a, 15), [])) >= 120)
    ok30 = sum(1 for a in assets_set if isinstance(candles_map.get((a, 30), []), list) and len(candles_map.get((a, 30), [])) >= 14)
    ok60 = sum(1 for a in assets_set if isinstance(candles_map.get((a, 60), []), list) and len(candles_map.get((a, 60), [])) >= 120)
    print(
        f"[S12] Multi-TF readiness: 15s OK={ok15} assets, 30s OK={ok30} assets, 60s OK={ok60} assets (OK means ≥14 bars for 30s, ≥120 for others)"
    )

    # If confluence is not possible for some assets, inform fallback
    fallback_assets = []
    fallback_tf_union: Set[int] = set()
    for a in assets_set:
        def _enough_tf(tf_val: int, arr: List[Dict[str, Any]]) -> bool:
            return len(arr) >= (20 if tf_val == 30 else 120)
        av = [tf for tf in (15, 30, 60)
              if isinstance(candles_map.get((a, tf), []), list)
              and _enough_tf(tf, candles_map.get((a, tf), []))]
        if 1 <= len(av) < 2:
            fallback_assets.append(a)
            for tf in av:
                fallback_tf_union.add(tf)
    if fallback_assets:
        tf_names = ", ".join(f"{tf}s" for tf in sorted(fallback_tf_union)) if fallback_tf_union else "none"
        only_str = " only" if len(fallback_tf_union) == 1 else ""
        print(
            f"[S12] Confluence unavailable; using single-TF fallback for {len(fallback_assets)} assets (available TFs: {tf_names}{only_str})"
        )

    _print_phase(f"Phase 2 done: loaded {len(candles_map)} asset-TF pairs")

    # If nothing loaded above threshold, exit early with guidance
    if not candles_map:
        print("[S12] No historical data loaded (>=120 bars). Try lowering min_payout or rerun later.")
        return []

    # 3) Phase 0 (new): Pattern discovery for TF-specific → synthesize initial variants
    variants: List[RuleVariant]
    if cfg.timeframes_s == [30]:
        try:
            from .patterns import mine_patterns_30s as _mine
            from .learner import synthesize_rule_variants_from_patterns
            _print_phase("Phase 0: Mining data-driven patterns (30s)...")
            per_asset_tf = {a: candles_map.get((a, 30), []) for a in assets_set}
            patterns = _mine(
                {a: cs for a, cs in per_asset_tf.items() if isinstance(cs, list) and len(cs) >= 20},
                allowed_hours=None if (getattr(cfg, "diagnostic_mode", False) and getattr(cfg, "diag_disable_hours", False)) else cfg.ist_hours,
                min_global_trades=max(10, cfg.min_trades // 2),
                min_asset_trades=max(3, (getattr(cfg, 'asset_min_trades', 0) // 4) or 3),
                min_asset_wr=max(0.65, getattr(cfg, 'asset_min_wr', 0.0) or 0.65),
            )
            runlog.write_json("mined_patterns.json", [
                {
                    "direction": getattr(p, "direction", getattr(p, "direction", None)),
                    "predicates": list(getattr(p, "predicates", [])),
                    "global_trades": getattr(p, "global_trades", 0),
                    "global_wins": getattr(p, "global_wins", 0),
                    "win_rate": getattr(p, "win_rate", 0.0),
                    "per_asset": getattr(p, "per_asset", {}),
                } for p in patterns
            ])
            synth = synthesize_rule_variants_from_patterns(patterns, base_name="12m")
            variants = [sv.variant for sv in synth]
            if not variants:
                _print_phase("Phase 0 produced no variants; falling back to 30s-wide templates")
                try:
                    from .search_wide_30s import generate_rule_variants_wide_30s as _gen30
                    variants = _gen30(cfg.max_variants)
                except Exception:
                    from .search_wide import generate_rule_variants_wide_60s as _gen
                    variants = _gen(cfg.max_variants)
            _print_phase(f"Phase 3: Variant generation (mined first) — {len(variants)} candidates")
        except Exception as e:
            print(f"[S12] Pattern mining failed ({e}); using 60s-wide templates")
            from .search_wide import generate_rule_variants_wide_60s as _gen
            variants = _gen(cfg.max_variants)
            _print_phase(f"Phase 3: Variant generation (60s-wide) — {len(variants)} candidates")
    elif cfg.timeframes_s == [60]:
        try:
            from .patterns import mine_patterns_60s
            from .learner import synthesize_rule_variants_from_patterns
            _print_phase("Phase 0: Mining data-driven patterns (60s)...")
            per_asset_tf = {a: candles_map.get((a, 60), []) for a in assets_set}
            patterns = mine_patterns_60s(
                {a: cs for a, cs in per_asset_tf.items() if isinstance(cs, list) and len(cs) >= 120},
                allowed_hours=None if (getattr(cfg, "diagnostic_mode", False) and getattr(cfg, "diag_disable_hours", False)) else cfg.ist_hours,
                min_global_trades=max(40, cfg.min_trades),
                min_asset_trades=max(10, cfg.asset_min_trades // 2),
                min_asset_wr=max(0.75, cfg.asset_min_wr),
            )
            runlog.write_json("mined_patterns.json", [
                {
                    "direction": getattr(p, "direction", getattr(p, "direction", None)),
                    "predicates": list(getattr(p, "predicates", [])),
                    "global_trades": getattr(p, "global_trades", 0),
                    "global_wins": getattr(p, "global_wins", 0),
                    "win_rate": getattr(p, "win_rate", 0.0),
                    "per_asset": getattr(p, "per_asset", {}),
                } for p in patterns
            ])
            synth = synthesize_rule_variants_from_patterns(patterns, base_name="12m")
            variants = [sv.variant for sv in synth]
            if not variants:
                _print_phase("Phase 0 produced no variants; falling back to 60s-wide templates")
                from .search_wide import generate_rule_variants_wide_60s as _gen
                variants = _gen(cfg.max_variants)
            _print_phase(f"Phase 3: Variant generation (mined first) — {len(variants)} candidates")
        except Exception as e:
            print(f"[S12] Pattern mining failed ({e}); using 60s-wide templates")
            from .search_wide import generate_rule_variants_wide_60s as _gen
            variants = _gen(cfg.max_variants)
            _print_phase(f"Phase 3: Variant generation (60s-wide) — {len(variants)} candidates")
    else:
        try:
            # Use new curated high-precision generator guided by timeframes and strict bias
            from .search import generate_rule_variants_smart as _smart
            strict_bias = bool(getattr(cfg, "strict95_mode", False))
            variants = _smart(cfg.max_variants, timeframes=list(cfg.timeframes_s), strict_bias=strict_bias)
            _print_phase(f"Phase 3: Variant generation (smart) — {len(variants)} candidates")
        except Exception:
            from .search import generate_rule_variants as _gen
            variants = _gen(cfg.max_variants)
            _print_phase(f"Phase 3: Variant generation — {len(variants)} candidates")

    # Build per-asset timeframe map (for confluence mode)
    per_asset_map: Dict[str, Dict[int, List[Dict[str, Any]]]] = {}
    for (a, tf), cs in candles_map.items():
        per_asset_map.setdefault(a, {})[tf] = cs

    # ---- Multi-timeframe pretrade gates for backtests (no API calls) ----
    from .features import ema as _ema_f, atr as _atr_f

    MTF_MIN = {15: 4800, 30: 2400, 60: 1200}

    def _asset_counts(tfs_for_asset: Dict[int, List[Dict[str, Any]]]) -> Dict[int, int]:
        return {tf: len(tfs_for_asset.get(tf, []) or []) for tf in (15, 30, 60)}

    def _has_full_mtf(tfs_for_asset: Dict[int, List[Dict[str, Any]]]) -> bool:
        c = _asset_counts(tfs_for_asset)
        return c.get(60, 0) >= MTF_MIN[60] and c.get(30, 0) >= MTF_MIN[30] and c.get(15, 0) >= MTF_MIN[15]

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

    def _body_ratio(c: Dict[str, Any]) -> float:
        try:
            body = abs(float(c["close"]) - float(c["open"]))
            rng = max(1e-9, float(c["high"]) - float(c["low"]))
            return (body / rng) if rng else 0.0
        except Exception:
            return 0.0

    def _ts_safe(c: Dict[str, Any]) -> float:
        t = _ts(c)
        return float(t or 0.0)

    def _find_idx_leq(candles: List[Dict[str, Any]], ts: float) -> int:
        # find rightmost index with time <= ts
        if not candles:
            return -1
        lo, hi = 0, len(candles) - 1
        ans = -1
        while lo <= hi:
            mid = (lo + hi) // 2
            cts = _ts_safe(candles[mid])
            if cts <= ts:
                ans = mid
                lo = mid + 1
            else:
                hi = mid - 1
        return ans

    def _gated_backtest_single(
        tf: int,
        candles: List[Dict[str, Any]],
        tfs_for_asset: Dict[int, List[Dict[str, Any]]],
        rule_eval,
        allowed_hours: Optional[Set[int]],
        oos_frac: float,
        min_trades_goal: Optional[int],
    ):
        # Precompute 60s arrays for gating
        c60 = tfs_for_asset.get(60, [])
        closes60 = [float(x["close"]) for x in c60] if c60 else []
        e11_60 = _ema_f(closes60, 11) if closes60 else []
        e55_60 = _ema_f(closes60, 55) if closes60 else []
        e7_60  = _ema_f(closes60, 7) if closes60 else []
        atr60  = _atr_f([float(x.get("high",0)) for x in c60], [float(x.get("low",0)) for x in c60], closes60, 14) if c60 else []
        # 30s/15s helpers
        c30 = tfs_for_asset.get(30, [])
        closes30 = [float(x["close"]) for x in c30] if c30 else []
        e11_30 = _ema_f(closes30, 11) if closes30 else []
        e55_30 = _ema_f(closes30, 55) if closes30 else []
        c15 = tfs_for_asset.get(15, [])
        closes15 = [float(x["close"]) for x in c15] if c15 else []
        e11_15 = _ema_f(closes15, 11) if closes15 else []

        n = len(candles)
        if n == 0:
            from types import SimpleNamespace
            return SimpleNamespace(total_trades=0, wins=0, ist_hour_stats={})
        min_bars = 20 if tf == 30 else 60
        base_idx = int(n * (1.0 - max(0.0, min(0.9, oos_frac))))
        start_idx = max(1 if tf == 30 else min_bars, base_idx)

        wins = 0
        total = 0
        hours: Dict[int, Tuple[int, int]] = {}
        # Confidence by payout tier from cfg.min_payout (proxy for history)
        min_conf = 0.60 if cfg.min_payout >= 90.0 else 0.64

        for i in range(start_idx, n - 1):
            prefix = candles[: i + 1]
            has_signal, direction = rule_eval(prefix)
            if not has_signal:
                continue
            if allowed_hours:
                hrts = _ts(prefix[-1])
                hr = None
                try:
                    from datetime import datetime as _dt
                    from zoneinfo import ZoneInfo as _ZI
                    if hrts:
                        hr = _dt.utcfromtimestamp(hrts).replace(tzinfo=_ZI("UTC")).astimezone(_ZI("Asia/Kolkata")).hour
                except Exception:
                    hr = None
                if hr is not None and hr not in allowed_hours:
                    continue
            # Locate 60s anchor index for gating
            ts_cur = _ts_safe(prefix[-1])
            i60 = _find_idx_leq(c60, ts_cur) if c60 else -1
            # Build features
            align60 = False
            body_ok = False
            atr_ok = False
            atr_pctile = 0.0
            microtrend_ok = False
            align30 = False
            slope15 = False
            if i60 >= 0 and i60 < len(c60):
                try:
                    if i60 < len(e11_60) and i60 < len(e55_60):
                        align60 = (e11_60[i60] > e55_60[i60]) if direction == "call" else (e11_60[i60] < e55_60[i60])
                    if i60 < len(e7_60):
                        last2 = [min(i60, len(e7_60)-1), min(i60, len(e7_60)-1)]
                    # microtrend bodies
                    if i60 >= 2:
                        last3 = c60[i60-2:i60+1]
                        bodies = [(float(k["close"]) - float(k["open"])) for k in last3]
                        dir_bools = [(b > 0) if direction == "call" else (b < 0) for b in bodies]
                        micro_bodies_ok = sum(1 for x in dir_bools if x) >= 2
                        # EMA7 side
                        if i60 >= 1 and i60 < len(e7_60):
                            c1 = float(c60[i60-1]["close"]) > float(e7_60[i60-1]) if direction == "call" else float(c60[i60-1]["close"]) < float(e7_60[i60-1])
                            c2 = float(c60[i60]["close"])   > float(e7_60[i60])   if direction == "call" else float(c60[i60]["close"])   < float(e7_60[i60])
                            microtrend_ok = micro_bodies_ok and (c1 and c2)
                    # body ratio
                    body_ok = _body_ratio(c60[i60]) >= 0.33
                    # ATR percentile
                    if i60 < len(atr60) and i60 >= 1:
                        window = atr60[max(0, i60-119):i60+1]
                        atr_pctile = _pct_rank(window, float(atr60[i60]))
                        atr_ok = 35.0 <= atr_pctile <= 80.0
                except Exception:
                    pass
            # 30s align
            if c30 and len(e11_30) and len(e55_30):
                j30 = _find_idx_leq(c30, ts_cur)
                if j30 >= 0 and j30 < len(e11_30) and j30 < len(e55_30):
                    align30 = (e11_30[j30] > e55_30[j30]) if direction == "call" else (e11_30[j30] < e55_30[j30])
            # 15s slope
            if c15 and len(e11_15) >= 2:
                k15 = _find_idx_leq(c15, ts_cur)
                if k15 >= 1 and k15 < len(e11_15):
                    slope15 = (e11_15[k15] > e11_15[k15-1]) if direction == "call" else (e11_15[k15] < e11_15[k15-1])

            # Score and pass check (must-haves: align60, body_ok, atr_ok)
            w = {"align60": 0.30, "align30": 0.20, "slope15": 0.15, "micro": 0.20, "body": 0.10, "atr": 0.05}
            score = 0.0
            if align60: score += w["align60"]
            if align30: score += w["align30"]
            if slope15: score += w["slope15"]
            if microtrend_ok: score += w["micro"]
            if body_ok: score += w["body"]
            if atr_ok: score += w["atr"]
            passed = align60 and body_ok and atr_ok and (score >= min_conf)
            if not passed:
                continue
            # Outcome eval: unify to 60s expiry for all TFs when 60s is present
            if i60 < 0 or i60 + 1 >= len(c60):
                continue
            c_entry = float(c60[i60]["close"])  # anchor on 60s bar at or before ts
            c_exit = float(c60[i60 + 1]["close"])  # next 60s close
            won = (c_exit > c_entry) if direction == "call" else (c_exit < c_entry)
            total += 1
            wins += 1 if won else 0
            ts = _ts(candles[i])
            hr = None
            if ts is not None:
                try:
                    from datetime import datetime as _dt
                    from zoneinfo import ZoneInfo as _ZI
                    hr = _dt.utcfromtimestamp(ts).replace(tzinfo=_ZI("UTC")).astimezone(_ZI("Asia/Kolkata")).hour
                except Exception:
                    hr = None
            if hr is not None:
                W, T = hours.get(hr, (0, 0))
                hours[hr] = (W + (1 if won else 0), T + 1)
        from types import SimpleNamespace
        return SimpleNamespace(total_trades=total, wins=wins, ist_hour_stats=hours)

    # Fast rule-eval builder: precompute indicators once per candles and evaluate last bar only
    def _make_fast_rule_eval(candles: List[Dict[str, Any]], params: Dict[str, Any], tf: Optional[int] = None):
        min_bars = 20 if tf == 30 else max(60, int(params.get("min_bars", 60)))
        if not candles or len(candles) < min_bars:
            return lambda prefix: (False, "call")
        from .features import ema as _ema, williams_r as _wr, body_ratio as _br
        try:
            from strategy10_confluence import compute_psar as _psar  # type: ignore
        except Exception:
            try:
                from .strategy10_confluence import compute_psar as _psar  # type: ignore
            except Exception:
                _psar = None  # type: ignore
        opens = [float(x.get("open", 0)) for x in candles]
        highs = [float(x.get("high", 0)) for x in candles]
        lows = [float(x.get("low", 0)) for x in candles]
        closes = [float(x.get("close", 0)) for x in candles]
        e_fast = _ema(closes, int(params.get("ema_fast", 11)))
        e_slow = _ema(closes, int(params.get("ema_slow", 55)))
        wr = _wr(highs, lows, closes, period=int(params.get("wpr_period", 14)))
        br = _br(opens, closes, highs, lows)
        if _psar:
            psar = _psar(highs, lows, step=float(params.get("psar_step", 0.02)), max_step=float(params.get("psar_max", 0.3)))
        else:
            psar = [float('inf')] * len(closes)
        def _eval(prefix: List[Dict[str, Any]]):
            i = len(prefix) - 1
            if len(closes) < min_bars or i < 1 or i >= len(closes):
                return False, "call"
            if i >= len(e_fast) or i >= len(e_slow) or i >= len(psar) or i >= len(wr) or i >= len(br):
                return False, "call"
            cross_up = e_fast[i-1] <= e_slow[i-1] and e_fast[i] > e_slow[i]
            cross_dn = e_fast[i-1] >= e_slow[i-1] and e_fast[i] < e_slow[i]
            psar_bull = closes[i] > psar[i]
            psar_bear = closes[i] < psar[i]
            wpr_up = wr[i-1] < float(params.get("wpr_upper_in", -20)) and wr[i] > float(params.get("wpr_upper_out", -80))
            wpr_dn = wr[i-1] > float(params.get("wpr_lower_in", -80)) and wr[i] < float(params.get("wpr_lower_out", -20))
            # More permissive body ratio default for 30s
            br_thresh = float(params.get("min_body_ratio", 0.10 if tf == 30 else 0.25))
            br_ok = br[i] >= br_thresh
            dist_ok = abs(e_fast[i] - e_slow[i]) >= float(params.get("min_ema_dist", 0.0))
            if tf == 30:
                ema_above = e_fast[i] > e_slow[i]
                ema_below = e_fast[i] < e_slow[i]
                # 30s permissive: EMA relation OR cross, plus (W%R cross OR PSAR state), and body OK
                if ((ema_above or cross_up) and (wpr_up or psar_bull) and br_ok):
                    return True, "call"
                if ((ema_below or cross_dn) and (wpr_dn or psar_bear) and br_ok):
                    return True, "put"
            else:
                if cross_up and psar_bull and wpr_up and br_ok and dist_ok:
                    return True, "call"
                if cross_dn and psar_bear and wpr_dn and br_ok and dist_ok:
                    return True, "put"
            return False, "call"
        return _eval

    # 4) Backtest each variant with parallel per-asset processing
    async def _eval_variant(v: RuleVariant) -> dict:
        total_trades = 0
        total_wins = 0
        all_hours: Dict[int, Tuple[int, int]] = {}
        per_asset: Dict[str, Dict[str, int]] = {}

        sem = asyncio.Semaphore(max(1, cfg.max_parallel_assets))

        # Strict95 toggles/params
        strict = bool(getattr(cfg, "strict95_mode", False))
        conf_mode = getattr(cfg, "strict95_confluence_mode", "adaptive") if strict else ("2of3" if cfg.confluence_mode else "off")
        min_conf_base = float(getattr(cfg, "strict95_min_conf", 0.75) if strict else (0.60 if cfg.min_payout >= 90.0 else 0.64))
        gate_body_min = float(getattr(cfg, "strict95_pretrade_body_min", 0.40) if strict else 0.33)
        gate_atr_lo, gate_atr_hi = (getattr(cfg, "strict95_pretrade_atr_band", (40.0, 70.0)) if strict else (35.0, 80.0))
        ofrac_default = float(getattr(cfg, "strict95_oos_frac", 0.6) if strict else cfg.oos_frac)
        min_goal_default = int(getattr(cfg, "strict95_min_trades", 50) if strict else (40 if cfg.timeframes_s == [60] else cfg.min_trades))

        async def eval_one_asset(a: str) -> Tuple[str, Any]:
            async with sem:
                asset_tfs = per_asset_map.get(a, {})
                if not asset_tfs:
                    return a, None
                # Merge strict TA overrides into variant params if provided
                if getattr(cfg, 'override_params', None):
                    if isinstance(v.params, dict):
                        v.params.update(cfg.override_params)

                # Hours
                ah = v.params.get("allowed_hours") if isinstance(v, RuleVariant) and isinstance(v.params, dict) and v.params.get("allowed_hours") else None
                if ah is None:
                    if cfg.timeframes_s == [30]:
                        ah = None
                    elif cfg.timeframes_s == [60] and isinstance(v, RuleVariant) and str(getattr(v, 'name', '')).startswith('12m.'):
                        ah = None
                    else:
                        ah = None if (getattr(cfg, "diagnostic_mode", False) and getattr(cfg, "diag_disable_hours", False)) else cfg.ist_hours
                ofrac = ofrac_default
                if getattr(cfg, "diagnostic_mode", False) and getattr(cfg, "diag_oos_frac", None) is not None:
                    ofrac = cfg.diag_oos_frac  # type: ignore
                min_goal = min_goal_default

                # Confluence availability
                c15 = asset_tfs.get(15, [])
                c30 = asset_tfs.get(30, [])
                c60 = asset_tfs.get(60, [])
                def _enough(candles: List[Dict[str, Any]], tf_val: int) -> bool:
                    return len(candles) >= (14 if tf_val == 30 else 120)
                available_tfs = sum(1 for c, tfv in zip([c15, c30, c60], [15, 30, 60]) if _enough(c, tfv))
                have_two = available_tfs >= 2

                if have_two and cfg.confluence_mode:
                    fast60 = _make_fast_rule_eval(c60, v.params, 60) if c60 else (lambda p: (False, "call"))
                    # Low-data safeguards (same as earlier; strict95 uses higher base min_conf but we still escalate in very low data)
                    is_low_data = (len(c15) < 180) or (len(c30) < 40) or (len(c60) < 180)
                    min_conf = max(min_conf_base, 0.78) if is_low_data else min_conf_base
                    strict_micro = True

                    # Precompute once per asset for 60s-gated features to speed up backtest
                    from .features import ema as _ema_f, atr as _atr_f
                    closes60 = [float(x.get("close", 0)) for x in c60] if c60 else []
                    highs60 = [float(x.get("high", 0)) for x in c60] if c60 else []
                    lows60  = [float(x.get("low", 0)) for x in c60] if c60 else []
                    e11_60  = _ema_f(closes60, 11) if closes60 else []
                    e55_60  = _ema_f(closes60, 55) if closes60 else []
                    e7_60   = _ema_f(closes60, 7) if closes60 else []
                    atr60   = _atr_f(highs60, lows60, closes60, 14) if closes60 else []
                    closes30 = [float(x.get("close", 0)) for x in c30] if c30 else []
                    e11_30   = _ema_f(closes30, 11) if closes30 else []
                    e55_30   = _ema_f(closes30, 55) if closes30 else []
                    closes15 = [float(x.get("close", 0)) for x in c15] if c15 else []
                    e11_15   = _ema_f(closes15, 11) if closes15 else []
                    pre = {
                        "closes60": closes60,
                        "highs60": highs60,
                        "lows60": lows60,
                        "e11_60": e11_60,
                        "e55_60": e55_60,
                        "e7_60": e7_60,
                        "atr60": atr60,
                        "closes30": closes30,
                        "e11_30": e11_30,
                        "e55_30": e55_30,
                        "closes15": closes15,
                        "e11_15": e11_15,
                    }

                    paper_args = None
                    if getattr(cfg, "paper_trading", False) and float(getattr(cfg, "paper_profit_target", 0.0)) > 0 and float(getattr(cfg, "paper_loss_limit", 0.0)) > 0:
                        paper_args = {
                            "profit_target": float(getattr(cfg, "paper_profit_target", 0.0)),
                            "loss_limit": float(getattr(cfg, "paper_loss_limit", 0.0)),
                            "mode": str(getattr(cfg, "paper_mode", "price")),
                        }

                    br = await asyncio.to_thread(
                        backtest_variant_confluence_gated,
                        c15,
                        c30,
                        c60,
                        fast60,
                        min_conf,
                        2 if (c30 and not c60) else 1,
                        ah,
                        0.2 if is_low_data and not strict else ofrac,
                        min_goal,
                        strict_micro,
                        gate_body_min,
                        float(gate_atr_lo),
                        float(gate_atr_hi),
                        conf_mode,
                        pre,
                        paper_args,
                        require_align30=bool(strict),
                    )
                else:
                    # Fallback to single-TF mode
                    brs = []
                    for tf, candles in asset_tfs.items():
                        min_ok = 20 if tf == 30 else 120
                        if len(candles) >= min_ok:
                            fast = _make_fast_rule_eval(candles, v.params, tf)
                            cper = per_asset_map.get(a, {})
                            use_gates = 60 in cper
                            if use_gates:
                                b = _gated_backtest_single(tf, candles, cper, fast, ah, ofrac, min_goal)
                            else:
                                b = await asyncio.to_thread(
                                    backtest_variant,
                                    candles,
                                    fast,
                                    tf,
                                    1,
                                    ah,
                                    ofrac,
                                    min_goal,
                                )
                            brs.append(b)
                    if not brs:
                        return a, None
                    wins = sum(b.wins for b in brs)
                    trades = sum(b.total_trades for b in brs)
                    hrs: Dict[int, Tuple[int, int]] = {}
                    for b in brs:
                        for hr, (w, t) in b.ist_hour_stats.items():
                            W, T = hrs.get(hr, (0, 0))
                            hrs[hr] = (W + w, T + t)
                    from types import SimpleNamespace
                    br = SimpleNamespace(total_trades=trades, wins=wins, ist_hour_stats=hrs)
                return a, br

        tasks = [asyncio.create_task(eval_one_asset(a)) for a in per_asset_map.keys()]
        results_asset = await asyncio.gather(*tasks)
        for a, br in results_asset:
            if not br or br.total_trades == 0:
                continue
            total_trades += br.total_trades
            total_wins += br.wins
            pa = per_asset.get(a, {"wins": 0, "trades": 0})
            pa["wins"] += br.wins
            pa["trades"] += br.total_trades
            per_asset[a] = pa
            for hr, (w, t) in br.ist_hour_stats.items():
                W, T = all_hours.get(hr, (0, 0))
                all_hours[hr] = (W + w, T + t)

        # Per-asset validation (strict95): WR≥0.80 and trades≥15
        if strict:
            per_asset = {a: s for a, s in per_asset.items() if s.get("trades", 0) >= max(10, int(getattr(cfg, 'asset_min_trades', 15))) and (s.get("wins", 0) / max(1, s.get("trades", 0))) >= max(0.80, float(getattr(cfg, 'asset_min_wr', 0.75)))}

        # Cross-asset dominance check (strict95): any single asset >45% of trades → log
        dominance_asset = None
        dominance_ratio = 0.0
        if strict:
            Ttot = sum(s.get("trades", 0) for s in per_asset.values()) or 0
            if Ttot > 0:
                amax = max(per_asset.items(), key=lambda kv: kv[1].get("trades", 0))
                dominance_asset = amax[0]
                dominance_ratio = (amax[1].get("trades", 0) / Ttot) if Ttot else 0.0
                if dominance_ratio > 0.45:
                    runlog.append_jsonl("cross_asset_dominance.jsonl", {"variant": v.name, "asset": dominance_asset, "ratio": round(dominance_ratio, 3), "total_trades": Ttot})

        acc = (total_wins / total_trades) if total_trades > 0 else 0.0
        return {
            "variant": v.name,
            "params": v.params,
            "accuracy": round(acc, 4),
            "total_trades": int(total_trades),
            "wins": int(total_wins),
            "ist_hour_stats": all_hours,
            "per_asset": per_asset,
            "dominance": {"asset": dominance_asset, "ratio": round(dominance_ratio, 3) if dominance_ratio else 0.0} if strict else None,
        }

    # Evaluate all variants in parallel
    # Sequential per-variant with progress (reduces peak memory; clearer progress)
    results: List[dict] = []
    for idx, v in enumerate(variants, start=1):
        est_assets = len(set(a for a in per_asset_map.keys()))
        print(f"[S12] Backtesting variant {v.name} ({idx}/{len(variants)}) on ~{est_assets} assets...")
        rv = await _eval_variant(v)
        results.append(rv)

    # 4b) Hour selection: manual override or auto-optimization
    async def _apply_hours(r: dict) -> dict:
        # Manual hours take precedence when enabled
        if getattr(cfg, "manual_ist_hours", False):
            sel = set(int(h) for h in (getattr(cfg, "manual_selected_hours", set()) or set()))
            if sel:
                print(f"[S12] Manual IST hours enabled; using hours={sorted(list(sel))} for variant {r.get('variant')}")
                v = RuleVariant(name=r["variant"], params=r["params"])
                p2 = dict(v.params)
                p2["allowed_hours"] = sorted(list(sel))
                v2 = RuleVariant(name=f"{v.name}-hrs", params=p2)
                rr2 = await _eval_variant(v2)
                runlog.append_jsonl("hour_selection.jsonl", {
                    "mode": "manual",
                    "variant": r["variant"],
                    "selected_hours": sorted(list(sel)),
                    "before": {"acc": r.get("accuracy", 0.0), "trades": r.get("total_trades", 0)},
                    "after": {"acc": rr2.get("accuracy", 0.0), "trades": rr2.get("total_trades", 0)},
                })
                return rr2
            else:
                print("[S12] Manual IST hours enabled but manual_selected_hours is empty; skipping hour filter.")
                runlog.append_jsonl("hour_selection.jsonl", {"mode": "manual", "variant": r.get("variant"), "selected_hours": []})
                return r

        # Auto-optimization (legacy greedy)
        stats: Dict[int, Tuple[int, int]] = r.get("ist_hour_stats", {})
        if not stats:
            return r
        hours_scored = []
        for hr, (w, t) in stats.items():
            if t <= 0:
                continue
            wr = w / t
            hours_scored.append((hr, wr, t))
        if not hours_scored:
            return r
        hours_scored.sort(key=lambda x: (x[1], x[2]), reverse=True)
        sel: set[int] = set()
        tw = 0
        tt = 0
        min_goal_hours = int(getattr(cfg, "strict95_min_trades", cfg.min_trades) if getattr(cfg, "strict95_mode", False) else cfg.min_trades)
        max_hours = int(getattr(cfg, "strict95_max_ist_hours", 2) if getattr(cfg, "strict95_mode", False) else 24)
        for hr, wr, t in hours_scored:
            if len(sel) >= max_hours:
                break
            sel.add(hr)
            tw += next((w for h, (w, tr) in stats.items() if h == hr), 0)
            tt += t
            if tt >= min_goal_hours:
                break
        if not sel:
            return r
        v = RuleVariant(name=r["variant"], params=r["params"])
        p2 = dict(v.params)
        p2["allowed_hours"] = sorted(list(sel))
        v2 = RuleVariant(name=f"{v.name}-hrs", params=p2)
        rr2 = await _eval_variant(v2)
        if rr2.get("accuracy", 0.0) > r.get("accuracy", 0.0) and rr2.get("total_trades", 0) >= cfg.min_trades:
            runlog.append_jsonl("hour_selection.jsonl", {
                "mode": "auto",
                "variant": r["variant"],
                "selected_hours": sorted(list(sel)),
                "before": {"acc": r.get("accuracy", 0.0), "trades": r.get("total_trades", 0)},
                "after": {"acc": rr2.get("accuracy", 0.0), "trades": rr2.get("total_trades", 0)},
            })
            return rr2
        return r

    # Skip hour optimization when manual mode / live-override / fast-mode is active
    if getattr(cfg, "manual_ist_hours", False) or getattr(cfg, "live_hour_override", False) or getattr(cfg, "fast_mode", False):
        print("[S12] Hour optimization skipped (manual/live-override/fast-mode).")
    else:
        results = [await _apply_hours(r) for r in results]

    # Fast exit: skip heavy phases in fast/live-override modes and display results immediately
    if getattr(cfg, "fast_mode", False) or getattr(cfg, "live_hour_override", False):
        print("[S12] Fast path: skipping trade boosting/refinement/broadening; showing results.")
        results_sorted = sorted(results, key=lambda r: (r["accuracy"], r["total_trades"]), reverse=True)
        display = results_sorted
        table = []
        for r in display:
            winrate_pct = round(100.0 * r["accuracy"], 1)
            wins = int(r.get("wins", 0))
            trades = int(r.get("total_trades", 0))
            hours = ",".join(str(h) for h in sorted(r.get("ist_hour_stats", {}).keys()))
            table.append([r["variant"], f"{winrate_pct}%", f"{wins}/{trades}", trades, hours])
        headers = ["Variant", "WinRate%", "Wins/Trades", "Trades", "IST Hours"]
        print(tabulate(table, headers=headers, tablefmt="github"))
        if cfg.show_per_asset_breakdown:
            print("\n[S12] Per-asset breakdown (filtered to WR≥asset_min_wr and trades≥asset_min_trades):")
            rows = []
            for r in display:
                for a, pv in (r.get("per_asset", {}) or {}).items():
                    t = int(pv.get("trades", 0)); w = int(pv.get("wins", 0))
                    wr = (w / t) if t > 0 else 0.0
                    if t >= cfg.asset_min_trades and wr >= max(0.80, cfg.asset_min_wr):
                        rows.append([r["variant"], a, f"{round(100*wr,1)}%", f"{w}/{t}"])
            if rows:
                print(tabulate(rows, headers=["Variant", "Asset", "WR%", "Wins/Trades"], tablefmt="github"))
        # Hour analysis across displayed variants
        if display:
            agg: Dict[int, Tuple[int, int]] = {}
            for r in display:
                hrs = r.get("ist_hour_stats", {}) or {}
                for hr, (w, t) in hrs.items():
                    aw, at = agg.get(hr, (0, 0))
                    agg[hr] = (aw + int(w), at + int(t))
            thresh = int(getattr(cfg, 'hour_analysis_min_trades', 20))
            topk = int(getattr(cfg, 'hour_analysis_top_k', 3))
            rows2 = []
            for hr, (w, t) in agg.items():
                if t >= thresh:
                    wr = (w / t) if t > 0 else 0.0
                    rows2.append([hr, f"{round(100*wr,1)}%", f"{w}/{t}"])
            rows2.sort(key=lambda x: float(x[1].strip('%')), reverse=True)
            if rows2:
                print("\n[S12] Hour-by-hour aggregate (min trades per hour =", thresh, "):")
                print(tabulate(rows2[:topk], headers=["IST Hour", "WR%", "Wins/Trades"], tablefmt="github"))
        return display

    # 4c) Trade-count boosting iteration (loosen first to increase signals, then refine)
    DISPLAY_MIN_ACC = 0.80
    BOOST_MIN_TRADES = cfg.min_trades

    async def _loosen_for_trades(r: dict, step: int = 1) -> dict:
        """Try to increase trade count by loosening parameters slightly.
        Low-data mode: if 15/30 missing heavily, also relax min_conf slightly only for discovery.
        """
        v = RuleVariant(name=r["variant"], params=r["params"])
        p = dict(v.params)
        # Loosen body and EMA distance a bit
        p["min_body_ratio"] = max(0.15, round(p.get("min_body_ratio", 0.25) - 0.05 * step, 2))
        p["min_ema_dist"] = max(0.0, round(p.get("min_ema_dist", 0.0) - 0.01 * step, 3))
        # Loosen %R zones
        p["wpr_upper_in"] = -min(40, abs(int(p.get("wpr_upper_in", -20))) + 5 * step)
        p["wpr_upper_out"] = -min(95, abs(int(p.get("wpr_upper_out", -80))) + 5 * step)
        p["wpr_lower_in"] = -min(95, abs(int(p.get("wpr_lower_in", -80))) + 5 * step)
        p["wpr_lower_out"] = -min(40, abs(int(p.get("wpr_lower_out", -20))) + 5 * step)
        v2 = RuleVariant(name=f"{v.name}-wide{step}", params=p)
        rr = await _eval_variant(v2)
        return rr

    boosted: List[dict] = []
    for r in results:
        rr = r
        if rr["total_trades"] < BOOST_MIN_TRADES and cfg.timeframes_s == [60]:
            # up to 2 boosting steps
            for s in (1, 2):
                rr2 = await _loosen_for_trades(rr, step=s)
                if rr2["total_trades"] > rr["total_trades"]:
                    rr = rr2
                if rr["total_trades"] >= BOOST_MIN_TRADES:
                    break
        boosted.append(rr)

    # 4c) Accuracy-focused refinement (data-driven failure analysis) after boosting
    from .learner import analyze_failures, refine_variant_by_analysis

    async def _collect_trade_log(v: RuleVariant) -> list[dict]:
        """Collect per-trade predicate context for 60s only to drive refinement."""
        logs: list[dict] = []
        # Allowed hours toggle same as eval
        ah = None if (getattr(cfg, "diagnostic_mode", False) and getattr(cfg, "diag_disable_hours", False)) else cfg.ist_hours
        try:
            from .patterns import build_feature_states_60s, label_outcomes_60s
        except Exception:
            return logs
        for a, tfs in per_asset_map.items():
            c60 = tfs.get(60, [])
            if not c60 or len(c60) < 120:
                continue
            states = build_feature_states_60s(c60)
            if not isinstance(states, dict):
                states = {}
            call_win, put_win = label_outcomes_60s(c60, expiry_steps=1)
            call_win = list(call_win or [])
            put_win = list(put_win or [])
            # Iterate bars; evaluate signal at i using candles[:i+1]
            for i in range(len(c60) - 1):
                # IST hour filter
                if ah:
                    ts = _ts(c60[i])
                    try:
                        import datetime as _dt, zoneinfo as _zi
                        hr = _dt.datetime.utcfromtimestamp(ts).replace(tzinfo=_zi.ZoneInfo("UTC")).astimezone(_zi.ZoneInfo("Asia/Kolkata")).hour if ts else None
                    except Exception:
                        hr = None
                    if hr is None or hr not in ah:
                        continue
                has_sig, direction = v.evaluate(c60[: i + 1])
                if not has_sig:
                    continue
                if i >= len(call_win) or i >= len(put_win):
                    continue
                won = call_win[i] if direction == "call" else put_win[i]
                try:
                    active_preds = [name for name, arr in states.items() if isinstance(arr, list) and i < len(arr) and arr[i]]
                except Exception:
                    active_preds = []
                logs.append({"asset": a, "i": i, "predicates": active_preds, "direction": direction, "won": bool(won)})
        return logs

    MAX_REFINEMENT_ROUNDS = getattr(cfg, "strict95_max_refinement_iterations", 5) if getattr(cfg, "strict95_mode", False) else 10
    refined: List[dict] = []
    refined.extend([r for r in boosted if r["accuracy"] >= DISPLAY_MIN_ACC])
    TARGET_GOOD = 3
    if not (cfg.early_stop_refinement and len(refined) >= TARGET_GOOD):
        for r in boosted:
            if r["accuracy"] >= DISPLAY_MIN_ACC:
                continue
            v = RuleVariant(name=r["variant"], params=r["params"])
            best = r
            for round_idx in range(MAX_REFINEMENT_ROUNDS):
                trade_log = await _collect_trade_log(v)
                analysis = analyze_failures(trade_log)
                v = refine_variant_by_analysis(v, analysis, round_idx=round_idx)
                rr = await _eval_variant(v)
                # Log step
                runlog.append_jsonl("refinement_steps.jsonl", {
                    "variant": r["variant"],
                    "round": round_idx + 1,
                    "analysis": analysis,
                    "params": v.params,
                    "result": {"accuracy": rr["accuracy"], "total_trades": rr["total_trades"]},
                })
                if rr["accuracy"] > best["accuracy"]:
                    best = rr
                # Stop criteria per mode
                if getattr(cfg, "strict95_mode", False):
                    target = float(getattr(cfg, "strict95_target_wr", 0.95))
                    if rr["accuracy"] >= target and rr["total_trades"] >= int(getattr(cfg, "strict95_min_trades", 50)):
                        refined.append(rr)
                        break
                else:
                    if rr["accuracy"] >= DISPLAY_MIN_ACC and rr["total_trades"] >= cfg.min_trades:
                        refined.append(rr)
                        break
            else:
                refined.append(best)
            if cfg.early_stop_refinement and len([x for x in refined if x["accuracy"] >= DISPLAY_MIN_ACC]) >= TARGET_GOOD:
                break

    results = refined

    # 4d) If still no >=80%, broaden search and re-evaluate a fresh batch
    if all(r["accuracy"] < DISPLAY_MIN_ACC for r in results):
        extra = generate_broader_variants(count=24 if cfg.timeframes_s == [60] else 12)
        extra_results: List[dict] = await asyncio.gather(*[asyncio.create_task(_eval_variant(v)) for v in extra])
        results += extra_results

    # 5) Diagnostics: show raw stats before strict filter if enabled
    results_sorted = sorted(results, key=lambda r: (r["accuracy"], r["total_trades"]), reverse=True)
    if cfg.diagnostic_mode and cfg.diag_show_all:
        print("\n[S12] Diagnostic: raw variant stats before strict filter")
        for r in results_sorted:
            wr = r.get("accuracy", 0.0)
            trades = int(r.get("total_trades", 0))
            reason = []
            if wr < 0.80:
                reason.append("WR < 80%")
            if trades < cfg.min_trades:
                reason.append(f"trades < {cfg.min_trades}")
            status = "pass" if not reason else f"filtered (needs >={cfg.min_trades} trades and >=80% WR)"
            print(f"[S12] Variant {r['variant']} raw stats: acc={wr:.2f} trades={trades} -> {status}")

    # 5b) Strict filter disabled: show all results sorted
    display = results_sorted
    table = []
    for r in display:
        winrate_pct = round(100.0 * r["accuracy"], 1)
        wins = int(r.get("wins", 0))
        trades = int(r.get("total_trades", 0))
        hours = ",".join(str(h) for h in sorted(r.get("ist_hour_stats", {}).keys()))
        table.append([r["variant"], f"{winrate_pct}%", f"{wins}/{trades}", trades, hours])
    headers = ["Variant", "WinRate%", "Wins/Trades", "Trades", "IST Hours"]
    print(tabulate(table, headers=headers, tablefmt="github"))

    # Optional per-asset breakdown (still filtered by asset_min_trades and asset_min_wr for readability)
    if cfg.show_per_asset_breakdown:
        print("\n[S12] Per-asset breakdown (filtered to WR≥asset_min_wr and trades≥asset_min_trades):")
        rows = []
        for r in display:
            per = r.get("per_asset", {})
            for a, pv in per.items():
                t = int(pv.get("trades", 0)); w = int(pv.get("wins", 0))
                wr = (w / t) if t > 0 else 0.0
                if t >= cfg.asset_min_trades and wr >= max(0.80, cfg.asset_min_wr):
                    rows.append([r["variant"], a, f"{round(100*wr,1)}%", f"{w}/{t}"])
        if rows:
            print(tabulate(rows, headers=["Variant", "Asset", "WR%", "Wins/Trades"], tablefmt="github"))

    # 6) Return results (strict filter disabled)
    return display

