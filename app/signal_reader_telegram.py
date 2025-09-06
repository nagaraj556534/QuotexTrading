import asyncio
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import AsyncIterator, Dict, List, Optional, Set

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore


# Telegram-style signal blocks sample (from histories.txt):
# WIN ✅
# 💳 USDBDT-OTC
# 🔥 M1
# ⌛ 23:10:00
# 🔽 put
#
# 🚦 Tend: Sell
# 📈 Forecast: 73.35%
# 💸 Payout: 82.0%
#
# We parse asset, timeframe (minutes), trade_time (HH:MM[:SS]), direction, trend, forecast%, payout%.


@dataclass
class TelegramSignal:
    asset: str
    direction: str  # "call" | "put"
    timeframe_min: Optional[int] = None
    trade_time: Optional[str] = None  # HH:MM[:SS]
    trend: Optional[str] = None  # Buy/Sell
    forecast_pct: Optional[float] = None
    payout_pct: Optional[float] = None
    raw_block: str = ""

    def key(self) -> str:
        return f"{self.asset}|{self.trade_time or ''}|{self.direction}"


_ASSET_RE = re.compile(r"^\s*[💳]?\s*([A-Z0-9\-_/]+)(?:\s*)$")
_ASSET_LINE_RE = re.compile(r"^\s*💳\s*([A-Z0-9\-_/]+)\s*$", re.IGNORECASE)
_TIMEFRAME_RE = re.compile(r"^\s*🔥\s*M(\d+)\s*$", re.IGNORECASE)
_TRADE_TIME_RE = re.compile(r"^\s*⌛\s*([0-2]?\d:\d{2}(?::\d{2})?)\s*$", re.IGNORECASE)
_DIRECTION_WORD_RE = re.compile(r"\b(call|put)\b", re.IGNORECASE)
_TREND_RE = re.compile(r"^\s*🚦\s*T(?:r|)end\s*:\s*(Buy|Sell)\s*$", re.IGNORECASE)
_FORECAST_RE = re.compile(r"^\s*📈\s*Forecast\s*:\s*([0-9]+(?:\.[0-9]+)?)%\s*$", re.IGNORECASE)
_PAYOUT_RE = re.compile(r"^\s*💸\s*Payout\s*:\s*([0-9]+(?:\.[0-9]+)?)%\s*$", re.IGNORECASE)


def _normalize_direction_from_line(s: str) -> Optional[str]:
    s_low = s.lower()
    if "🔼" in s_low or " call" in s_low:
        return "call"
    if "🔽" in s_low or " put" in s_low:
        return "put"
    m = _DIRECTION_WORD_RE.search(s)
    if m:
        v = m.group(1).lower()
        return "call" if v == "call" else "put"
    return None


class TelegramSignalParser:
    """Stateful line-by-line parser that emits TelegramSignal when sufficient fields are captured."""

    def __init__(self) -> None:
        self._state: Dict[str, Optional[str]] = {}
        self._block_lines: List[str] = []

    def reset(self) -> None:
        self._state.clear()
        self._block_lines.clear()

    def feed_line(self, line: str) -> Optional[TelegramSignal]:
        s = line.strip()
        if not s:
            # blank lines delimit blocks; try emitting if complete
            sig = self._maybe_emit()
            if sig:
                return sig
            # else keep waiting
            return None

        # Accumulate raw block text
        self._block_lines.append(s)

        # Recognize a new block start by an Asset line or WIN line
        if s.upper().startswith("WIN") or s.startswith("💳"):
            # If prior block was complete, emit before starting fresh
            prev = self._maybe_emit()
            # Reset for new block if asset line; WIN just marks outcome
            if s.startswith("💳"):
                self._state.clear()
                self._block_lines[:] = [s]
            if prev:
                return prev

        # Asset
        m = _ASSET_LINE_RE.match(s)
        if m:
            self._state["asset"] = m.group(1).strip()

        # Timeframe
        m = _TIMEFRAME_RE.match(s)
        if m:
            self._state["timeframe_min"] = m.group(1)

        # Trade time
        m = _TRADE_TIME_RE.match(s)
        if m:
            tt = m.group(1).strip().strip('"')
            self._state["trade_time"] = tt

        # Direction (arrow or word)
        dire = _normalize_direction_from_line(s)
        if dire:
            self._state["direction"] = dire

        # Trend
        m = _TREND_RE.match(s)
        if m:
            self._state["trend"] = m.group(1).title()

        # Forecast
        m = _FORECAST_RE.match(s)
        if m:
            try:
                self._state["forecast_pct"] = str(float(m.group(1)))
            except Exception:
                pass

        # Payout
        m = _PAYOUT_RE.match(s)
        if m:
            try:
                self._state["payout_pct"] = str(float(m.group(1)))
            except Exception:
                pass

        # Emit ASAP when core fields are present
        return self._maybe_emit(partial_ok=True)

    def _maybe_emit(self, partial_ok: bool = False) -> Optional[TelegramSignal]:
        asset = self._state.get("asset")
        direction = self._state.get("direction")
        trade_time = self._state.get("trade_time")
        if not asset or not direction:
            return None
        # For partial_ok, allow missing trade_time/timeframe; emit when direction+asset set
        if not partial_ok and (asset and direction and trade_time is None):
            return None

        tf = self._state.get("timeframe_min")
        trend = self._state.get("trend")
        f = self._state.get("forecast_pct")
        p = self._state.get("payout_pct")
        raw = "\n".join(self._block_lines[-12:])
        sig = TelegramSignal(
            asset=asset,
            direction=direction,
            timeframe_min=int(tf) if tf else None,
            trade_time=trade_time,
            trend=trend,
            forecast_pct=float(f) if f is not None else None,
            payout_pct=float(p) if p is not None else None,
            raw_block=raw,
        )
        # After emitting once per block, clear direction to avoid duplicates
        self._state.pop("direction", None)
        return sig


async def tail_file(path: str) -> AsyncIterator[TelegramSignal]:
    """Tail a UTF-8 file and yield TelegramSignal as they are parsed.
    Tolerates emoji and partial lines; does not seek to end to allow initial backlog.
    """
    parser = TelegramSignalParser()
    seen: Set[str] = set()

    # Wait for file to appear
    for _ in range(200):
        if os.path.exists(path):
            break
        await asyncio.sleep(0.1)

    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            while True:
                line = f.readline()
                if not line:
                    await asyncio.sleep(0.1)
                    continue
                sig = parser.feed_line(line)
                if sig:
                    k = sig.key()
                    if k in seen:
                        continue
                    seen.add(k)
                    yield sig
    except asyncio.CancelledError:
        raise


def parse_file_once(path: str) -> List[TelegramSignal]:
    """Parse entire file once (no tail) and return all signals, de-duplicated by (asset,time,direction)."""
    parser = TelegramSignalParser()
    seen: Set[str] = set()
    out: List[TelegramSignal] = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            sig = parser.feed_line(line)
            if sig:
                k = sig.key()
                if k in seen:
                    continue
                seen.add(k)
                out.append(sig)
    return out


def ist_epoch_for_trade_time(hhmm: str, ref_ts: Optional[float] = None) -> int:
    """Convert HH:MM[:SS] assumed IST clock to epoch seconds.
    If time already passed today, schedule for next day.
    """
    try:
        parts = hhmm.split(":")
        h, m = int(parts[0]), int(parts[1])
        s = int(parts[2]) if len(parts) > 2 else 0
        tz = ZoneInfo("Asia/Kolkata") if ZoneInfo else timezone.utc
        now = datetime.now(tz) if ref_ts is None else datetime.fromtimestamp(ref_ts, tz)
        trade_dt = now.replace(hour=h, minute=m, second=s, microsecond=0)
        if trade_dt <= now:
            # next day
            trade_dt = trade_dt.replace(day=trade_dt.day) + timedelta(days=1)  # type: ignore[name-defined]
        return int(trade_dt.timestamp())
    except Exception:
        return 0


if __name__ == "__main__":
    import argparse
    import json

    ap = argparse.ArgumentParser(description="Parse Telegram histories.txt blocks into structured signals")
    ap.add_argument("--file", default=os.path.join(os.path.dirname(__file__), "..", "histories.txt"))
    ap.add_argument("--tail", action="store_true", help="Tail the file and stream signals")
    args = ap.parse_args()

    path = os.path.abspath(args.file)
    if args.tail:
        async def _run():
            async for sig in tail_file(path):
                print(json.dumps(sig.__dict__, ensure_ascii=False))
        asyncio.run(_run())
    else:
        sigs = parse_file_once(path)
        for s in sigs:
            print(json.dumps(s.__dict__, ensure_ascii=False))

