import os
import csv
import asyncio
import time
from typing import Optional
from dataclasses import dataclass

# Reuse existing parser + scheduling helpers (support direct + package import)
try:
    from signal_reader_telegram import (
        TelegramSignalParser,
        schedule_signal,
        ScheduledTelegramSignal,
    )
except Exception:
    try:
        from .signal_reader_telegram import (  # type: ignore
            TelegramSignalParser,
            schedule_signal,
            ScheduledTelegramSignal,
        )
    except Exception as _e:  # pragma: no cover
        raise ImportError(f"Cannot import signal_reader_telegram: {_e}")

# Lazy import Telethon only when running
try:
    from telethon import TelegramClient, events
except Exception:  # pragma: no cover - telethon optional at import time
    TelegramClient = None  # type: ignore
    events = None  # type: ignore


@dataclass
class S19Config:
    api_id: int
    api_hash: str
    phone: str
    group: str  # username ("mygroup") OR numeric id ("-1001234567890")
    session_name: str = "s19_session"
    # optional future filter (broker lookup needed)
    min_payout: float = 0.0
    # forecast pct filter
    min_forecast: float = 0.0
    # seconds before trade_time to send order
    lead_s: int = 5
    log_csv: str = os.path.join(
        os.path.dirname(__file__), "..", "strategy19_signals.csv"
    )
    # future toggle for actual trade execution
    trade: bool = False
    default_expiry_min: int = 1
    cooldown_same_asset_s: int = 90
    # if already passed by <= this, still attempt
    allow_past_grace_s: int = 8
    tz_offset_min: int = 330  # default IST (+330). Set -180 for UTC-3.


_HEADER = [
    "ts_local",
    "asset",
    "direction",
    "timeframe_min",
    "trade_time",
    "forecast_pct",
    "payout_pct",
    "trade_epoch",
    "seconds_until_entry",
    "ignored",
    "reason",
    "raw",
]


def _ensure_log(path: str):
    try:
        new = not os.path.exists(path)
        if new:
            os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", newline="", encoding="utf-8") as f:
            if new:
                csv.writer(f).writerow(_HEADER)
    except Exception:
        pass


def _log_signal(path: str, sig: ScheduledTelegramSignal):
    try:
        _ensure_log(path)
        with open(path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                time.strftime("%Y-%m-%d %H:%M:%S"),
                sig.asset,
                sig.direction,
                sig.timeframe_min or "",
                sig.trade_time or "",
                sig.forecast_pct or "",
                sig.payout_pct or "",
                sig.trade_epoch or "",
                sig.seconds_until_entry(),
                int(sig.ignored),
                sig.reason,
                sig.raw_block.replace("\n", " | ")[:200],
            ])
    except Exception:
        pass


class Strategy19Follower:
    def __init__(self, cfg: S19Config, execute_cb=None):
        self.cfg = cfg
        self.parser = TelegramSignalParser()
        self.seen_keys: set[str] = set()
        self.cooldowns: dict[str, float] = {}
        self.execute_cb = execute_cb  # async function(sig) -> None
        # Fallback type if telethon missing
        self.client: Optional[object] = None

    def _filter(self, sig: ScheduledTelegramSignal) -> ScheduledTelegramSignal:
        # Forecast filter
        if (
            sig.forecast_pct is not None
            and sig.forecast_pct < self.cfg.min_forecast
        ):
            sig.ignored = True
            sig.reason = f"forecast<{self.cfg.min_forecast}"
            return sig
        # Cooldown
        now = time.time()
        cd_until = self.cooldowns.get(sig.asset, 0.0)
        if now < cd_until:
            sig.ignored = True
            sig.reason = "asset_cooldown"
            return sig
        # Late arrival check
        if (
            sig.trade_epoch is not None
            and (sig.trade_epoch - sig.entry_lead_s) < now
        ):
            # Already past intended entry moment
            if (
                now - (sig.trade_epoch - sig.entry_lead_s)
                > self.cfg.allow_past_grace_s
            ):
                sig.ignored = True
                sig.reason = "missed_entry"
                return sig
        sig.reason = "ok"
        return sig

    async def _handle_signal(self, sig: ScheduledTelegramSignal):
        k = sig.key()
        if k in self.seen_keys:
            return
        self.seen_keys.add(k)
        sig = self._filter(sig)
        _log_signal(self.cfg.log_csv, sig)
        # Print summary
        eta = sig.seconds_until_entry()
        print(
            f"[S19] {sig.asset} {sig.direction} tt={sig.trade_time} "
            f"eta={eta}s fore={sig.forecast_pct} pay={sig.payout_pct} "
            f"ignored={sig.ignored} reason={sig.reason}"
        )
        # Execute via injected callback
        if self.cfg.trade and not sig.ignored and self.execute_cb:
            try:
                await self.execute_cb(sig)
            except Exception as e:  # pragma: no cover
                print(f"[S19][EXEC_ERR] {e}")
        # Set cooldown after scheduling (even if not traded) to prevent spam
        self.cooldowns[sig.asset] = (
            time.time() + self.cfg.cooldown_same_asset_s
        )

    async def start(self):
        if TelegramClient is None:
            raise RuntimeError(
                "Telethon not installed. Run: pip install telethon"
            )
        self.client = TelegramClient(
            self.cfg.session_name, self.cfg.api_id, self.cfg.api_hash
        )
        await self.client.start(phone=self.cfg.phone)

        group = self.cfg.group
        
        @self.client.on(events.NewMessage(chats=group))  # type: ignore
        async def _on_msg(event):  # noqa: N802
            try:
                txt = event.raw_text or ""
                for line in txt.splitlines():
                    sig = self.parser.feed_line(line + "\n")
                    if sig:
                        sched = schedule_signal(
                            sig,
                            default_expiry_min=self.cfg.default_expiry_min,
                            lead_s=self.cfg.lead_s,
                            tz_offset_min=self.cfg.tz_offset_min,
                        )
                        await self._handle_signal(sched)
            except Exception as e:  # pragma: no cover
                print(f"[S19][ERR] handler: {e}")

        # Fresh start listening banner
        try:
            from colorama import Fore, Style  # type: ignore
        except Exception:  # pragma: no cover
            class _No:
                RED = YELLOW = MAGENTA = CYAN = GREEN = RESET = BLUE = WHITE = RESET_ALL = ""
            Fore = Style = _No()  # type: ignore
        print(
            Fore.CYAN
            + f"[S19] Listening group={group} session={self.cfg.session_name}"
            + Style.RESET_ALL
        )
        print(Fore.CYAN + "[S19] (Ctrl+C to stop)" + Style.RESET_ALL)
        await self.client.run_until_disconnected()


def load_cfg_from_env() -> S19Config:
    try:
        api_id = int(os.environ.get("TELEGRAM_API_ID", "0"))
    except Exception:
        api_id = 0
    session_name = os.environ.get(
        "TELEGRAM_SESSION", "s19_session"
    ).strip() or "s19_session"
    return S19Config(
        api_id=api_id,
        api_hash=os.environ.get("TELEGRAM_API_HASH", ""),
        phone=os.environ.get("TELEGRAM_PHONE", ""),
        group=os.environ.get("TELEGRAM_GROUP", ""),
        session_name=session_name,
        min_payout=float(os.environ.get("S19_MIN_PAYOUT", "0") or 0),
        min_forecast=float(os.environ.get("S19_MIN_FORECAST", "0") or 0),
        lead_s=int(os.environ.get("S19_LEAD_S", "5") or 5),
        trade=os.environ.get("S19_TRADE", "0").lower() in ("1", "true", "yes"),
    )


async def main():
    cfg = load_cfg_from_env()
    if not (cfg.api_id and cfg.api_hash and cfg.phone and cfg.group):
        print(
            "Missing TELEGRAM_* env vars. Need TELEGRAM_API_ID, "
            "TELEGRAM_API_HASH, TELEGRAM_PHONE, TELEGRAM_GROUP"
        )
        return
    follower = Strategy19Follower(cfg)
    await follower.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[S19] Stopped by user.")
