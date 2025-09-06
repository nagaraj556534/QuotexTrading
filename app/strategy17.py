import asyncio
import csv
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, Optional

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None  # type: ignore


# Strategy 17: External Signal Watcher (signalbot.exe)
# - Launches the companion executable and streams its console output
# - Parses human-readable signal blocks (Currency/Trade Time/Expiry/Direction[, Confidence])
# - Saves every parsed signal to CSV log with IST timestamp
# - Designed to be used in capture-only mode initially


@dataclass
class S17Config:
    exe_path: Optional[str] = None
    log_csv_path: Optional[str] = None


def _resolve_default_exe_path() -> Optional[str]:
    base = os.path.dirname(__file__)
    # Prefer underscore directory (as in repository); fall back to space variant
    candidates = [
        os.path.join(base, "New Auto Trading Bot", "signalbot.exe"),
        os.path.join(base, "New_Auto_Trading_Bot", "signalbot.exe"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return None


def _ensure_log_header(path: str) -> None:
    try:
        new = not os.path.exists(path)
        if new:
            os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", newline="", encoding="utf-8") as f:
            if new:
                csv.writer(f).writerow([
                    "ts_local", "ts_ist", "currency", "trade_time", "expiry", "direction", "confidence"
                ])
    except Exception:
        pass


_SIG_START_RE = re.compile(r"SIGNAL\s+EXTRACTED", re.IGNORECASE)
_FIELD_RE = {
    "currency": re.compile(r"^\s*Currency\s*:\s*([^\s]+)\s*$", re.IGNORECASE),
    "trade_time": re.compile(r"^\s*Trade\s*Time\s*:\s*([0-2]?\d:\d{2})\s*$", re.IGNORECASE),
    "expiry": re.compile(r"^\s*Expiry\s*:\s*([A-Za-z0-9]+)\s*$", re.IGNORECASE),
    "direction": re.compile(r"^\s*Direction\s*:\s*(call|put|buy|sell)\s*$", re.IGNORECASE),
    "confidence": re.compile(r"^\s*Confidence\s*:\s*([0-9]+(?:\.[0-9]+)?)%?\s*$", re.IGNORECASE),
}


def _normalize_direction(val: str) -> str:
    v = val.strip().lower()
    return {"buy": "call", "sell": "put"}.get(v, v)


def _parse_line(state: Dict[str, str], line: str) -> Optional[Dict[str, str]]:
    """Update state with any matched field; return signal when complete.
    We emit once direction is seen and currency is available.
    """
    if _SIG_START_RE.search(line):
        state.clear()
        return None

    for key, rx in _FIELD_RE.items():
        m = rx.search(line)
        if m:
            val = m.group(1).strip()
            if key == "direction":
                val = _normalize_direction(val)
            state[key] = val
            break

    if "direction" in state and "currency" in state:
        # Emit once per block; copy and reset direction so we don't emit twice
        out = dict(state)
        state.pop("direction", None)
        return out
    return None


async def stream_signalbot(cfg: S17Config) -> AsyncIterator[Dict[str, str]]:
    exe = cfg.exe_path or _resolve_default_exe_path()
    if not exe or not os.path.exists(exe):
        raise FileNotFoundError(f"signalbot.exe not found. Tried: {exe or '(auto)'}")

    # Start process; capture stdout/stderr
    # Try popen with pipes; if it exits immediately (GUI app), fallback to spawn without pipes
    # Launch via PowerShell with UTF-8 console to avoid UnicodeEncodeError (\u2705)
    ps_cmd = (
        "[Console]::OutputEncoding=[System.Text.UTF8Encoding]::new(); "
        "chcp 65001 > $null; .\\signalbot.exe"
    )
    env = os.environ.copy()
    # Force a tolerant stdio encoding in child to avoid emoji crash
    env["PYTHONIOENCODING"] = "cp1252:replace"
    env["PYTHONUTF8"] = "1"
    env["PYTHONLEGACYWINDOWSSTDIO"] = "1"
    try:
        proc = await asyncio.create_subprocess_exec(
            "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=os.path.dirname(exe),
            env=env,
        )
    except Exception as e:
        raise RuntimeError(f"Failed to launch signalbot.exe: {e}")

    async def _consume_stderr(stream):
        try:
            while True:
                if stream.at_eof():
                    break
                chunk = await stream.readline()
                if not chunk:
                    break
                line = chunk.decode("utf-8", errors="ignore").strip()
                if line:
                    print(f"[S17][ERR] {line}")
        except Exception as e:
            print(f"[S17][ERR] stderr reader error: {e}")

    # fire-and-forget stderr consumer
    if proc.stderr is not None:
        asyncio.create_task(_consume_stderr(proc.stderr))

    state: Dict[str, str] = {}
    stream = proc.stdout
    assert stream is not None

    # If no output in a short grace period, fallback to running via PowerShell wrapper
    first_line_timeout_s = 2.5
    got_any_line = False

    try:
        start_ts = asyncio.get_event_loop().time()
        while True:
            if stream.at_eof():
                break
            raw = await stream.readline()
            if not raw:
                if not got_any_line and (asyncio.get_event_loop().time() - start_ts) > first_line_timeout_s:
                    # Fallback: restart through PowerShell to force console-bound exe to stream
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    pwsh = ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ".\\signalbot.exe"]
                    proc = await asyncio.create_subprocess_exec(
                        *pwsh,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                        cwd=os.path.dirname(exe),
                    )
                    stream = proc.stdout
                    start_ts = asyncio.get_event_loop().time()
                    continue
                await asyncio.sleep(0.05)
                continue
            got_any_line = True
            # Replace problematic emojis to avoid Windows console UnicodeEncodeError
            line = raw.decode("utf-8", errors="ignore").replace("\u2705", "[OK]").strip()
            sig = _parse_line(state, line)
            if sig:
                yield sig
    finally:
        try:
            if proc.returncode is None:
                proc.kill()
        except Exception:
            pass


def _ist_now_str() -> str:
    try:
        tz = ZoneInfo("Asia/Kolkata") if ZoneInfo else timezone.utc
        return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return datetime.now().isoformat(timespec="seconds")


def _log_signal_row(path: str, sig: Dict[str, str]) -> None:
    try:
        _ensure_log_header(path)
        with open(path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                datetime.now().isoformat(timespec="seconds"),
                _ist_now_str(),
                sig.get("currency", ""),
                sig.get("trade_time", ""),
                sig.get("expiry", ""),
                sig.get("direction", ""),
                sig.get("confidence", ""),
            ])
    except Exception:
        pass



async def stream_signalbot_file_tail(cfg: S17Config) -> AsyncIterator[Dict[str, str]]:
    exe = cfg.exe_path or _resolve_default_exe_path()
    if not exe or not os.path.exists(exe):
        raise FileNotFoundError(f"signalbot.exe not found. Tried: {exe or '(auto)'}")
    work = os.path.dirname(exe)
    out_path = os.path.join(work, "strategy17_signalbot_out.txt")
    err_path = os.path.join(work, "strategy17_signalbot_err.txt")
    # Rotate any old files
    try:
        for p in (out_path, err_path):
            if os.path.exists(p):
                try:
                    os.replace(p, p + ".old")
                except Exception:
                    pass
    except Exception:
        pass

    # Launch detached via PowerShell -> Start-Process with redirected output
    ps_cmd = (
        "$env:PYTHONIOENCODING='utf-8';"
        "$env:PYTHONLEGACYWINDOWSSTDIO='1';"
        "chcp 65001 > $null;"
        f"Start-Process -FilePath '.\\signalbot.exe' -WorkingDirectory '.' "
        f"-RedirectStandardOutput '{os.path.basename(out_path)}' "
        f"-RedirectStandardError '{os.path.basename(err_path)}' -WindowStyle Hidden"
    )
    try:
        _ = await asyncio.create_subprocess_exec(
            "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_cmd,
            cwd=work,
        )
    except Exception as e:
        raise RuntimeError(f"Failed to spawn signalbot via file-tail: {e}")

    # Tail the UTF-8 file
    # NB: tolerates partial lines and emoji by errors='ignore'
    state: Dict[str, str] = {}
    # Wait for file to appear
    for _ in range(50):
        if os.path.exists(out_path):
            break
        await asyncio.sleep(0.1)
    # Tail loop
    try:
        with open(out_path, "r", encoding="utf-8", errors="ignore") as f:
            # do not seek to end; we want initial lines too
            while True:
                line = f.readline()
                if not line:
                    await asyncio.sleep(0.1)
                    continue
                s = line.strip()
                if not s:
                    continue
                sig = _parse_line(state, s)
                if sig:
                    yield sig
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"[S17] Tail error: {e}")

async def run_capture(cfg: Optional[S17Config] = None) -> None:
    """Run signalbot watcher and append every parsed signal to CSV log.
    Press Ctrl+C to stop. This does not place trades; capture-only mode.
    """
    cfg = cfg or S17Config()
    csv_path = cfg.log_csv_path or os.path.join(os.path.dirname(__file__), "..", "strategy17_signals.csv")
    csv_path = os.path.abspath(csv_path)

    print(f"[S17] Log file: {csv_path}")
    exe = cfg.exe_path or _resolve_default_exe_path()
    print(f"[S17] Launching: {exe}")

    try:
        # First attempt: direct stdout streaming
        async for sig in stream_signalbot(S17Config(exe_path=exe, log_csv_path=csv_path)):
            cur = sig.get("currency", "?")
            dire = sig.get("direction", "?")
            tstr = sig.get("trade_time", "--:--")
            exp = sig.get("expiry", "")
            conf = sig.get("confidence", "")
            print(f"[S17] {cur} {dire} at {tstr} expiry={exp} conf={conf}")
            _log_signal_row(csv_path, sig)
    except Exception as e1:
        print(f"[S17] Direct stream failed ({e1}); switching to file-tail mode...")
        # Fallback: file tail mode (robust against console encoding issues)
        try:
            async for sig in stream_signalbot_file_tail(S17Config(exe_path=exe, log_csv_path=csv_path)):
                cur = sig.get("currency", "?")
                dire = sig.get("direction", "?")
                tstr = sig.get("trade_time", "--:--")
                exp = sig.get("expiry", "")
                conf = sig.get("confidence", "")
                print(f"[S17] {cur} {dire} at {tstr} expiry={exp} conf={conf}")
                _log_signal_row(csv_path, sig)
        except Exception as e2:
            print(f"[S17] File-tail mode failed: {e2}")
    except asyncio.CancelledError:
        print("[S17] Stopped.")
    except KeyboardInterrupt:
        print("[S17] Interrupted by user.")


__all__ = [
    "S17Config",
    "stream_signalbot",
    "run_capture",
]

