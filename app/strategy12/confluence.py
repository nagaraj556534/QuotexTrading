from __future__ import annotations
from typing import Dict, Any, List, Tuple

# Multi-timeframe confluence helpers
# signals: dict {tf_seconds: (has_signal(bool), direction(str))}
# Returns: (has_signal, direction)

def two_of_three_confluence(signals: Dict[int, Tuple[bool, str]]) -> Tuple[bool, str]:
    acts: List[Tuple[int, str]] = []
    for _tf, (ok, dirn) in signals.items():
        if ok:
            acts.append((_tf, dirn))
    if len(acts) < 2:
        return False, "call"
    up = sum(1 for _, d in acts if d == "call")
    dn = sum(1 for _, d in acts if d == "put")
    if up >= 2:
        return True, "call"
    if dn >= 2:
        return True, "put"
    return False, "call"


def three_of_three_confluence(signals: Dict[int, Tuple[bool, str]]) -> Tuple[bool, str]:
    acts: List[str] = []
    for _tf, (ok, dirn) in signals.items():
        if ok:
            acts.append(dirn)
    if len(acts) < 3:
        return False, "call"
    if all(d == "call" for d in acts):
        return True, "call"
    if all(d == "put" for d in acts):
        return True, "put"
    return False, "call"
