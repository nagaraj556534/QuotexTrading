# Strategy 12 (Phase 1: Rule-Search Engine)

This module generates multiple rule-based variants (12.x) using EMA/PSAR/Williams %R and candle body filters, backtests them across assets and timeframes (15s/30s/60s), ranks by accuracy, and allows selection for paper-trading.

Core components:
- data.py: candle fetching helpers
- features.py: lightweight indicators
- rules.py: rule templates and evaluation
- backtest.py: backtester with IST hour bucketing
- search.py: simple parameter generator (base + random perturbations)
- pipeline.py: end-to-end run and ranking table output
- cli.py: interactive selection wrapper
- deploy.py: scanning for first signal on selected variant (paper/live hook)

Target:
- Accuracy >= 95% with >= 50 trades before promotion to paper, and sustained thereafter before live.

No external ML dependencies are required in Phase 1.

