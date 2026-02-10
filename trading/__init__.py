"""
Meta Engine — Automated Options Trading Module
================================================
Executes top 3 PUT and CALL picks via Alpaca paper trading.
"""

from .executor import execute_trades, check_and_manage_positions

__all__ = ["execute_trades", "check_and_manage_positions"]
