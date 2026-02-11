"""
Meta Engine — Automated Options Trading Executor
==================================================
Trades the top 3 PUT and CALL picks from each scan session via
Alpaca paper trading.

Flow:
  1. Receive top picks from meta_engine pipeline
  2. For each pick → search Alpaca for best options contract
  3. Place buy order for 5 contracts
  4. Monitor open positions → take-profit / stop-loss / time-stop

Smart contract selection:
  • Strike: 3-8 % OTM for leverage (targeting 3-10x return)
  • Expiry: 7-21 days out (sweet-spot gamma/theta trade-off)
  • Liquidity: prefer highest open-interest contracts
"""

import os
import json
import uuid
import logging
import requests
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .trade_db import TradeDB

logger = logging.getLogger("meta_engine.trading")

# ═══════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════
CONTRACTS_PER_TRADE = 5
STRIKE_OTM_PCT = 0.05          # 5 % out-of-the-money
MIN_DAYS_TO_EXPIRY = 5
MAX_DAYS_TO_EXPIRY = 21
TAKE_PROFIT_MULT = 3.0         # Sell when premium hits 3× entry
STOP_LOSS_PCT = 0.50           # Sell when premium drops 50 %
TOP_N_TRADES = 3               # Trade top 3 from each engine


# ═══════════════════════════════════════════════════════
# Alpaca REST client (direct HTTP — avoids SDK version issues)
# ═══════════════════════════════════════════════════════
class AlpacaClient:
    """Thin wrapper around Alpaca v2 REST API."""

    def __init__(self):
        self.api_key = os.getenv("ALPACA_API_KEY", "")
        self.secret = os.getenv("ALPACA_SECRET_KEY", "")
        base = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
        # Normalize: strip trailing /v2 or /v2/ — we always add /v2/ ourselves
        base = base.rstrip("/")
        if base.endswith("/v2"):
            base = base[:-3]
        self.base_url = base
        # Data API base (for quotes/snapshots)
        self.data_url = "https://data.alpaca.markets"
        self.headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret,
            "Content-Type": "application/json",
        }
        self.is_paper = "paper" in self.base_url.lower()

    # ── Account ───────────────────────────────────────
    def get_account(self) -> Dict:
        r = requests.get(f"{self.base_url}/v2/account", headers=self.headers, timeout=10)
        r.raise_for_status()
        return r.json()

    # ── Options contracts search ──────────────────────
    def search_option_contracts(
        self,
        symbol: str,
        option_type: str,        # 'call' or 'put'
        expiry_gte: str,
        expiry_lte: str,
        strike_gte: float,
        strike_lte: float,
    ) -> List[Dict]:
        params = {
            "underlying_symbols": symbol,
            "type": option_type,
            "expiration_date_gte": expiry_gte,
            "expiration_date_lte": expiry_lte,
            "strike_price_gte": str(strike_gte),
            "strike_price_lte": str(strike_lte),
            "status": "active",
            "limit": 50,
        }
        r = requests.get(
            f"{self.base_url}/v2/options/contracts",
            headers=self.headers, params=params, timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        # Alpaca may return {"option_contracts": [...]} or a list
        if isinstance(data, dict):
            return data.get("option_contracts", data.get("contracts", []))
        return data if isinstance(data, list) else []

    # ── Latest option snapshot (bid/ask) ──────────────
    def get_option_snapshot(self, occ_symbol: str) -> Dict:
        """Get latest quote for an option contract."""
        r = requests.get(
            f"{self.data_url}/v1beta1/options/snapshots/{occ_symbol}",
            headers=self.headers, timeout=10,
        )
        if r.status_code == 200:
            return r.json()
        return {}

    # ── Place order ───────────────────────────────────
    def place_order(
        self,
        symbol: str,
        qty: int,
        side: str,           # 'buy' or 'sell'
        order_type: str,     # 'market' or 'limit'
        time_in_force: str,  # 'day' or 'gtc'
        limit_price: float = None,
    ) -> Dict:
        payload: Dict[str, Any] = {
            "symbol": symbol,
            "qty": str(qty),
            "side": side,
            "type": order_type,
            "time_in_force": time_in_force,
        }
        if order_type == "limit" and limit_price is not None:
            payload["limit_price"] = str(round(limit_price, 2))
        r = requests.post(
            f"{self.base_url}/v2/orders",
            headers=self.headers, json=payload, timeout=15,
        )
        r.raise_for_status()
        return r.json()

    # ── Positions ─────────────────────────────────────
    def get_positions(self) -> List[Dict]:
        r = requests.get(f"{self.base_url}/v2/positions", headers=self.headers, timeout=10)
        r.raise_for_status()
        return r.json()

    def close_position(self, symbol_or_id: str) -> Dict:
        r = requests.delete(
            f"{self.base_url}/v2/positions/{symbol_or_id}",
            headers=self.headers, timeout=10,
        )
        r.raise_for_status()
        return r.json()

    # ── Orders (check status) ─────────────────────────
    def get_order(self, order_id: str) -> Dict:
        r = requests.get(
            f"{self.base_url}/v2/orders/{order_id}",
            headers=self.headers, timeout=10,
        )
        r.raise_for_status()
        return r.json()

    # ── Latest stock quote ────────────────────────────
    def get_latest_trade(self, symbol: str) -> Dict:
        r = requests.get(
            f"{self.data_url}/v2/stocks/{symbol}/trades/latest",
            headers=self.headers, timeout=10,
        )
        if r.status_code == 200:
            return r.json()
        return {}

    # ── Market clock ──────────────────────────────────
    def is_market_open(self) -> Tuple[bool, str]:
        """
        Check if the US stock market is currently open.
        Returns (is_open, next_event_description).
        """
        try:
            r = requests.get(
                f"{self.base_url}/v2/clock",
                headers=self.headers, timeout=10,
            )
            r.raise_for_status()
            clock = r.json()
            is_open = clock.get("is_open", False)
            if is_open:
                close_time = clock.get("next_close", "")
                return True, f"Market open — closes at {close_time}"
            else:
                open_time = clock.get("next_open", "")
                return False, f"Market closed — next open at {open_time}"
        except Exception as e:
            logger.warning(f"Clock check failed: {e} — assuming open")
            return True, "unknown (clock check failed)"


# ═══════════════════════════════════════════════════════
# Contract selection algorithm
# ═══════════════════════════════════════════════════════

def _compute_strike_range(price: float, option_type: str) -> Tuple[float, float]:
    """
    Compute OTM strike range for 3-10× return potential.
    For CALLS: strike 3-8 % above current price
    For PUTS:  strike 3-8 % below current price
    """
    if option_type == "call":
        lo = price * (1 + 0.03)
        hi = price * (1 + 0.08)
    else:
        lo = price * (1 - 0.08)
        hi = price * (1 - 0.03)

    # Round to nice strike levels
    if price < 50:
        lo, hi = round(lo, 0), round(hi, 0)
    elif price < 200:
        lo = round(lo / 2.5) * 2.5
        hi = round(hi / 2.5) * 2.5
    else:
        lo = round(lo / 5) * 5
        hi = round(hi / 5) * 5

    return min(lo, hi), max(lo, hi)


def _compute_expiry_range() -> Tuple[str, str]:
    """Return (min_expiry, max_expiry) strings for contract search."""
    today = date.today()
    gte = (today + timedelta(days=MIN_DAYS_TO_EXPIRY)).isoformat()
    lte = (today + timedelta(days=MAX_DAYS_TO_EXPIRY)).isoformat()
    return gte, lte


def _select_best_contract(
    contracts: List[Dict],
    option_type: str,
    current_price: float,
) -> Optional[Dict]:
    """
    From the list of contracts, pick the one with:
      1. Highest open_interest (liquidity)
      2. Strike closest to 5 % OTM
      3. Expiry 7-14 days preferred
    """
    if not contracts:
        return None

    target_strike = current_price * (1.05 if option_type == "call" else 0.95)
    today = date.today()

    scored = []
    for c in contracts:
        strike = float(c.get("strike_price", 0))
        oi = int(c.get("open_interest", 0) or 0)
        exp_str = c.get("expiration_date", "")
        if not exp_str or strike <= 0:
            continue
        exp_date = date.fromisoformat(exp_str)
        dte = (exp_date - today).days

        # Score: prefer close to target_strike, high OI, 7-14 day expiry
        strike_dist = abs(strike - target_strike) / current_price
        dte_penalty = abs(dte - 10) * 0.01  # prefer ~10 DTE
        oi_bonus = min(oi / 1000, 5.0)      # cap at 5

        score = oi_bonus - strike_dist * 10 - dte_penalty
        scored.append((score, c))

    if not scored:
        return None

    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]


# ═══════════════════════════════════════════════════════
# Trade execution
# ═══════════════════════════════════════════════════════

def _execute_single_trade(
    pick: Dict[str, Any],
    option_type: str,          # 'call' or 'put'
    session_label: str,
    db: TradeDB,
    client: AlpacaClient,
) -> Optional[str]:
    """
    Execute a single options trade for a pick.
    Returns the trade_id if successful, else None.
    """
    symbol = pick.get("symbol", "")
    score = pick.get("score", 0) or pick.get("source_score", 0)
    price = pick.get("price", 0)
    signals = pick.get("signals", [])
    source = pick.get("source_engine", "Moonshot" if option_type == "call" else "PutsEngine")

    if not symbol:
        return None

    # Get current stock price if not available
    if price <= 0:
        try:
            trade_data = client.get_latest_trade(symbol)
            price = float(trade_data.get("trade", {}).get("p", 0))
        except Exception:
            logger.warning(f"  ⚠️ Cannot get price for {symbol} — skipping")
            return None

    if price <= 0:
        logger.warning(f"  ⚠️ {symbol}: price is 0 — skipping trade")
        return None

    trade_id = f"ME-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{symbol}-{uuid.uuid4().hex[:6]}"
    scan_date = date.today().isoformat()

    logger.info(f"  🔍 {symbol} ({option_type.upper()}) — searching contracts "
                f"(price=${price:.2f}, score={score:.2f})")

    # 1. Compute strike and expiry range
    strike_lo, strike_hi = _compute_strike_range(price, option_type)
    expiry_gte, expiry_lte = _compute_expiry_range()

    logger.info(f"     Strike range: ${strike_lo:.0f}-${strike_hi:.0f} | "
                f"Expiry: {expiry_gte} to {expiry_lte}")

    # 2. Search for contracts
    try:
        contracts = client.search_option_contracts(
            symbol=symbol,
            option_type=option_type,
            expiry_gte=expiry_gte,
            expiry_lte=expiry_lte,
            strike_gte=strike_lo,
            strike_lte=strike_hi,
        )
    except Exception as e:
        logger.error(f"     ❌ Contract search failed for {symbol}: {e}")
        # Record failed attempt
        db.insert_trade({
            "trade_id": trade_id, "session": session_label,
            "scan_date": scan_date, "symbol": symbol,
            "option_type": option_type, "underlying_price": price,
            "meta_score": score, "meta_signals": signals,
            "source_engine": source, "status": "cancelled",
            "exit_reason": f"contract_search_failed: {e}",
        })
        return None

    if not contracts:
        logger.warning(f"     ⚠️ No contracts found for {symbol} {option_type.upper()}")
        db.insert_trade({
            "trade_id": trade_id, "session": session_label,
            "scan_date": scan_date, "symbol": symbol,
            "option_type": option_type, "underlying_price": price,
            "meta_score": score, "meta_signals": signals,
            "source_engine": source, "status": "cancelled",
            "exit_reason": "no_contracts_found",
        })
        return None

    logger.info(f"     Found {len(contracts)} contracts")

    # 3. Select best contract
    best = _select_best_contract(contracts, option_type, price)
    if not best:
        logger.warning(f"     ⚠️ No suitable contract for {symbol}")
        return None

    occ_symbol = best.get("symbol", "")
    strike = float(best.get("strike_price", 0))
    expiry = best.get("expiration_date", "")
    oi = best.get("open_interest", 0)

    logger.info(f"     ✅ Selected: {occ_symbol} | Strike ${strike:.0f} | "
                f"Exp {expiry} | OI {oi}")

    # 4. Get latest option quote for limit price
    entry_price = 0.0
    try:
        snap = client.get_option_snapshot(occ_symbol)
        if snap:
            latest_quote = snap.get("latestQuote", {})
            ask = float(latest_quote.get("ap", 0) or 0)
            bid = float(latest_quote.get("bp", 0) or 0)
            if ask > 0:
                # Use ask price for buy limit order
                entry_price = ask
            elif bid > 0:
                entry_price = bid * 1.05  # slightly above bid
    except Exception as e:
        logger.debug(f"     Snapshot failed: {e}")

    # 5. Place order
    try:
        if entry_price > 0:
            order = client.place_order(
                symbol=occ_symbol,
                qty=CONTRACTS_PER_TRADE,
                side="buy",
                order_type="limit",
                time_in_force="day",
                limit_price=entry_price,
            )
        else:
            # Fallback to market order
            order = client.place_order(
                symbol=occ_symbol,
                qty=CONTRACTS_PER_TRADE,
                side="buy",
                order_type="market",
                time_in_force="day",
            )

        order_id = order.get("id", "")
        order_status = order.get("status", "")
        filled_price = float(order.get("filled_avg_price", 0) or 0)
        if filled_price > 0:
            entry_price = filled_price

        logger.info(f"     📈 ORDER PLACED: {occ_symbol} x{CONTRACTS_PER_TRADE} @ "
                     f"${entry_price:.2f} | Status: {order_status} | ID: {order_id[:12]}")

        # 6. Save to database
        db_status = "filled" if order_status == "filled" else "pending"
        db.insert_trade({
            "trade_id": trade_id,
            "session": session_label,
            "scan_date": scan_date,
            "symbol": symbol,
            "option_symbol": occ_symbol,
            "option_type": option_type,
            "strike_price": strike,
            "expiry_date": expiry,
            "contracts": CONTRACTS_PER_TRADE,
            "entry_price": entry_price,
            "underlying_price": price,
            "meta_score": score,
            "meta_signals": signals,
            "source_engine": source,
            "entry_order_id": order_id,
            "status": db_status,
        })

        return trade_id

    except Exception as e:
        logger.error(f"     ❌ Order failed for {symbol}: {e}")
        db.insert_trade({
            "trade_id": trade_id, "session": session_label,
            "scan_date": scan_date, "symbol": symbol,
            "option_symbol": occ_symbol, "option_type": option_type,
            "strike_price": strike, "expiry_date": expiry,
            "contracts": CONTRACTS_PER_TRADE, "entry_price": entry_price,
            "underlying_price": price, "meta_score": score,
            "meta_signals": signals, "source_engine": source,
            "status": "cancelled", "exit_reason": f"order_failed: {e}",
        })
        return None


# ═══════════════════════════════════════════════════════
# Position management (exits)
# ═══════════════════════════════════════════════════════

def check_and_manage_positions(db: TradeDB = None, client: AlpacaClient = None) -> Dict:
    """
    Check all open positions and apply exit rules:
      • Take profit at 3× entry premium
      • Stop loss at 50 % drawdown
      • Time stop: close 1 day before expiry
    Called on every scan run (twice daily).
    """
    if db is None:
        db = TradeDB()
    if client is None:
        client = AlpacaClient()

    result = {"checked": 0, "closed": 0, "errors": 0, "details": []}

    # 1. Sync pending orders (check if filled)
    pending = db.get_pending_trades()
    for trade in pending:
        order_id = trade.get("entry_order_id", "")
        if not order_id:
            continue
        try:
            order = client.get_order(order_id)
            status = order.get("status", "")
            if status == "filled":
                filled_price = float(order.get("filled_avg_price", 0) or 0)
                db.update_trade(trade["trade_id"],
                                status="open",
                                entry_price=filled_price or trade.get("entry_price", 0),
                                filled_at=order.get("filled_at", ""))
                logger.info(f"  ✅ Order filled: {trade['symbol']} @ ${filled_price:.2f}")
            elif status in ("cancelled", "expired", "rejected"):
                db.update_trade(trade["trade_id"],
                                status="cancelled",
                                exit_reason=f"order_{status}")
                logger.info(f"  ❌ Order {status}: {trade['symbol']}")
        except Exception as e:
            logger.debug(f"  Order check failed for {trade['trade_id']}: {e}")

    # 2. Check open positions for exit signals
    open_trades = db.get_open_positions()
    if not open_trades:
        logger.info("  No open positions to manage.")
        return result

    # Get live positions from Alpaca
    try:
        live_positions = {p["symbol"]: p for p in client.get_positions()}
    except Exception as e:
        logger.error(f"  Failed to get Alpaca positions: {e}")
        return result

    today = date.today()

    for trade in open_trades:
        result["checked"] += 1
        occ = trade.get("option_symbol", "")
        entry_px = float(trade.get("entry_price", 0))
        expiry_str = trade.get("expiry_date", "")

        if not occ or entry_px <= 0:
            continue

        # Get current price from Alpaca position
        pos = live_positions.get(occ, {})
        current_px = float(pos.get("current_price", 0) or 0)
        unrealized_pnl = float(pos.get("unrealized_pl", 0) or 0)

        if current_px <= 0:
            # Try snapshot
            try:
                snap = client.get_option_snapshot(occ)
                latest_trade = snap.get("latestTrade", {})
                current_px = float(latest_trade.get("p", 0) or 0)
            except Exception:
                continue

        if current_px <= 0:
            continue

        # Update current price in DB
        contracts = int(trade.get("contracts", 5))
        pnl = (current_px - entry_px) * contracts * 100  # options are 100 shares
        pnl_pct = ((current_px / entry_px) - 1) * 100 if entry_px > 0 else 0

        db.update_trade(trade["trade_id"],
                        current_price=current_px, pnl=round(pnl, 2),
                        pnl_pct=round(pnl_pct, 1))

        # ── Exit Rules ────────────────────────────────
        exit_reason = None

        # Take profit: 3× entry premium
        if current_px >= entry_px * TAKE_PROFIT_MULT:
            exit_reason = "take_profit"
            logger.info(f"  🎯 TAKE PROFIT: {trade['symbol']} ({occ}) "
                        f"${entry_px:.2f}→${current_px:.2f} ({pnl_pct:+.0f}%)")

        # Stop loss: 50 % drop
        elif current_px <= entry_px * (1 - STOP_LOSS_PCT):
            exit_reason = "stop_loss"
            logger.info(f"  🛑 STOP LOSS: {trade['symbol']} ({occ}) "
                        f"${entry_px:.2f}→${current_px:.2f} ({pnl_pct:+.0f}%)")

        # Time stop: 1 day before expiry
        elif expiry_str:
            try:
                exp_date = date.fromisoformat(expiry_str)
                if (exp_date - today).days <= 1:
                    exit_reason = "time_stop"
                    logger.info(f"  ⏰ TIME STOP: {trade['symbol']} ({occ}) "
                                f"expiring {expiry_str}")
            except ValueError:
                pass

        # Execute exit
        if exit_reason:
            try:
                client.close_position(occ)
                db.update_trade(
                    trade["trade_id"],
                    status="closed",
                    exit_price=current_px,
                    exit_reason=exit_reason,
                    pnl=round(pnl, 2),
                    pnl_pct=round(pnl_pct, 1),
                    closed_at=datetime.utcnow().isoformat(),
                )
                result["closed"] += 1
                result["details"].append({
                    "symbol": trade["symbol"],
                    "reason": exit_reason,
                    "pnl": round(pnl, 2),
                })
            except Exception as e:
                logger.error(f"  ❌ Failed to close {occ}: {e}")
                result["errors"] += 1

    return result


# ═══════════════════════════════════════════════════════
# Main entry point
# ═══════════════════════════════════════════════════════

def execute_trades(
    cross_results: Dict[str, Any],
    session_label: str = "AM",
) -> Dict[str, Any]:
    """
    Execute options trades for the top 3 PUT and CALL picks.
    Called from meta_engine.py after report generation.

    Args:
        cross_results: Output from cross_analyze()
        session_label: 'PreMarket' (9:21), 'AM' (9:50), or 'PM' (3:15)

    Returns:
        Dict with trade execution results
    """
    logger.info("=" * 50)
    logger.info(f"💰 TRADING EXECUTOR — {session_label} Session")
    logger.info("=" * 50)

    db = TradeDB()
    client = AlpacaClient()

    # Verify Alpaca connection
    try:
        account = client.get_account()
        buying_power = float(account.get("buying_power", 0))
        equity = float(account.get("equity", 0))
        paper = "PAPER" if client.is_paper else "LIVE"
        logger.info(f"  📊 Alpaca [{paper}] — Equity: ${equity:,.2f} | "
                     f"Buying Power: ${buying_power:,.2f}")
    except Exception as e:
        logger.error(f"  ❌ Alpaca connection failed: {e}")
        return {"status": "error", "error": str(e), "trades": []}

    # Verify market is open before placing new trades
    market_open, market_msg = client.is_market_open()
    logger.info(f"  🕐 {market_msg}")
    if not market_open:
        logger.warning("  ⚠️ Market is closed — skipping new trades (will still manage open positions)")
        # Still manage existing positions (they may need exit)
        pos_result = check_and_manage_positions(db, client)
        return {
            "status": "market_closed",
            "message": market_msg,
            "positions_checked": pos_result.get("checked", 0),
            "positions_closed": pos_result.get("closed", 0),
            "trades_placed": 0,
            "trade_ids": [],
        }

    results = {
        "status": "ok",
        "session": session_label,
        "trades_attempted": 0,
        "trades_placed": 0,
        "positions_closed": 0,
        "trade_ids": [],
    }

    # ── Step 1: Manage existing positions ─────────────
    logger.info("\n  📋 Checking existing positions...")
    pos_result = check_and_manage_positions(db, client)
    results["positions_closed"] = pos_result.get("closed", 0)
    if pos_result["closed"] > 0:
        logger.info(f"  Closed {pos_result['closed']} positions")

    # ── Step 2: Execute new PUT trades (top 3) ────────
    puts_picks = cross_results.get("puts_through_moonshot", [])[:TOP_N_TRADES]
    logger.info(f"\n  🔴 Trading top {len(puts_picks)} PUT picks:")

    for i, pick in enumerate(puts_picks, 1):
        logger.info(f"\n  --- PUT #{i}: {pick.get('symbol', '?')} ---")
        results["trades_attempted"] += 1
        tid = _execute_single_trade(pick, "put", session_label, db, client)
        if tid:
            results["trades_placed"] += 1
            results["trade_ids"].append(tid)

    # ── Step 3: Execute new CALL trades (top 3) ───────
    moon_picks = cross_results.get("moonshot_through_puts", [])[:TOP_N_TRADES]
    logger.info(f"\n  🟢 Trading top {len(moon_picks)} CALL picks:")

    for i, pick in enumerate(moon_picks, 1):
        logger.info(f"\n  --- CALL #{i}: {pick.get('symbol', '?')} ---")
        results["trades_attempted"] += 1
        tid = _execute_single_trade(pick, "call", session_label, db, client)
        if tid:
            results["trades_placed"] += 1
            results["trade_ids"].append(tid)

    # ── Summary ───────────────────────────────────────
    logger.info(f"\n  💰 TRADING SUMMARY ({session_label}):")
    logger.info(f"     Attempted: {results['trades_attempted']}")
    logger.info(f"     Placed:    {results['trades_placed']}")
    logger.info(f"     Closed:    {results['positions_closed']}")
    logger.info("=" * 50)

    return results
