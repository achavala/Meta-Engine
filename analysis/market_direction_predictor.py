"""
Market Direction Predictor — Weather-Grade Precision
====================================================
Predicts market direction with 10+ indicator fusion,
outputting human-readable "weather" labels:

  🟢 Clean Green          — Strong bullish, low volatility
  🔴 Clean Red            — Strong bearish, low volatility
  🟢🌊 Green / Choppy     — Bullish bias with high intraday swings
  🔴🌊 Red / Choppy       — Bearish bias with high intraday swings
  🔴➡️🟢 Red → Green      — First half sell-off, second half recovery
  🟢➡️🔴 Green → Red      — First half rally fades into sell-off
  ⚪ Flat / Range-Bound   — No directional conviction

Indicators consumed (all read-only — zero new UW API calls):
  1. SPY 30-day bars + intraday (Polygon)
  2. QQQ 30-day bars + intraday (Polygon)
  3. VIX level & trend  (Polygon)
  4. GEX regime  (PutsEngine market_direction.json + UW cache)
  5. Futures pre-market  (PutsEngine market_direction.json)
  6. Sector breadth  (PutsEngine market_direction.json)
  7. Put/Call ratio  (UW OI change cache)
  8. Dark pool flow  (UW darkpool cache for SPY/QQQ)
  9. IV term structure  (UW IV term cache)
  10. Options flow sentiment  (UW flow cache)
  11. Key support/resistance levels  (Polygon bars)
  12. MACD / RSI momentum  (computed)

30+ years trading + PhD quant + institutional microstructure lens
"""

import json
import math
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import requests
import pytz

logger = logging.getLogger(__name__)

EST = pytz.timezone("US/Eastern")

# Direction categories (weather labels)
DIRECTIONS = {
    "CLEAN_GREEN":    "🟢 Clean Green",
    "CLEAN_RED":      "🔴 Clean Red",
    "GREEN_CHOPPY":   "🟢🌊 Green / Choppy",
    "RED_CHOPPY":     "🔴🌊 Red / Choppy",
    "RED_TO_GREEN":   "🔴➡️🟢 Red → Green",
    "GREEN_TO_RED":   "🟢➡️🔴 Green → Red",
    "FLAT":           "⚪ Flat / Range-Bound",
}

# Confidence levels
CONFIDENCE_LEVELS = {
    "HIGH":   "☀️ High Confidence",
    "MEDIUM": "⛅ Medium Confidence",
    "LOW":    "🌫️ Low Confidence",
}


class MarketDirectionPredictor:
    """
    Multi-indicator market direction predictor.
    Fuses 10+ signals into a weather-grade forecast.
    """

    def __init__(self):
        from config import MetaConfig
        self.polygon_key = MetaConfig.POLYGON_API_KEY
        self.putsengine_path = Path(MetaConfig.PUTSENGINE_PATH)
        self.tradenova_path = Path(MetaConfig.TRADENOVA_PATH)

    # ─── DATA FETCHING ─────────────────────────────────────────────────

    def _fetch_polygon_bars(self, symbol: str, days: int = 30) -> List[Dict]:
        """Fetch daily bars from Polygon."""
        if not self.polygon_key:
            return []
        try:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days + 15)).strftime("%Y-%m-%d")
            url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
            resp = requests.get(
                url,
                params={"adjusted": "true", "sort": "asc", "limit": 50, "apiKey": self.polygon_key},
                timeout=15,
            )
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                return [
                    {"o": r.get("o", 0), "h": r.get("h", 0), "l": r.get("l", 0),
                     "c": r.get("c", 0), "v": r.get("v", 0), "vw": r.get("vw", 0),
                     "t": r.get("t", 0)}
                    for r in results
                ]
        except Exception as e:
            logger.debug(f"Polygon bars fetch failed for {symbol}: {e}")
        return []

    def _fetch_polygon_prev_close(self, symbol: str) -> Dict:
        """Fetch previous close from Polygon."""
        if not self.polygon_key:
            return {}
        try:
            resp = requests.get(
                f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev",
                params={"apiKey": self.polygon_key},
                timeout=10,
            )
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results:
                    r = results[0]
                    return {"o": r.get("o", 0), "h": r.get("h", 0), "l": r.get("l", 0),
                            "c": r.get("c", 0), "v": r.get("v", 0), "vw": r.get("vw", 0)}
        except Exception as e:
            logger.debug(f"Polygon prev close failed for {symbol}: {e}")
        return {}

    def _fetch_polygon_snapshot(self, symbol: str) -> Dict:
        """Fetch real-time snapshot from Polygon."""
        if not self.polygon_key:
            return {}
        try:
            resp = requests.get(
                f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}",
                params={"apiKey": self.polygon_key},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json().get("ticker", {})
                return data
        except Exception as e:
            logger.debug(f"Polygon snapshot failed for {symbol}: {e}")
        return {}

    def _read_putsengine_direction(self) -> Dict:
        """Read PutsEngine market_direction.json."""
        md_path = self.putsengine_path / "logs" / "market_direction.json"
        try:
            if md_path.exists():
                with open(md_path) as f:
                    return json.load(f)
        except Exception as e:
            logger.debug(f"PutsEngine direction read failed: {e}")
        return {}

    def _read_uw_cache(self, cache_name: str) -> Dict:
        """Read UW cache file from TradeNova."""
        cache_map = {
            "darkpool": "darkpool_cache.json",
            "gex": "uw_gex_cache.json",
            "iv_term": "uw_iv_term_cache.json",
            "oi_change": "uw_oi_change_cache.json",
            "skew": "uw_skew_cache.json",
            "flow": "uw_flow_cache.json",
        }
        fname = cache_map.get(cache_name)
        if not fname:
            return {}
        try:
            fpath = self.tradenova_path / "data" / fname
            if fpath.exists():
                with open(fpath) as f:
                    data = json.load(f)
                # Some caches have inner "data" or "flow_data" key
                if cache_name == "flow":
                    return data.get("flow_data", data)
                if "data" in data and isinstance(data["data"], dict):
                    return data["data"]
                return data
        except Exception as e:
            logger.debug(f"UW cache read failed for {cache_name}: {e}")
        return {}

    # ─── TECHNICAL CALCULATIONS ────────────────────────────────────────

    @staticmethod
    def _calc_rsi(closes: List[float], period: int = 14) -> float:
        """Calculate RSI."""
        if len(closes) < period + 1:
            return 50.0
        deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains = [d if d > 0 else 0 for d in deltas]
        losses = [-d if d < 0 else 0 for d in deltas]

        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period

        for i in range(period, len(deltas)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    @staticmethod
    def _calc_ema(prices: List[float], period: int) -> float:
        """Calculate EMA."""
        if not prices or len(prices) < period:
            return prices[-1] if prices else 0
        multiplier = 2 / (period + 1)
        ema = sum(prices[:period]) / period
        for price in prices[period:]:
            ema = (price - ema) * multiplier + ema
        return ema

    @staticmethod
    def _calc_macd(closes: List[float]) -> Dict:
        """Calculate MACD (12, 26, 9)."""
        if len(closes) < 26:
            return {"macd": 0, "signal": 0, "histogram": 0}

        def ema_series(data, period):
            result = [sum(data[:period]) / period]
            mult = 2 / (period + 1)
            for val in data[period:]:
                result.append((val - result[-1]) * mult + result[-1])
            return result

        ema12 = ema_series(closes, 12)
        ema26 = ema_series(closes, 26)

        # Align lengths
        diff = len(ema12) - len(ema26)
        ema12 = ema12[diff:]

        macd_line = [a - b for a, b in zip(ema12, ema26)]
        if len(macd_line) >= 9:
            signal_line = ema_series(macd_line, 9)
            histogram = macd_line[-1] - signal_line[-1]
            return {"macd": macd_line[-1], "signal": signal_line[-1], "histogram": histogram}
        return {"macd": macd_line[-1] if macd_line else 0, "signal": 0, "histogram": 0}

    @staticmethod
    def _calc_atr(bars: List[Dict], period: int = 14) -> float:
        """Calculate Average True Range."""
        if len(bars) < period + 1:
            return 0
        trs = []
        for i in range(1, len(bars)):
            h, l, prev_c = bars[i]["h"], bars[i]["l"], bars[i - 1]["c"]
            tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
            trs.append(tr)
        if len(trs) < period:
            return sum(trs) / len(trs) if trs else 0
        return sum(trs[-period:]) / period

    @staticmethod
    def _find_support_resistance(bars: List[Dict], n: int = 5) -> Dict:
        """Find key support and resistance levels from recent bars."""
        if not bars:
            return {"support": 0, "resistance": 0}
        recent = bars[-20:] if len(bars) >= 20 else bars
        highs = [b["h"] for b in recent]
        lows = [b["l"] for b in recent]
        return {
            "support": min(lows),
            "resistance": max(highs),
            "pivot": sum(b["c"] for b in recent[-5:]) / 5,
        }

    # ─── SIGNAL SCORING ────────────────────────────────────────────────

    def _score_spy_technicals(self, bars: List[Dict]) -> Dict:
        """Score SPY technical indicators. Returns score -1.0 (bearish) to +1.0 (bullish)."""
        if not bars or len(bars) < 20:
            return {"score": 0.0, "signals": ["Insufficient SPY data"], "details": {}}

        closes = [b["c"] for b in bars]
        latest = bars[-1]
        prev = bars[-2] if len(bars) >= 2 else latest

        signals = []
        details = {}
        score = 0.0

        # 1. RSI
        rsi = self._calc_rsi(closes)
        details["rsi"] = rsi
        if rsi > 70:
            score -= 0.15
            signals.append(f"SPY RSI overbought ({rsi:.0f})")
        elif rsi > 60:
            score += 0.10
            signals.append(f"SPY RSI bullish ({rsi:.0f})")
        elif rsi < 30:
            score += 0.10  # oversold bounce potential
            signals.append(f"SPY RSI oversold bounce ({rsi:.0f})")
        elif rsi < 40:
            score -= 0.10
            signals.append(f"SPY RSI weak ({rsi:.0f})")
        else:
            signals.append(f"SPY RSI neutral ({rsi:.0f})")

        # 2. EMA trend
        ema9 = self._calc_ema(closes, 9)
        ema20 = self._calc_ema(closes, 20)
        ema50 = self._calc_ema(closes, min(50, len(closes)))
        details["ema9"] = ema9
        details["ema20"] = ema20
        details["ema50"] = ema50

        # 3. MACD (compute FIRST, used for EMA momentum decay)
        macd = self._calc_macd(closes)
        details["macd"] = macd

        # ── EMA scoring with MOMENTUM DECAY ──
        # If MACD histogram is negative (momentum fading), discount EMA bullish signal.
        # This prevents stale bullish EMA from dominating when trend is weakening.
        macd_weakening = macd["histogram"] < 0
        ema_decay = 0.40 if macd_weakening else 1.0  # 60% discount when momentum fading

        if ema9 > ema20 > ema50:
            ema_contribution = 0.20 * ema_decay
            score += ema_contribution
            if macd_weakening:
                signals.append(f"SPY EMA stacked bullish (9>20>50) [FADING: MACD hist neg → {ema_contribution:+.2f}]")
            else:
                signals.append("SPY EMA stacked bullish (9>20>50)")
        elif ema9 > ema20:
            score += 0.10 * ema_decay
            signals.append("SPY EMA9 > EMA20 (bullish)")
        elif ema9 < ema20 < ema50:
            score -= 0.20
            signals.append("SPY EMA stacked bearish (9<20<50)")
        elif ema9 < ema20:
            score -= 0.10
            signals.append("SPY EMA9 < EMA20 (bearish)")

        # ── MACD scoring ──
        if macd["histogram"] > 0 and macd["macd"] > 0:
            score += 0.15
            signals.append(f"SPY MACD bullish (hist +{macd['histogram']:.3f})")
        elif macd["histogram"] < 0 and macd["macd"] < 0:
            score -= 0.15
            signals.append(f"SPY MACD bearish (hist {macd['histogram']:.3f})")
        elif macd["histogram"] > 0:
            score += 0.05
            signals.append("SPY MACD improving")
        elif macd["histogram"] < 0:
            # Stronger penalty when MACD is weakening (histogram negative, line positive)
            score -= 0.10
            signals.append("SPY MACD weakening (hist negative)")

        # 4. Price vs VWAP
        vwap = latest.get("vw", 0)
        if vwap > 0 and latest["c"] > 0:
            details["vwap"] = vwap
            if latest["c"] > vwap:
                score += 0.05
                signals.append(f"SPY above VWAP (${latest['c']:.2f} > ${vwap:.2f})")
            else:
                score -= 0.05
                signals.append(f"SPY below VWAP (${latest['c']:.2f} < ${vwap:.2f})")

        # 5. Consecutive direction (ENHANCED: stronger weighting)
        reds = 0
        greens = 0
        for b in reversed(bars[-5:]):
            if b["c"] < b["o"]:
                reds += 1
            else:
                greens += 1
        if reds >= 4:
            score -= 0.20
            signals.append(f"SPY {reds}/5 red candles — STRONG selling pressure")
        elif reds >= 3:
            score -= 0.15
            signals.append(f"SPY {reds}/5 red candles — selling pressure")
        elif greens >= 4:
            score += 0.20
            signals.append(f"SPY {greens}/5 green candles — STRONG buying momentum")
        elif greens >= 3:
            score += 0.15
            signals.append(f"SPY {greens}/5 green candles — buying momentum")

        # 6. Day change (latest completed day)
        day_change = ((latest["c"] - latest["o"]) / latest["o"] * 100) if latest["o"] > 0 else 0
        details["day_change"] = day_change
        if day_change > 0.5:
            score += 0.10
            signals.append(f"SPY strong day ({day_change:+.2f}%)")
        elif day_change < -0.5:
            score -= 0.10
            signals.append(f"SPY weak day ({day_change:+.2f}%)")

        # 6b. Overnight gap analysis (close-to-open)
        if len(bars) >= 2:
            prev_close = bars[-2]["c"]
            curr_open = latest["o"]
            if prev_close > 0:
                gap_pct = (curr_open - prev_close) / prev_close * 100
                details["overnight_gap"] = gap_pct
                if gap_pct > 0.3:
                    score += 0.10
                    signals.append(f"SPY gap-up ({gap_pct:+.2f}%) — bullish overnight")
                elif gap_pct < -0.3:
                    score -= 0.10
                    signals.append(f"SPY gap-down ({gap_pct:+.2f}%) — bearish overnight")

        # 6c. Multi-day trend (close-to-close over 3 days)
        if len(bars) >= 4:
            close_3ago = bars[-4]["c"]
            close_now = latest["c"]
            if close_3ago > 0:
                trend_3d = (close_now - close_3ago) / close_3ago * 100
                details["trend_3d"] = trend_3d
                if trend_3d > 1.5:
                    score += 0.10
                    signals.append(f"SPY 3d trend strong ({trend_3d:+.2f}%)")
                elif trend_3d < -1.5:
                    score -= 0.10
                    signals.append(f"SPY 3d trend weak ({trend_3d:+.2f}%)")

        # 7. Support / Resistance proximity
        sr = self._find_support_resistance(bars)
        details["support"] = sr["support"]
        details["resistance"] = sr["resistance"]
        price = latest["c"]
        if price > 0 and sr["resistance"] > 0:
            to_resistance = (sr["resistance"] - price) / price * 100
            to_support = (price - sr["support"]) / price * 100
            if to_resistance < 0.5:
                score -= 0.05
                signals.append(f"SPY near resistance (${sr['resistance']:.2f})")
            elif to_support < 0.5:
                score += 0.05
                signals.append(f"SPY near support (${sr['support']:.2f})")

        # 8. ATR for choppiness
        atr = self._calc_atr(bars)
        details["atr"] = atr
        if price > 0 and atr > 0:
            atr_pct = atr / price * 100
            details["atr_pct"] = atr_pct
            if atr_pct > 1.5:
                signals.append(f"SPY high ATR ({atr_pct:.2f}%) — choppy")
                details["choppy"] = True
            else:
                details["choppy"] = False

        return {"score": max(-1.0, min(1.0, score)), "signals": signals, "details": details}

    def _score_vix(self, bars: List[Dict]) -> Dict:
        """
        Score VIX level and trend.
        
        Uses DUAL scoring:
        1. Absolute level (for actual VIX data)
        2. Percentile-based relative scoring (works with VIX proxies like VIXY)
        
        The percentile approach ensures the score differentiates even when
        absolute levels cluster in a narrow range (e.g., VIXY as VIX proxy).
        """
        if not bars:
            return {"score": 0.0, "signals": ["No VIX data"], "details": {}}

        signals = []
        details = {}
        score = 0.0

        latest = bars[-1]
        vix_close = latest["c"]
        details["vix_level"] = vix_close

        # ── Absolute level scoring (traditional VIX ranges) ──
        abs_score = 0.0
        if vix_close < 14:
            abs_score = 0.20
            signals.append(f"VIX very low ({vix_close:.1f}) — complacency/bullish")
        elif vix_close < 18:
            abs_score = 0.10
            signals.append(f"VIX low ({vix_close:.1f}) — calm/bullish")
        elif vix_close < 22:
            signals.append(f"VIX neutral ({vix_close:.1f})")
        elif vix_close < 28:
            abs_score = -0.15
            signals.append(f"VIX elevated ({vix_close:.1f}) — fear/bearish")
        else:
            abs_score = -0.25
            signals.append(f"VIX high ({vix_close:.1f}) — extreme fear")

        # ── Percentile-based relative scoring (works with any VIX proxy) ──
        # Compare current VIX to its own 20-day range
        rel_score = 0.0
        if len(bars) >= 20:
            closes_20d = [b["c"] for b in bars[-20:]]
            vix_min = min(closes_20d)
            vix_max = max(closes_20d)
            vix_range = vix_max - vix_min
            if vix_range > 0:
                percentile = (vix_close - vix_min) / vix_range  # 0 = 20d low, 1 = 20d high
                details["vix_20d_percentile"] = round(percentile * 100, 1)
                if percentile < 0.20:
                    rel_score = 0.15  # Near 20d low → complacent → bullish
                    signals.append(f"VIX near 20d low ({percentile*100:.0f}% percentile) — bullish")
                elif percentile < 0.40:
                    rel_score = 0.05  # Below median → mild bullish
                elif percentile > 0.80:
                    rel_score = -0.15  # Near 20d high → fear → bearish
                    signals.append(f"VIX near 20d high ({percentile*100:.0f}% percentile) — bearish")
                elif percentile > 0.60:
                    rel_score = -0.05  # Above median → mild bearish

        # Blend: 40% absolute, 60% relative (relative is more adaptive)
        score = abs_score * 0.40 + rel_score * 0.60

        # ── VIX TREND (3-day change) — most impactful VIX signal ──
        if len(bars) >= 4:
            vix_3d_ago = bars[-4]["c"]
            vix_change = ((vix_close - vix_3d_ago) / vix_3d_ago * 100) if vix_3d_ago > 0 else 0
            details["vix_3d_change"] = vix_change
            # Tighter thresholds for VIX proxies (VIXY moves less than VIX)
            if vix_change > 8:
                score -= 0.20
                signals.append(f"VIX spiking ({vix_change:+.1f}% in 3d) — RISK-OFF")
            elif vix_change > 3:
                score -= 0.10
                signals.append(f"VIX rising ({vix_change:+.1f}% in 3d) — caution")
            elif vix_change < -8:
                score += 0.20
                signals.append(f"VIX collapsing ({vix_change:+.1f}% in 3d) — RISK-ON")
            elif vix_change < -3:
                score += 0.10
                signals.append(f"VIX falling ({vix_change:+.1f}% in 3d) — improving")

        # ── VIX 1-day change (more responsive) ──
        if len(bars) >= 2:
            vix_1d_ago = bars[-2]["c"]
            vix_1d_change = ((vix_close - vix_1d_ago) / vix_1d_ago * 100) if vix_1d_ago > 0 else 0
            details["vix_1d_change"] = vix_1d_change
            if vix_1d_change > 5:
                score -= 0.10
                signals.append(f"VIX 1d spike ({vix_1d_change:+.1f}%) — fear")
            elif vix_1d_change < -5:
                score += 0.10
                signals.append(f"VIX 1d drop ({vix_1d_change:+.1f}%) — relief")

        # VIX term structure (contango = bullish, backwardation = bearish)
        if len(bars) >= 10:
            front_avg = sum(b["c"] for b in bars[-3:]) / 3
            back_avg = sum(b["c"] for b in bars[-10:-7]) / 3
            details["term_structure"] = "contango" if front_avg < back_avg else "backwardation"
            if front_avg < back_avg * 0.95:
                score += 0.05
                signals.append("VIX in contango (bullish)")
            elif front_avg > back_avg * 1.05:
                score -= 0.05
                signals.append("VIX in backwardation (bearish)")

        return {"score": max(-1.0, min(1.0, score)), "signals": signals, "details": details}

    def _score_gex_regime(self, pe_direction: Dict) -> Dict:
        """Score GEX regime from PutsEngine data and UW cache."""
        signals = []
        details = {}
        score = 0.0

        # From PutsEngine
        raw = pe_direction.get("raw_data", {})
        gamma_data = raw.get("gamma", {})
        gex_value = gamma_data.get("gex_value", 0)
        gamma_regime = gamma_data.get("gamma_regime", "NEUTRAL")
        details["gex_value"] = gex_value
        details["gamma_regime"] = gamma_regime

        # GEX regime scoring
        if gamma_regime == "POSITIVE" or gex_value > 0:
            score += 0.15
            signals.append(f"GEX POSITIVE ({gex_value:,.0f}) — dealer hedging suppresses vol")
        elif gamma_regime == "NEGATIVE" or gex_value < 0:
            score -= 0.15
            signals.append(f"GEX NEGATIVE ({gex_value:,.0f}) — dealer hedging amplifies vol")
            details["amplified_vol"] = True
        else:
            signals.append(f"GEX NEUTRAL ({gex_value:,.0f})")

        # Near gamma flip = extreme fragility
        notes = pe_direction.get("notes", [])
        for note in notes:
            if "KNIFE-EDGE" in note or "gamma flip" in note.lower():
                score -= 0.10
                signals.append("Near gamma flip — extreme fragility ⚠️")
                details["near_gamma_flip"] = True

        # UW GEX cache for SPY
        uw_gex = self._read_uw_cache("gex")
        spy_gex = uw_gex.get("SPY") or uw_gex.get("spy")
        if spy_gex:
            net_gex = spy_gex.get("net_gex", spy_gex.get("gex_net", 0))
            details["spy_uw_gex"] = net_gex
            if net_gex > 0:
                score += 0.05
                signals.append(f"SPY UW GEX positive ({net_gex:+,.0f})")
            elif net_gex < 0:
                score -= 0.05
                signals.append(f"SPY UW GEX negative ({net_gex:+,.0f})")

        return {"score": max(-1.0, min(1.0, score)), "signals": signals, "details": details}

    def _score_futures_premarket(self, pe_direction: Dict) -> Dict:
        """Score pre-market futures data."""
        signals = []
        details = {}
        score = 0.0

        raw = pe_direction.get("raw_data", {})
        futures = raw.get("futures", {})

        spy_change = futures.get("spy_change", 0)
        qqq_change = futures.get("qqq_change", 0)
        avg_change = futures.get("avg_change", 0)

        details["spy_futures"] = spy_change
        details["qqq_futures"] = qqq_change

        # Futures scoring
        if avg_change > 0.5:
            score += 0.25
            signals.append(f"Futures strong green (SPY {spy_change:+.2f}%, QQQ {qqq_change:+.2f}%)")
        elif avg_change > 0.1:
            score += 0.10
            signals.append(f"Futures mildly green (SPY {spy_change:+.2f}%, QQQ {qqq_change:+.2f}%)")
        elif avg_change < -0.5:
            score -= 0.25
            signals.append(f"Futures strong red (SPY {spy_change:+.2f}%, QQQ {qqq_change:+.2f}%)")
        elif avg_change < -0.1:
            score -= 0.10
            signals.append(f"Futures mildly red (SPY {spy_change:+.2f}%, QQQ {qqq_change:+.2f}%)")
        else:
            signals.append(f"Futures flat (SPY {spy_change:+.2f}%, QQQ {qqq_change:+.2f}%)")

        # Divergence (SPY vs QQQ)
        divergence = abs(spy_change - qqq_change)
        if divergence > 0.5:
            signals.append(f"SPY-QQQ divergence ({divergence:.2f}%) — rotation signal")
            details["spy_qqq_divergence"] = divergence

        return {"score": max(-1.0, min(1.0, score)), "signals": signals, "details": details}

    def _score_sector_breadth(self, pe_direction: Dict) -> Dict:
        """Score sector breadth from PutsEngine data."""
        signals = []
        details = {}
        score = 0.0

        raw = pe_direction.get("raw_data", {})
        breadth = raw.get("breadth", {})
        sectors = breadth.get("sectors", {})

        if not sectors:
            return {"score": 0.0, "signals": ["No sector breadth data"], "details": {}}

        # Count green/red sectors
        green_sectors = {k: v for k, v in sectors.items() if v > 0}
        red_sectors = {k: v for k, v in sectors.items() if v < 0}
        total = len(sectors)
        details["sectors"] = sectors
        details["green_count"] = len(green_sectors)
        details["red_count"] = len(red_sectors)

        # Breadth scoring
        green_pct = len(green_sectors) / total if total > 0 else 0.5
        if green_pct > 0.7:
            score += 0.20
            signals.append(f"Broad rally: {len(green_sectors)}/{total} sectors green")
        elif green_pct > 0.5:
            score += 0.05
            signals.append(f"Mild breadth: {len(green_sectors)}/{total} sectors green")
        elif green_pct < 0.3:
            score -= 0.20
            signals.append(f"Broad sell-off: {len(red_sectors)}/{total} sectors red")
        elif green_pct < 0.5:
            score -= 0.05
            signals.append(f"Weak breadth: {len(red_sectors)}/{total} sectors red")
        else:
            signals.append(f"Mixed breadth: {len(green_sectors)} green, {len(red_sectors)} red")

        # Identify leading sectors
        if sectors:
            best_sector = max(sectors, key=sectors.get)
            worst_sector = min(sectors, key=sectors.get)
            signals.append(f"Leader: {best_sector} ({sectors[best_sector]:+.2f}%)")
            signals.append(f"Laggard: {worst_sector} ({sectors[worst_sector]:+.2f}%)")
            details["leader"] = best_sector
            details["laggard"] = worst_sector

        return {"score": max(-1.0, min(1.0, score)), "signals": signals, "details": details}

    def _score_put_call_ratio(self) -> Dict:
        """Score put/call ratio from UW OI change cache."""
        signals = []
        details = {}
        score = 0.0

        oi_cache = self._read_uw_cache("oi_change")
        if not oi_cache:
            return {"score": 0.0, "signals": ["No OI change data"], "details": {}}

        # Aggregate put/call OI across all symbols
        total_call_oi = 0
        total_put_oi = 0
        for symbol_data in oi_cache.values():
            if isinstance(symbol_data, dict):
                total_call_oi += abs(symbol_data.get("call_oi_change", 0))
                total_put_oi += abs(symbol_data.get("put_oi_change", 0))

        if total_call_oi > 0:
            pc_ratio = total_put_oi / total_call_oi
            details["put_call_ratio"] = pc_ratio
            details["total_call_oi"] = total_call_oi
            details["total_put_oi"] = total_put_oi

            # P/C ratio scoring (contrarian: high = bullish, low = bearish)
            if pc_ratio > 1.2:
                score += 0.10  # Contrarian bullish
                signals.append(f"P/C ratio high ({pc_ratio:.2f}) — contrarian bullish (excess fear)")
            elif pc_ratio > 0.9:
                signals.append(f"P/C ratio neutral ({pc_ratio:.2f})")
            elif pc_ratio < 0.6:
                score -= 0.10  # Contrarian bearish
                signals.append(f"P/C ratio low ({pc_ratio:.2f}) — contrarian bearish (complacency)")
            else:
                signals.append(f"P/C ratio mildly low ({pc_ratio:.2f})")
        else:
            signals.append("No aggregate OI data available")

        return {"score": max(-1.0, min(1.0, score)), "signals": signals, "details": details}

    def _score_dark_pool(self) -> Dict:
        """Score dark pool activity for SPY/QQQ."""
        signals = []
        details = {}
        score = 0.0

        dp_cache = self._read_uw_cache("darkpool")
        if not dp_cache:
            return {"score": 0.0, "signals": ["No dark pool data"], "details": {}}

        for sym in ["SPY", "QQQ"]:
            data = dp_cache.get(sym)
            if data and isinstance(data, dict):
                blocks = data.get("block_count", 0)
                total_val = data.get("total_value", 0)
                net_flow = data.get("net_flow", 0) or data.get("net_delta", 0)
                details[f"{sym}_blocks"] = blocks
                details[f"{sym}_value"] = total_val

                if net_flow > 0:
                    score += 0.05
                    signals.append(f"{sym} dark pool net positive ({net_flow:+,.0f})")
                elif net_flow < 0:
                    score -= 0.05
                    signals.append(f"{sym} dark pool net negative ({net_flow:+,.0f})")

                if blocks > 50:
                    signals.append(f"{sym} heavy institutional activity ({blocks} blocks)")
                elif blocks > 20:
                    signals.append(f"{sym} moderate institutional activity ({blocks} blocks)")

        return {"score": max(-1.0, min(1.0, score)), "signals": signals, "details": details}

    def _score_iv_structure(self) -> Dict:
        """Score IV term structure for market stress."""
        signals = []
        details = {}
        score = 0.0

        iv_cache = self._read_uw_cache("iv_term")
        if not iv_cache:
            return {"score": 0.0, "signals": ["No IV term data"], "details": {}}

        # Check SPY IV term structure
        spy_iv = iv_cache.get("SPY")
        if spy_iv and isinstance(spy_iv, dict):
            front_iv = spy_iv.get("front_iv", 0)
            back_iv = spy_iv.get("back_iv", 0)
            details["spy_front_iv"] = front_iv
            details["spy_back_iv"] = back_iv

            if front_iv > 0 and back_iv > 0:
                ratio = front_iv / back_iv
                details["iv_ratio"] = ratio
                if ratio > 1.15:
                    score -= 0.15
                    signals.append(f"SPY IV INVERTED (front {front_iv:.0f}% > back {back_iv:.0f}%) — stress")
                elif ratio > 1.05:
                    score -= 0.05
                    signals.append(f"SPY IV mildly inverted ({ratio:.2f}x)")
                elif ratio < 0.90:
                    score += 0.10
                    signals.append(f"SPY IV contango (front {front_iv:.0f}% < back {back_iv:.0f}%) — calm")
                else:
                    signals.append(f"SPY IV normal (ratio {ratio:.2f})")

        return {"score": max(-1.0, min(1.0, score)), "signals": signals, "details": details}

    def _score_options_flow(self) -> Dict:
        """Score aggregate options flow sentiment."""
        signals = []
        details = {}
        score = 0.0

        flow_cache = self._read_uw_cache("flow")
        if not flow_cache:
            return {"score": 0.0, "signals": ["No options flow data"], "details": {}}

        # Aggregate across major symbols
        total_call_premium = 0
        total_put_premium = 0
        bullish_sweeps = 0
        bearish_sweeps = 0

        for symbol, entries in flow_cache.items():
            if not isinstance(entries, list):
                continue
            for e in entries:
                premium = e.get("premium", 0) or 0
                pc = e.get("put_call", "").upper()
                trade_type = (e.get("trade_type", "") or "").lower()

                if pc == "C":
                    total_call_premium += premium
                    if "sweep" in trade_type:
                        bullish_sweeps += 1
                elif pc == "P":
                    total_put_premium += premium
                    if "sweep" in trade_type:
                        bearish_sweeps += 1

        details["total_call_premium"] = total_call_premium
        details["total_put_premium"] = total_put_premium
        details["bullish_sweeps"] = bullish_sweeps
        details["bearish_sweeps"] = bearish_sweeps

        if total_call_premium + total_put_premium > 0:
            call_pct = total_call_premium / (total_call_premium + total_put_premium)
            details["call_premium_pct"] = call_pct

            if call_pct > 0.65:
                score += 0.10
                signals.append(f"Options flow bullish ({call_pct:.0%} call premium)")
            elif call_pct < 0.35:
                score -= 0.10
                signals.append(f"Options flow bearish ({1-call_pct:.0%} put premium)")
            else:
                signals.append(f"Options flow mixed ({call_pct:.0%} call premium)")

        if bullish_sweeps > bearish_sweeps * 2:
            score += 0.05
            signals.append(f"Bullish sweep dominance ({bullish_sweeps} vs {bearish_sweeps})")
        elif bearish_sweeps > bullish_sweeps * 2:
            score -= 0.05
            signals.append(f"Bearish sweep dominance ({bearish_sweeps} vs {bullish_sweeps})")

        return {"score": max(-1.0, min(1.0, score)), "signals": signals, "details": details}

    def _score_qqq_technicals(self, bars: List[Dict]) -> Dict:
        """Score QQQ technical indicators (lighter weight, complementary to SPY)."""
        if not bars or len(bars) < 15:
            return {"score": 0.0, "signals": ["Insufficient QQQ data"], "details": {}}

        closes = [b["c"] for b in bars]
        latest = bars[-1]
        signals = []
        details = {}
        score = 0.0

        # RSI
        rsi = self._calc_rsi(closes)
        details["qqq_rsi"] = rsi

        # MACD for momentum decay
        macd = self._calc_macd(closes)
        macd_weakening = macd["histogram"] < 0

        # EMA alignment with momentum decay
        ema9 = self._calc_ema(closes, 9)
        ema20 = self._calc_ema(closes, 20)
        ema_decay = 0.40 if macd_weakening else 1.0
        if ema9 > ema20:
            ema_contrib = 0.10 * ema_decay
            score += ema_contrib
            if macd_weakening:
                signals.append(f"QQQ EMA bullish [FADING] RSI {rsi:.0f}")
            else:
                signals.append(f"QQQ EMA9 > EMA20 (bullish), RSI {rsi:.0f}")
        else:
            score -= 0.10
            signals.append(f"QQQ EMA9 < EMA20 (bearish), RSI {rsi:.0f}")

        # Day change
        day_change = ((latest["c"] - latest["o"]) / latest["o"] * 100) if latest["o"] > 0 else 0
        details["qqq_day_change"] = day_change
        if day_change > 0.5:
            score += 0.05
            signals.append(f"QQQ strong ({day_change:+.2f}%)")
        elif day_change < -0.5:
            score -= 0.05
            signals.append(f"QQQ weak ({day_change:+.2f}%)")

        # 3-day trend
        if len(bars) >= 4:
            close_3ago = bars[-4]["c"]
            if close_3ago > 0:
                trend_3d = (latest["c"] - close_3ago) / close_3ago * 100
                details["qqq_trend_3d"] = trend_3d
                if trend_3d > 1.5:
                    score += 0.05
                    signals.append(f"QQQ 3d trend strong ({trend_3d:+.2f}%)")
                elif trend_3d < -1.5:
                    score -= 0.05
                    signals.append(f"QQQ 3d trend weak ({trend_3d:+.2f}%)")

        # Consecutive direction
        reds = sum(1 for b in bars[-5:] if b["c"] < b["o"])
        greens = 5 - reds
        if reds >= 3:
            score -= 0.05
            signals.append(f"QQQ {reds}/5 red candles")
        elif greens >= 3:
            score += 0.05
            signals.append(f"QQQ {greens}/5 green candles")

        return {"score": max(-1.0, min(1.0, score)), "signals": signals, "details": details}

    # ─── DIRECTION CLASSIFICATION ──────────────────────────────────────

    def _classify_direction(
        self,
        composite_score: float,
        is_choppy: bool,
        intraday_reversal_signal: str,
    ) -> str:
        """
        Classify the composite score into a weather-grade direction label.

        Args:
            composite_score: -1.0 (bearish) to +1.0 (bullish)
            is_choppy: True if high ATR / GEX negative / VIX elevated
            intraday_reversal_signal: "RED_TO_GREEN", "GREEN_TO_RED", or ""
        """
        # Intraday reversal patterns take priority if signal is strong
        if intraday_reversal_signal == "RED_TO_GREEN" and composite_score > -0.15:
            return "RED_TO_GREEN"
        if intraday_reversal_signal == "GREEN_TO_RED" and composite_score < 0.15:
            return "GREEN_TO_RED"

        # Main classification — tighter thresholds to reduce false FLAT predictions
        if composite_score > 0.25:
            return "CLEAN_GREEN" if not is_choppy else "GREEN_CHOPPY"
        elif composite_score > 0.05:
            return "GREEN_CHOPPY" if is_choppy else "CLEAN_GREEN"
        elif composite_score < -0.25:
            return "CLEAN_RED" if not is_choppy else "RED_CHOPPY"
        elif composite_score < -0.05:
            return "RED_CHOPPY" if is_choppy else "CLEAN_RED"
        else:
            # Near zero — check for reversal patterns
            if intraday_reversal_signal:
                return intraday_reversal_signal
            return "FLAT"

    def _detect_reversal_pattern(self, spy_bars: List[Dict], pe_direction: Dict) -> str:
        """
        Detect intraday reversal patterns for tomorrow prediction.
        
        Red → Green: strong morning selloff followed by recovery
        Green → Red: morning rally fades
        """
        if not spy_bars or len(spy_bars) < 3:
            return ""

        latest = spy_bars[-1]
        prev = spy_bars[-2]
        prev2 = spy_bars[-3]

        # Pattern: 2+ red days followed by bullish reversal candle
        if (prev["c"] < prev["o"] and prev2["c"] < prev2["o"] and
                latest["c"] > latest["o"] and latest["c"] > prev["c"]):
            return "RED_TO_GREEN"

        # Pattern: 2+ green days followed by bearish reversal candle
        if (prev["c"] > prev["o"] and prev2["c"] > prev2["o"] and
                latest["c"] < latest["o"] and latest["c"] < prev["c"]):
            return "GREEN_TO_RED"

        # Hammer / shooting star patterns
        body = abs(latest["c"] - latest["o"])
        lower_wick = min(latest["o"], latest["c"]) - latest["l"]
        upper_wick = latest["h"] - max(latest["o"], latest["c"])

        if body > 0:
            # Hammer (bullish reversal) = long lower wick, small body
            if lower_wick > body * 2 and latest["c"] > latest["o"]:
                return "RED_TO_GREEN"
            # Shooting star (bearish reversal) = long upper wick, small body
            if upper_wick > body * 2 and latest["c"] < latest["o"]:
                return "GREEN_TO_RED"

        return ""

    def _determine_confidence(self, scores: Dict[str, Dict]) -> Tuple[str, float]:
        """Determine prediction confidence based on signal agreement."""
        # Count agreeing signals
        bullish = sum(1 for s in scores.values() if s.get("score", 0) > 0.05)
        bearish = sum(1 for s in scores.values() if s.get("score", 0) < -0.05)
        total = bullish + bearish
        
        if total == 0:
            return "LOW", 40.0

        agreement = max(bullish, bearish) / total
        
        if agreement > 0.80 and total >= 5:
            return "HIGH", min(92.0, 70 + agreement * 25)
        elif agreement > 0.65 and total >= 4:
            return "MEDIUM", min(80.0, 55 + agreement * 30)
        else:
            return "LOW", min(65.0, 40 + agreement * 30)

    # ─── MAIN PREDICTION ──────────────────────────────────────────────

    def predict_market_direction(self, timeframe: str = "today") -> Dict[str, Any]:
        """
        Generate market direction prediction.
        
        Args:
            timeframe: "today" (for AM 9:35 scan) or "tomorrow" (for PM 3:15 scan)
        
        Returns:
            Dict with direction, confidence, scores, signals, and label
        """
        logger.info(f"🌤️ Predicting market direction ({timeframe})...")

        # 1. Fetch all data
        spy_bars = self._fetch_polygon_bars("SPY", days=30)
        qqq_bars = self._fetch_polygon_bars("QQQ", days=30)
        vix_bars = self._fetch_polygon_bars("VIX", days=30)
        pe_direction = self._read_putsengine_direction()

        # 2. Score each indicator
        scores = {}

        # SPY Technicals (weight: 25%)
        spy_score = self._score_spy_technicals(spy_bars)
        scores["spy_technicals"] = spy_score
        logger.info(f"  SPY: {spy_score['score']:+.3f} ({len(spy_score['signals'])} signals)")

        # QQQ Technicals (weight: 10%)
        qqq_score = self._score_qqq_technicals(qqq_bars)
        scores["qqq_technicals"] = qqq_score
        logger.info(f"  QQQ: {qqq_score['score']:+.3f}")

        # VIX (weight: 15%)
        vix_score = self._score_vix(vix_bars)
        scores["vix"] = vix_score
        logger.info(f"  VIX: {vix_score['score']:+.3f}")

        # GEX Regime (weight: 15%)
        gex_score = self._score_gex_regime(pe_direction)
        scores["gex_regime"] = gex_score
        logger.info(f"  GEX: {gex_score['score']:+.3f}")

        # Futures Pre-Market (weight: 10% for today, 5% for tomorrow)
        futures_score = self._score_futures_premarket(pe_direction)
        scores["futures"] = futures_score
        logger.info(f"  Futures: {futures_score['score']:+.3f}")

        # Sector Breadth (weight: 10%)
        breadth_score = self._score_sector_breadth(pe_direction)
        scores["breadth"] = breadth_score
        logger.info(f"  Breadth: {breadth_score['score']:+.3f}")

        # Put/Call Ratio (weight: 5%)
        pc_score = self._score_put_call_ratio()
        scores["put_call_ratio"] = pc_score
        logger.info(f"  P/C: {pc_score['score']:+.3f}")

        # Dark Pool (weight: 5%)
        dp_score = self._score_dark_pool()
        scores["dark_pool"] = dp_score
        logger.info(f"  DP: {dp_score['score']:+.3f}")

        # IV Structure (weight: 5%)
        iv_score = self._score_iv_structure()
        scores["iv_structure"] = iv_score
        logger.info(f"  IV: {iv_score['score']:+.3f}")

        # Options Flow (weight: 5% today, 10% tomorrow)
        flow_score = self._score_options_flow()
        scores["options_flow"] = flow_score
        logger.info(f"  Flow: {flow_score['score']:+.3f}")

        # 3. Weighted composite
        if timeframe == "today":
            weights = {
                "spy_technicals": 0.25, "qqq_technicals": 0.10,
                "vix": 0.15, "gex_regime": 0.15,
                "futures": 0.10, "breadth": 0.10,
                "put_call_ratio": 0.05, "dark_pool": 0.03,
                "iv_structure": 0.04, "options_flow": 0.03,
            }
        else:  # tomorrow
            weights = {
                "spy_technicals": 0.20, "qqq_technicals": 0.10,
                "vix": 0.15, "gex_regime": 0.15,
                "futures": 0.05, "breadth": 0.10,
                "put_call_ratio": 0.08, "dark_pool": 0.05,
                "iv_structure": 0.05, "options_flow": 0.07,
            }

        composite = sum(
            scores[k]["score"] * w for k, w in weights.items()
            if k in scores
        )

        # 4. Determine choppiness
        is_choppy = False
        spy_details = spy_score.get("details", {})
        vix_details = vix_score.get("details", {})
        gex_details = gex_score.get("details", {})

        if spy_details.get("choppy", False):
            is_choppy = True
        if vix_details.get("vix_level", 0) > 22:
            is_choppy = True
        if gex_details.get("amplified_vol", False):
            is_choppy = True
        if gex_details.get("near_gamma_flip", False):
            is_choppy = True

        # 5. Detect reversal patterns (for tomorrow forecast)
        reversal = ""
        if timeframe == "tomorrow":
            reversal = self._detect_reversal_pattern(spy_bars, pe_direction)

        # 6. Classify direction
        direction_key = self._classify_direction(composite, is_choppy, reversal)
        direction_label = DIRECTIONS.get(direction_key, DIRECTIONS["FLAT"])

        # 7. Confidence level
        confidence_level, confidence_pct = self._determine_confidence(scores)

        # 8. Compile all signals
        all_signals = []
        for k, v in scores.items():
            all_signals.extend(v.get("signals", []))

        # 9. Generate notes/rationale
        rationale = self._generate_rationale(direction_key, scores, composite, is_choppy)

        prediction = {
            "timestamp": datetime.now(EST).isoformat(),
            "timeframe": timeframe,
            "direction_key": direction_key,
            "direction_label": direction_label,
            "composite_score": round(composite, 4),
            "confidence_level": confidence_level,
            "confidence_pct": round(confidence_pct, 1),
            "confidence_label": CONFIDENCE_LEVELS.get(confidence_level, ""),
            "is_choppy": is_choppy,
            "reversal_pattern": reversal,
            "scores": {k: {"score": v["score"], "signal_count": len(v.get("signals", []))}
                       for k, v in scores.items()},
            "signals": all_signals,
            "rationale": rationale,
            "indicator_details": {k: v.get("details", {}) for k, v in scores.items()},
        }

        logger.info(f"  🌤️ Prediction: {direction_label} ({confidence_level}, {confidence_pct:.0f}%)")
        logger.info(f"  📊 Composite: {composite:+.4f} | Choppy: {is_choppy}")

        # Save prediction
        self._save_prediction(prediction)

        return prediction

    def _generate_rationale(
        self,
        direction_key: str,
        scores: Dict[str, Dict],
        composite: float,
        is_choppy: bool,
    ) -> str:
        """Generate human-readable rationale."""
        parts = []

        if direction_key in ("CLEAN_GREEN", "GREEN_CHOPPY"):
            parts.append("Bullish bias detected.")
        elif direction_key in ("CLEAN_RED", "RED_CHOPPY"):
            parts.append("Bearish bias detected.")
        elif direction_key == "RED_TO_GREEN":
            parts.append("Reversal pattern: expect early weakness followed by recovery.")
        elif direction_key == "GREEN_TO_RED":
            parts.append("Reversal pattern: expect early strength followed by fade.")
        else:
            parts.append("No clear directional bias.")

        # Add key signal details
        key_signals = []
        for k, v in scores.items():
            if abs(v["score"]) >= 0.10:
                direction = "bullish" if v["score"] > 0 else "bearish"
                key_signals.append(f"{k.replace('_', ' ').title()}: {direction} ({v['score']:+.2f})")

        if key_signals:
            parts.append("Key drivers: " + "; ".join(key_signals[:4]))

        if is_choppy:
            parts.append("⚠️ Expect high intraday volatility.")

        return " ".join(parts)

    def _save_prediction(self, prediction: Dict):
        """Save prediction to file."""
        try:
            output_dir = Path(__file__).parent.parent / "output"
            output_dir.mkdir(exist_ok=True)
            
            fname = f"market_direction_{prediction['timeframe']}_{datetime.now(EST).strftime('%Y%m%d_%H%M%S')}.json"
            with open(output_dir / fname, "w") as f:
                json.dump(prediction, f, indent=2, default=str)
            
            # Also save as "latest"
            latest_path = output_dir / f"market_direction_{prediction['timeframe']}_latest.json"
            with open(latest_path, "w") as f:
                json.dump(prediction, f, indent=2, default=str)
            
            logger.info(f"  💾 Saved: {fname}")
        except Exception as e:
            logger.debug(f"Failed to save prediction: {e}")

    # ─── DISPLAY FORMATTING ────────────────────────────────────────────

    def format_for_x_post(self, prediction: Dict) -> str:
        """Format prediction for X/Twitter post (compact, fits in header tweet)."""
        timeframe = prediction.get("timeframe", "today")
        label = prediction.get("direction_label", "⚪ Flat")
        confidence = prediction.get("confidence_pct", 0)

        if timeframe == "today":
            return f"📊 Market Today: {label} ({confidence:.0f}%)"
        else:
            return f"📊 Tomorrow: {label} ({confidence:.0f}%)"

    def format_for_email(self, prediction: Dict) -> str:
        """Format prediction for email report (detailed)."""
        timeframe = prediction.get("timeframe", "today")
        label = prediction.get("direction_label", "⚪ Flat")
        conf = prediction.get("confidence_label", "")
        rationale = prediction.get("rationale", "")
        composite = prediction.get("composite_score", 0)

        header = "Market Direction Today" if timeframe == "today" else "Tomorrow Market Direction"

        lines = [
            f"{'='*50}",
            f"🌤️ {header}",
            f"{'='*50}",
            f"",
            f"  Direction:  {label}",
            f"  Confidence: {conf} ({prediction.get('confidence_pct', 0):.0f}%)",
            f"  Composite:  {composite:+.4f}",
            f"  Choppy:     {'Yes ⚠️' if prediction.get('is_choppy') else 'No'}",
            f"",
            f"  📝 {rationale}",
            f"",
        ]

        # Add key signals
        lines.append("  Key Signals:")
        for signal in prediction.get("signals", [])[:8]:
            lines.append(f"    • {signal}")

        return "\n".join(lines)

    def format_for_telegram(self, prediction: Dict) -> str:
        """Format prediction for Telegram (HTML, compact but informative)."""
        def _esc(text: str) -> str:
            """Escape HTML special chars in raw data for Telegram."""
            return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        timeframe = prediction.get("timeframe", "today")
        label = _esc(prediction.get("direction_label", "⚪ Flat"))
        conf_pct = prediction.get("confidence_pct", 0)
        rationale = _esc(prediction.get("rationale", ""))

        header = "Market Direction Today" if timeframe == "today" else "Tomorrow Market Direction"

        lines = [
            f"🌤️ <b>{header}</b>",
            f"",
            f"<b>{label}</b>",
            f"Confidence: {conf_pct:.0f}%",
            f"",
        ]

        if rationale:
            lines.append(f"<i>{rationale}</i>")
            lines.append("")

        # Top 5 signals
        signals = prediction.get("signals", [])[:5]
        if signals:
            lines.append("Key Signals:")
            for s in signals:
                lines.append(f"  • {_esc(s)}")

        return "\n".join(lines)

    def format_prediction_for_display(self, prediction: Dict) -> str:
        """Compact one-line display for embedding."""
        return prediction.get("direction_label", "⚪ Flat")


def get_market_direction_for_scan(session_label: str = "AM") -> Dict[str, Any]:
    """
    Convenience function called from meta_engine pipeline.
    
    Args:
        session_label: "AM" or "PM"
    
    Returns:
        Prediction dict
    """
    predictor = MarketDirectionPredictor()
    timeframe = "today" if session_label.upper() == "AM" else "tomorrow"
    return predictor.predict_market_direction(timeframe=timeframe)
