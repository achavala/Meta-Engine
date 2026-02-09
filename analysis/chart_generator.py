"""
Technical Analysis Chart Generator
====================================
Generates a composite chart with:
  - Price bars/candles for top picks from both engines
  - RSI indicator
  - Volume bars
  - Annotated with engine signals (Puts vs Moonshot)

Uses matplotlib for chart generation and saves as PNG for email attachment.
"""

import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path
import logging

import requests

logger = logging.getLogger(__name__)


def _fetch_price_history(symbol: str, api_key: str, days: int = 30) -> List[Dict]:
    """Fetch daily price bars from Polygon API."""
    try:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days + 10)).strftime("%Y-%m-%d")
        
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
        resp = requests.get(url, params={"apiKey": api_key, "limit": days, "sort": "asc"}, timeout=15)
        
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("results", [])
            return results
    except Exception as e:
        logger.debug(f"Failed to fetch price data for {symbol}: {e}")
    
    return []


def _calculate_rsi(prices: List[float], period: int = 14) -> List[float]:
    """Calculate RSI from a list of closing prices."""
    if len(prices) < period + 1:
        return [50.0] * len(prices)
    
    rsi_values = [50.0] * period  # Pad initial values
    
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    
    # Initial average gain/loss
    gains = [max(d, 0) for d in deltas[:period]]
    losses = [abs(min(d, 0)) for d in deltas[:period]]
    
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    
    for i in range(period, len(deltas)):
        delta = deltas[i]
        gain = max(delta, 0)
        loss = abs(min(delta, 0))
        
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        
        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))
        
        rsi_values.append(rsi)
    
    return rsi_values


def generate_meta_chart(
    cross_results: Dict[str, Any],
    polygon_api_key: str,
    output_dir: str = "",
    top_n: int = 6,
) -> Optional[str]:
    """
    Generate a composite technical analysis chart for the top picks.
    
    Creates a multi-panel chart showing:
    - Top 3 PutsEngine picks (bearish signals highlighted)
    - Top 3 Moonshot picks (bullish signals highlighted)
    Each with price action + RSI subplot.
    
    Args:
        cross_results: Output from cross_analyzer
        polygon_api_key: API key for price data
        output_dir: Directory to save chart
        top_n: Number of tickers to chart (split between engines)
        
    Returns:
        Path to saved chart PNG, or None if failed
    """
    try:
        import matplotlib
        matplotlib.use('Agg')  # Non-interactive backend
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from matplotlib.patches import FancyBboxPatch
    except ImportError:
        logger.error("matplotlib not installed. Run: pip install matplotlib")
        return None
    
    puts_picks = cross_results.get("puts_through_moonshot", [])[:top_n // 2]
    moon_picks = cross_results.get("moonshot_through_puts", [])[:top_n // 2]
    
    all_picks = []
    for p in puts_picks:
        all_picks.append({"symbol": p["symbol"], "engine": "PutsEngine", "score": p.get("score", 0)})
    for m in moon_picks:
        all_picks.append({"symbol": m["symbol"], "engine": "Moonshot", "score": m.get("score", 0)})
    
    if not all_picks:
        logger.warning("No picks to chart")
        return None
    
    n = len(all_picks)
    
    # Create figure with subplots: 2 rows per ticker (price + RSI)
    fig, axes = plt.subplots(n * 2, 1, figsize=(14, 4 * n), 
                              gridspec_kw={'height_ratios': [3, 1] * n})
    
    if n == 1:
        axes = [axes] if not isinstance(axes, list) else axes
    
    fig.suptitle(
        f"🏛️ META ENGINE — Technical Analysis Dashboard\n"
        f"{datetime.now().strftime('%B %d, %Y %I:%M %p ET')}",
        fontsize=16, fontweight='bold', y=0.98
    )
    
    # Style
    plt.style.use('dark_background')
    fig.patch.set_facecolor('#1a1a2e')
    
    colors = {
        "PutsEngine": {"main": "#ff6b6b", "fill": "#ff6b6b33", "label": "🔴 PUT"},
        "Moonshot": {"main": "#4ecdc4", "fill": "#4ecdc433", "label": "🟢 MOON"},
    }
    
    for i, pick in enumerate(all_picks):
        symbol = pick["symbol"]
        engine = pick["engine"]
        score = pick["score"]
        color_set = colors.get(engine, colors["PutsEngine"])
        
        ax_price = axes[i * 2]
        ax_rsi = axes[i * 2 + 1]
        
        # Fetch price data
        bars = _fetch_price_history(symbol, polygon_api_key, days=30)
        
        if not bars:
            ax_price.text(0.5, 0.5, f"{symbol} — No price data available",
                         transform=ax_price.transAxes, ha='center', va='center',
                         fontsize=14, color='white')
            ax_price.set_facecolor('#16213e')
            ax_rsi.set_facecolor('#16213e')
            continue
        
        # Extract data
        dates = [datetime.fromtimestamp(b["t"] / 1000) for b in bars]
        closes = [b["c"] for b in bars]
        highs = [b["h"] for b in bars]
        lows = [b["l"] for b in bars]
        volumes = [b["v"] for b in bars]
        
        # Price chart
        ax_price.set_facecolor('#16213e')
        ax_price.plot(dates, closes, color=color_set["main"], linewidth=2, label=f'{symbol}')
        ax_price.fill_between(dates, lows, highs, alpha=0.15, color=color_set["main"])
        
        # Add SMA20
        if len(closes) >= 20:
            sma20 = [sum(closes[max(0, j-19):j+1]) / min(j+1, 20) for j in range(len(closes))]
            ax_price.plot(dates, sma20, color='#ffd93d', linewidth=1, alpha=0.7, 
                         linestyle='--', label='SMA20')
        
        # Volume as bar chart (secondary y-axis)
        ax_vol = ax_price.twinx()
        max_vol = max(volumes) if volumes else 1
        normalized_vol = [v / max_vol * max(closes) * 0.2 for v in volumes]
        vol_colors = ['#4ecdc466' if closes[j] >= closes[max(0, j-1)] else '#ff6b6b66' 
                      for j in range(len(closes))]
        ax_vol.bar(dates, normalized_vol, width=0.8, color=vol_colors, alpha=0.4)
        ax_vol.set_ylim(0, max(closes) * 0.8)
        ax_vol.set_yticks([])
        
        # Labels
        last_price = closes[-1] if closes else 0
        change = ((closes[-1] - closes[0]) / closes[0] * 100) if len(closes) >= 2 and closes[0] > 0 else 0
        
        ax_price.set_title(
            f"{color_set['label']} {symbol}  —  ${last_price:.2f}  "
            f"({'+'if change >= 0 else ''}{change:.1f}%)  —  "
            f"Score: {score:.2f}",
            fontsize=13, fontweight='bold', color=color_set["main"],
            loc='left'
        )
        ax_price.legend(loc='upper left', fontsize=9)
        ax_price.grid(True, alpha=0.2)
        ax_price.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        
        # RSI subplot
        rsi_values = _calculate_rsi(closes)
        ax_rsi.set_facecolor('#16213e')
        ax_rsi.plot(dates[-len(rsi_values):], rsi_values, color=color_set["main"], linewidth=1.5)
        ax_rsi.axhline(y=70, color='#ff6b6b', linestyle='--', alpha=0.5, linewidth=0.8)
        ax_rsi.axhline(y=30, color='#4ecdc4', linestyle='--', alpha=0.5, linewidth=0.8)
        ax_rsi.axhline(y=50, color='white', linestyle=':', alpha=0.2, linewidth=0.5)
        ax_rsi.fill_between(dates[-len(rsi_values):], 30, rsi_values,
                           where=[r < 30 for r in rsi_values],
                           color='#4ecdc4', alpha=0.2)
        ax_rsi.fill_between(dates[-len(rsi_values):], 70, rsi_values,
                           where=[r > 70 for r in rsi_values],
                           color='#ff6b6b', alpha=0.2)
        ax_rsi.set_ylim(0, 100)
        ax_rsi.set_ylabel('RSI', fontsize=9, color='white')
        ax_rsi.grid(True, alpha=0.15)
        
        # Add current RSI value annotation
        current_rsi = rsi_values[-1] if rsi_values else 50
        rsi_color = '#ff6b6b' if current_rsi > 70 else '#4ecdc4' if current_rsi < 30 else 'white'
        ax_rsi.text(0.98, 0.85, f'RSI: {current_rsi:.1f}',
                   transform=ax_rsi.transAxes, ha='right', va='top',
                   fontsize=10, color=rsi_color, fontweight='bold',
                   bbox=dict(boxstyle='round,pad=0.3', facecolor='#16213e', 
                            edgecolor=rsi_color, alpha=0.8))
    
    plt.tight_layout(rect=[0, 0.02, 1, 0.96])
    
    # Save chart
    if not output_dir:
        output_dir = str(Path(__file__).parent.parent / "output")
    
    os.makedirs(output_dir, exist_ok=True)
    filename = f"meta_engine_chart_{datetime.now().strftime('%Y%m%d_%H%M')}.png"
    filepath = os.path.join(output_dir, filename)
    
    fig.savefig(filepath, dpi=150, bbox_inches='tight', facecolor=fig.get_facecolor())
    plt.close(fig)
    
    logger.info(f"📊 Chart saved: {filepath}")
    return filepath
