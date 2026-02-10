"""
3-Sentence Summary Generator
=============================
Generates concise institutional-grade 3-sentence summaries for each pick
from the cross-engine analysis output.

Each summary contains:
  Sentence 1: Current position and key metric (price, score, direction)
  Sentence 2: Engine signal interpretation (what the data says)
  Sentence 3: Actionable outlook (1-2 day expectation with risk)
"""

from datetime import datetime
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


def _sf(val, default: float = 0.0) -> float:
    """Safely convert any value to float (handles str, None, etc.)."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _classify_puts_signal(score: float, signals: list) -> str:
    """Classify PutsEngine signal strength."""
    if score >= 0.68:
        return "CLASS A institutional-grade distribution"
    elif score >= 0.55:
        return "strong distribution with multi-layer confirmation"
    elif score >= 0.40:
        return "moderate bearish pressure with partial signal convergence"
    elif score >= 0.25:
        return "early-stage distribution signals"
    else:
        return "minimal bearish indicators"


def _classify_moonshot_signal(score: float, signals: list) -> str:
    """Classify Moonshot signal strength."""
    if score >= 0.80:
        return "explosive moonshot setup with multiple catalyst alignment"
    elif score >= 0.60:
        return "strong upside asymmetry with institutional flow support"
    elif score >= 0.40:
        return "moderate breakout potential with partial squeeze setup"
    elif score >= 0.20:
        return "early momentum signals requiring confirmation"
    else:
        return "no significant moonshot catalysts"


def generate_pick_summary(pick: Dict[str, Any], cross_analysis: Dict[str, Any] = None) -> str:
    """
    Generate a 3-sentence institutional summary for a single pick.
    
    Args:
        pick: Pick data from either engine
        cross_analysis: Optional cross-engine analysis result
        
    Returns:
        3-sentence summary string
    """
    symbol = pick.get("symbol", "???")
    price = _sf(pick.get("price", 0))
    score = _sf(pick.get("score", 0))
    source = pick.get("engine", "Unknown")
    signals = pick.get("signals", [])
    
    # Sentence 1: Current position
    if "Puts" in source:
        signal_desc = _classify_puts_signal(score, signals)
        sentence1 = (
            f"{symbol} at ${price:.2f} shows {signal_desc} "
            f"(PutsEngine score: {score:.2f}), with "
            f"{len(signals)} confirming bearish signals active."
        )
    else:
        signal_desc = _classify_moonshot_signal(score, signals)
        sentence1 = (
            f"{symbol} at ${price:.2f} presents {signal_desc} "
            f"(Moonshot score: {score:.2f}), with "
            f"{len(signals)} bullish catalyst signals detected."
        )
    
    # Sentence 2: Cross-engine interpretation
    if cross_analysis:
        cross_engine = cross_analysis.get("engine", "")
        if "Puts" in source:
            # PutsEngine pick checked by Moonshot — use rich data if available
            opp_level = cross_analysis.get("opportunity_level", "LOW")
            data_source = cross_analysis.get("data_source", "")
            cross_score = _sf(cross_analysis.get("bullish_score", 0))
            mws_score = _sf(cross_analysis.get("mws_score", 0))
            cross_signals = cross_analysis.get("signals", [])
            n_cross_sigs = len(cross_signals) if isinstance(cross_signals, list) else 0
            uw_sentiment = cross_analysis.get("uw_sentiment", "")

            if opp_level == "HIGH":
                if "MWS" in data_source:
                    exp_range = cross_analysis.get("expected_range", [])
                    range_str = f", expected range ${_sf(exp_range[0]):.2f}–${_sf(exp_range[1]):.2f}" if len(exp_range) >= 2 else ""
                    sentence2 = (
                        f"Cross-engine CONFLICT: MWS 7-layer analysis detects HIGH bullish "
                        f"signal (MWS {mws_score:.0f}/100, prob {cross_analysis.get('bullish_probability', 0)}%{range_str}) — "
                        f"volatile tug-of-war between distribution sellers and institutional "
                        f"buyers, expect wide intraday ranges."
                    )
                elif "Recommendations" in data_source:
                    sentence2 = (
                        f"Cross-engine CONFLICT: TradeNova recommends this as a BUY "
                        f"(composite {cross_score*100:.0f}/100, UW sentiment: {uw_sentiment}, "
                        f"{n_cross_sigs} bullish signals) — distribution sellers face active "
                        f"buying resistance from momentum/catalyst engines."
                    )
                else:
                    sentence2 = (
                        f"Cross-engine CONFLICT: Moonshot Engine detects HIGH bullish signals "
                        f"(score {cross_score:.2f}, {n_cross_sigs} signals) — volatile tug-of-war "
                        f"between distribution sellers and momentum buyers, expect wide ranges."
                    )
            elif opp_level == "MODERATE":
                if "MWS" in data_source:
                    sentence2 = (
                        f"Moonshot MWS shows MODERATE upside (MWS {mws_score:.0f}/100, "
                        f"prob {cross_analysis.get('bullish_probability', 0)}%), suggesting "
                        f"the bearish distribution may face buying resistance — put thesis "
                        f"intact but monitor for failed breakdown if buyers defend support."
                    )
                elif "Recommendations" in data_source:
                    sentence2 = (
                        f"TradeNova shows MODERATE interest (composite {cross_score*100:.0f}/100, "
                        f"UW: {uw_sentiment}) — some institutional buying present, the put "
                        f"thesis is valid but reduce size; monitor for failed breakdown."
                    )
                else:
                    sentence2 = (
                        f"Moonshot Engine shows moderate upside ({cross_score:.2f}, "
                        f"{n_cross_sigs} signals) — bearish distribution may face buying "
                        f"resistance; put thesis intact but monitor for failed breakdown."
                    )
            else:
                if "MWS" in data_source:
                    sentence2 = (
                        f"MWS 7-layer confirms LOW bullish signal ({mws_score:.0f}/100, "
                        f"prob {cross_analysis.get('bullish_probability', 0)}%), reinforcing "
                        f"the PutsEngine bearish thesis — path of least resistance is "
                        f"to the downside with limited institutional buying interest."
                    )
                elif "Standalone" in data_source:
                    top_sigs = ", ".join(cross_signals[:3]) if cross_signals else "none"
                    sentence2 = (
                        f"Standalone Moonshot analysis confirms LOW bullish signal "
                        f"(score {cross_score:.2f}, {n_cross_sigs} signals: {top_sigs}), "
                        f"reinforcing the bearish thesis — no squeeze setup, no momentum "
                        f"catalyst, path of least resistance is to the downside."
                    )
                else:
                    sentence2 = (
                        f"Moonshot Engine confirms no significant bullish counter-signal "
                        f"(score {cross_score:.2f}), reinforcing the PutsEngine bearish "
                        f"thesis — the path of least resistance is to the downside "
                        f"with limited buying interest."
                    )
        else:
            # Moonshot pick checked by PutsEngine
            risk_level = cross_analysis.get("risk_level", "LOW")
            cross_score = _sf(cross_analysis.get("bearish_score", 0))
            cross_signals = cross_analysis.get("signals", [])
            n_sigs = len(cross_signals) if isinstance(cross_signals, list) else 0
            top_sigs_str = ", ".join(cross_signals[:3]) if cross_signals else ""

            if risk_level == "HIGH":
                sentence2 = (
                    f"Critical warning: PutsEngine detects HIGH distribution risk "
                    f"(score {cross_score:.2f}, {n_sigs} signals: {top_sigs_str}) — "
                    f"institutional sellers may be distributing into the bullish "
                    f"momentum, creating a potential bull trap."
                )
            elif risk_level == "MODERATE":
                sentence2 = (
                    f"PutsEngine flags moderate bearish risk (score {cross_score:.2f}, "
                    f"{n_sigs} signals: {top_sigs_str}) — some distribution activity "
                    f"detected; moonshot thesis valid but reduce size and tighten stops."
                )
            else:
                if n_sigs > 0 and top_sigs_str:
                    sentence2 = (
                        f"PutsEngine confirms low bearish risk (score {cross_score:.2f}, "
                        f"{n_sigs} minor signals: {top_sigs_str}) — no material "
                        f"distribution detected, giving the upside momentum a clean runway."
                    )
                else:
                    sentence2 = (
                        f"PutsEngine confirms low bearish risk (score {cross_score:.2f}) — "
                        f"no institutional distribution detected across 30-day analysis, "
                        f"giving the upside momentum a clean runway."
                    )
    else:
        sentence2 = (
            f"Key signals include: {', '.join(signals[:3]) if signals else 'broad market factors'} "
            f"— institutional flow analysis suggests {'sellers are actively distributing' if 'Puts' in source else 'buyers are accumulating asymmetric positions'}."
        )
    
    # Sentence 3: Actionable outlook
    if "Puts" in source:
        if score >= 0.55:
            sentence3 = (
                f"1-2 day outlook: expect -3% to -8% downside pressure with "
                f"{'high' if score >= 0.68 else 'moderate'} conviction — "
                f"optimal entry on any relief bounce with 7-14 DTE puts targeting "
                f"10% OTM strikes for maximum asymmetric payoff."
            )
        elif score >= 0.35:
            sentence3 = (
                f"1-2 day outlook: expect -1% to -4% drift lower — monitor for "
                f"acceleration signals before committing capital, this is a watch-list "
                f"candidate with developing but not yet confirmed distribution."
            )
        else:
            sentence3 = (
                f"1-2 day outlook: neutral to slightly bearish bias — insufficient "
                f"signal strength for immediate action, continue monitoring for "
                f"signal convergence before establishing positions."
            )
    else:
        if score >= 0.60:
            sentence3 = (
                f"1-2 day outlook: expect +3% to +10% upside potential with "
                f"{'high' if score >= 0.80 else 'moderate'} conviction — "
                f"position as lottery ticket with 7-14 DTE OTM calls, accept "
                f"100% loss possibility for 5x-10x asymmetric upside."
            )
        elif score >= 0.40:
            sentence3 = (
                f"1-2 day outlook: expect +1% to +5% upside if momentum holds — "
                f"wait for volume confirmation above key resistance before entering, "
                f"keep position size small as this is still a developing setup."
            )
        else:
            sentence3 = (
                f"1-2 day outlook: neutral — insufficient momentum signals for "
                f"a moonshot play, monitor for squeeze trigger events or unusual "
                f"options activity that could change the thesis."
            )
    
    return f"{sentence1} {sentence2} {sentence3}"


def _build_conflict_resolution(
    symbol: str,
    puts_score: float,
    moon_score: float,
    cross_results: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build institutional-grade conflict resolution for a ticker appearing in
    both PutsEngine and Moonshot Top 10.
    
    Uses MWS sensor data, market data, and cross-analysis signals to determine:
    1. WHY both engines flagged this ticker (not just that they did)
    2. Which thesis is likely dominant in the 1-2 day timeframe
    3. Specific trading recommendations for both sides
    """
    # Ensure scores are numeric
    puts_score = _sf(puts_score)
    moon_score = _sf(moon_score)
    # Find the pick in both cross-analysis lists
    puts_pick = None
    moon_pick = None
    
    for p in cross_results.get("puts_through_moonshot", []):
        if p["symbol"] == symbol:
            puts_pick = p
            break
    
    for m in cross_results.get("moonshot_through_puts", []):
        if m["symbol"] == symbol:
            moon_pick = m
            break
    
    # Extract key data points (safe-cast everything to float)
    puts_signals = puts_pick.get("signals", []) if puts_pick else []
    puts_engine_type = puts_pick.get("engine_type", "") if puts_pick else ""
    moon_signals = moon_pick.get("signals", []) if moon_pick else []
    puts_price = _sf(puts_pick.get("price", 0)) if puts_pick else 0.0
    moon_price = _sf(moon_pick.get("price", 0)) if moon_pick else 0.0
    
    # MWS sensor data from puts cross-analysis
    moonshot_cross = puts_pick.get("moonshot_analysis", {}) if puts_pick else {}
    mws_score = _sf(moonshot_cross.get("mws_score", 0))
    sensors = moonshot_cross.get("sensors", [])
    raw_range = moonshot_cross.get("expected_range", [])
    expected_range = [_sf(v) for v in raw_range] if raw_range else []
    bullish_prob = _sf(moonshot_cross.get("bullish_probability", 0))
    data_source = moonshot_cross.get("data_source", "")
    
    # PutsEngine cross-analysis from moonshot side
    puts_cross = moon_pick.get("puts_analysis", {}) if moon_pick else {}
    puts_risk = puts_cross.get("risk_level", "N/A")
    puts_bearish_score = _sf(puts_cross.get("bearish_score", 0))
    
    # Market data (use whichever pick has it)
    mkt = (puts_pick or moon_pick or {}).get("market_data", {})
    current_price = _sf(mkt.get("price", moon_price or puts_price))
    change_pct = _sf(mkt.get("change_pct", 0))
    rsi = _sf(mkt.get("rsi", 50))
    rvol = _sf(mkt.get("rvol", 1.0))
    
    # Analyze daily bars for recent price action
    daily_bars = mkt.get("daily_bars", [])
    
    # Calculate recent move magnitude (last 5 bars)
    recent_move_pct = 0.0
    if len(daily_bars) >= 5:
        close_5_ago = _sf(daily_bars[-5].get("c", 0))
        latest_close = _sf(daily_bars[-1].get("c", 0))
        if close_5_ago > 0:
            recent_move_pct = ((latest_close - close_5_ago) / close_5_ago) * 100
    
    # Identify conflict pattern
    puts_signals_str = " ".join(str(s).lower() for s in puts_signals)
    moon_signals_str = " ".join(str(s).lower() for s in moon_signals)
    
    # Determine conflict type
    if "pump_reversal" in puts_signals_str:
        conflict_type = "Distribution Into Strength"
        conflict_desc = (
            f"PutsEngine detects a pump-reversal pattern (institutional selling "
            f"into momentum rally), while Moonshot sees genuine momentum continuation. "
            f"This is a classic 'smart money distribution into retail strength' setup."
        )
    elif "exhaustion" in puts_signals_str:
        conflict_type = "Exhaustion vs Momentum"
        conflict_desc = (
            f"PutsEngine detects exhaustion signals (overextended move losing steam), "
            f"while Moonshot sees continued momentum/catalyst drivers. "
            f"The key question: is the momentum accelerating or decelerating?"
        )
    elif "gamma_drain" in puts_engine_type:
        conflict_type = "Gamma Squeeze Unwind"
        conflict_desc = (
            f"PutsEngine detects gamma drain (options-driven reversal risk), "
            f"while Moonshot sees options flow as bullish catalyst. "
            f"This conflict often resolves violently — expect whipsaw action."
        )
    else:
        conflict_type = "Directional Divergence"
        conflict_desc = (
            f"PutsEngine and Moonshot have fundamentally opposing views on direction. "
            f"Market microstructure is divided between distribution and accumulation."
        )
    
    # Extract sensor insights if MWS data available
    sensor_summary = ""
    bullish_sensors = 0
    bearish_sensors = 0
    if sensors:
        for s in sensors:
            sig = s.get("signal", "neutral")
            if sig == "bullish":
                bullish_sensors += 1
            elif sig == "bearish":
                bearish_sensors += 1
        
        # Key sensor highlights
        key_sensors = []
        for s in sensors:
            name = s.get("name", "").replace("\U0001f3db\ufe0f", "").replace("\U0001f30a", "").replace("\u26a1", "").replace("\U0001f300", "").replace("\U0001f4ca", "").replace("\U0001f9e0", "").replace("\U0001f32a\ufe0f", "").strip()
            score_val = s.get("score", 0)
            sig = s.get("signal", "neutral")
            key_sensors.append(f"{name} {score_val}/100 ({sig})")
        sensor_summary = " | ".join(key_sensors[:4])
    
    # Determine dominant thesis for 1-2 day timeframe
    # Scoring: higher puts_score with recent big move = puts wins (mean reversion)
    # Higher moon_score with strong technicals = moon wins (continuation)
    puts_conviction = puts_score
    moon_conviction = moon_score
    
    # Adjust conviction based on MWS sensors
    if mws_score > 60:
        moon_conviction += 0.15  # MWS bullish supports moonshot
    elif mws_score < 40:
        puts_conviction += 0.15  # MWS bearish supports puts
    
    # Adjust for recent move magnitude
    if abs(recent_move_pct) > 15:
        puts_conviction += 0.10  # Extreme recent move favors mean reversion
    
    # Adjust for RSI
    if rsi > 70:
        puts_conviction += 0.10  # Overbought favors puts
    elif rsi < 30:
        moon_conviction += 0.10  # Oversold favors moonshot bounce
    
    # Determine which thesis wins
    if puts_conviction > moon_conviction + 0.15:
        dominant = "BEARISH"
        dominant_detail = (
            f"PutsEngine thesis likely dominant in 1-2 day timeframe "
            f"(conviction {puts_conviction:.2f} vs {moon_conviction:.2f}). "
            f"The distribution pattern combined with {'overbought RSI ' if rsi > 65 else ''}"
            f"{'extreme recent move (+{:.0f}%) '.format(recent_move_pct) if recent_move_pct > 10 else ''}"
            f"favors mean reversion. Moonshot momentum may provide one more leg up "
            f"before reversal — ideal for put entry on the next spike."
        )
    elif moon_conviction > puts_conviction + 0.15:
        dominant = "BULLISH"
        dominant_detail = (
            f"Moonshot thesis likely dominant in 1-2 day timeframe "
            f"(conviction {moon_conviction:.2f} vs {puts_conviction:.2f}). "
            f"Technical momentum and institutional flow outweigh the distribution signals. "
            f"However, PutsEngine distribution warning suggests this rally has a shelf life — "
            f"take profits aggressively and don't hold overnight without stops."
        )
    else:
        dominant = "NEUTRAL — AVOID"
        dominant_detail = (
            f"Neither thesis has clear dominance (puts {puts_conviction:.2f} vs "
            f"moon {moon_conviction:.2f}). This is a high-risk coin flip with "
            f"institutional flow pulling in both directions. Experienced traders may "
            f"play the expected range with iron condors; directional traders should WAIT "
            f"for resolution before entering."
        )
    
    # Build range info
    range_str = ""
    if expected_range and len(expected_range) >= 2:
        range_str = f" Expected 1-2 day range: ${expected_range[0]:.2f}–${expected_range[1]:.2f}."
    
    # Build the full conflict summary
    summary = (
        f"⚡ CONFLICT — {symbol} ({conflict_type}): {conflict_desc} "
        f"PutsEngine bearish score {puts_score:.2f} ({len(puts_signals)} signals: "
        f"{', '.join(str(s) for s in puts_signals[:3])}) vs Moonshot bullish score "
        f"{moon_score:.2f} ({len(moon_signals)} signals: "
        f"{', '.join(str(s) for s in moon_signals[:3])}). "
        f"{'MWS 7-Layer: ' + sensor_summary + '. ' if sensor_summary else ''}"
        f"Dominant thesis: {dominant}. {dominant_detail}{range_str}"
    )
    
    # Build trading recommendations for both sides (safe-cast all numerics)
    entry_low = _sf(moon_pick.get("entry_low", 0)) if moon_pick else 0.0
    entry_high = _sf(moon_pick.get("entry_high", 0)) if moon_pick else 0.0
    target = _sf(moon_pick.get("target", 0)) if moon_pick else 0.0
    stop = _sf(moon_pick.get("stop", 0)) if moon_pick else 0.0
    
    recommendations = {
        "if_bullish": (
            f"CALL thesis: Entry ${entry_low:.2f}–${entry_high:.2f}, "
            f"target ${target:.2f} (+{((target/current_price - 1)*100) if current_price > 0 and target > 0 else 0:.0f}%), "
            f"stop ${stop:.2f}. Reduce size by 50% due to distribution risk. "
            f"Use 7 DTE max, take profits at first sign of volume exhaustion."
        ) if entry_low else "Entry data unavailable — wait for confirmation.",
        "if_bearish": (
            f"PUT thesis: Entry on spike above ${current_price:.2f}, target "
            f"{'${:.2f}'.format(expected_range[0]) if expected_range else 'support level'}, "
            f"stop {'${:.2f}'.format(expected_range[1]) if expected_range and len(expected_range) >= 2 else 'above today high'}. "
            f"Score {puts_score:.2f} = high conviction, but wait for momentum "
            f"exhaustion confirmation (RSI divergence, volume decline)."
        ),
        "neutral_play": (
            f"IRON CONDOR/STRADDLE: Expected range "
            f"{'${:.2f}–${:.2f}'.format(expected_range[0], expected_range[1]) if expected_range and len(expected_range) >= 2 else 'wide'}. "
            f"Sell both sides of the conflict — let theta work while bulls and bears fight."
        ),
    }
    
    return {
        "symbol": symbol,
        "conflict_type": conflict_type,
        "puts_score": puts_score,
        "moon_score": moon_score,
        "mws_score": mws_score,
        "dominant_thesis": dominant,
        "current_price": current_price,
        "rsi": rsi,
        "recent_move_pct": recent_move_pct,
        "bullish_sensors": bullish_sensors,
        "bearish_sensors": bearish_sensors,
        "expected_range": expected_range,
        "recommendations": recommendations,
        "summary": summary,
    }


def generate_all_summaries(cross_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate 3-sentence summaries for all picks from cross-analysis results.
    
    Args:
        cross_results: Output from cross_analyzer.cross_analyze()
        
    Returns:
        Dict with summaries for each category
    """
    logger.info("📝 Generating 3-sentence summaries...")
    
    summaries = {
        "timestamp": datetime.now().isoformat(),
        "puts_picks_summaries": [],
        "moonshot_picks_summaries": [],
        "conflict_summaries": [],
        "final_summary": "",
    }
    
    # 1. PutsEngine picks (run through Moonshot)
    for item in cross_results.get("puts_through_moonshot", []):
        summary = generate_pick_summary(
            pick=item,
            cross_analysis=item.get("moonshot_analysis", {})
        )
        summaries["puts_picks_summaries"].append({
            "symbol": item["symbol"],
            "summary": summary,
            "puts_score": _sf(item.get("score", 0)),
            "moonshot_level": item.get("moonshot_analysis", {}).get("opportunity_level", "N/A"),
        })
    
    # 2. Moonshot picks (run through PutsEngine)
    for item in cross_results.get("moonshot_through_puts", []):
        summary = generate_pick_summary(
            pick=item,
            cross_analysis=item.get("puts_analysis", {})
        )
        summaries["moonshot_picks_summaries"].append({
            "symbol": item["symbol"],
            "summary": summary,
            "moonshot_score": _sf(item.get("score", 0)),
            "puts_risk": item.get("puts_analysis", {}).get("risk_level", "N/A"),
        })
    
    # 3. Conflict summaries (tickers in both Top 10s) — institutional-grade resolution
    for entry in cross_results.get("conflict_matrix", []):
        if entry.get("in_puts_top10") and entry.get("in_moonshot_top10"):
            symbol = entry["symbol"]
            conflict_detail = _build_conflict_resolution(
                symbol=symbol,
                puts_score=_sf(entry.get("puts_score", 0)),
                moon_score=_sf(entry.get("moonshot_score", 0)),
                cross_results=cross_results,
            )
            summaries["conflict_summaries"].append(conflict_detail)
    
    # 4. Final meta-summary
    n_puts = len(summaries["puts_picks_summaries"])
    n_moon = len(summaries["moonshot_picks_summaries"])
    n_conflicts = len(summaries["conflict_summaries"])
    
    # Count high-conviction picks
    high_puts = sum(1 for p in summaries["puts_picks_summaries"] if p["puts_score"] >= 0.55)
    high_moon = sum(1 for m in summaries["moonshot_picks_summaries"] if m["moonshot_score"] >= 0.60)
    
    if high_puts > high_moon:
        bias = "BEARISH"
        bias_detail = f"PutsEngine has {high_puts} high-conviction bearish picks vs {high_moon} strong moonshots"
    elif high_moon > high_puts:
        bias = "BULLISH"
        bias_detail = f"Moonshot has {high_moon} high-conviction bullish picks vs {high_puts} strong puts"
    else:
        bias = "NEUTRAL"
        bias_detail = f"balanced signal — {high_puts} strong puts vs {high_moon} strong moonshots"
    
    summaries["final_summary"] = (
        f"Meta Engine Daily Analysis ({datetime.now().strftime('%B %d, %Y')}): "
        f"Overall market bias is {bias} — {bias_detail}. "
        f"{n_puts} put candidates and {n_moon} moonshot candidates identified, "
        f"with {n_conflicts} tickers flagged in both engines (conflict zones). "
        f"Cross-engine validation {'strengthens' if n_conflicts == 0 else 'complicates'} "
        f"directional conviction for today's session."
    )
    
    logger.info(f"📝 Generated {n_puts + n_moon} summaries, {n_conflicts} conflicts")
    
    return summaries


def format_summaries_for_telegram(summaries: Dict[str, Any]) -> str:
    """
    Legacy format for Telegram.
    NOTE: Telegram now uses its own formatter in telegram_sender.py.
    This is kept for backward compatibility only.
    """
    lines = []
    lines.append("🏛️ META ENGINE DAILY REPORT")
    lines.append(f"{datetime.now().strftime('%B %d, %Y — %I:%M %p ET')}")
    lines.append("")
    lines.append(summaries.get("final_summary", ""))
    lines.append("")
    
    for i, p in enumerate(summaries.get("puts_picks_summaries", [])[:5], 1):
        lines.append(f"{i}. {p['symbol']} (Score: {p['puts_score']:.2f})")
        short = p["summary"][:200] + "..." if len(p["summary"]) > 200 else p["summary"]
        lines.append(f"   {short}")
        lines.append("")
    
    for i, m in enumerate(summaries.get("moonshot_picks_summaries", [])[:5], 1):
        lines.append(f"{i}. {m['symbol']} (Score: {m['moonshot_score']:.2f})")
        short = m["summary"][:200] + "..." if len(m["summary"]) > 200 else m["summary"]
        lines.append(f"   {short}")
        lines.append("")
    
    text = "\n".join(lines)
    if len(text) > 4090:
        text = text[:4087] + "..."
    return text
