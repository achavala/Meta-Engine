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
    price = pick.get("price", 0)
    score = pick.get("score", 0)
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
            cross_score = cross_analysis.get("bullish_score", 0)
            mws_score = cross_analysis.get("mws_score", 0)
            cross_signals = cross_analysis.get("signals", [])
            n_cross_sigs = len(cross_signals) if isinstance(cross_signals, list) else 0
            uw_sentiment = cross_analysis.get("uw_sentiment", "")

            if opp_level == "HIGH":
                if "MWS" in data_source:
                    exp_range = cross_analysis.get("expected_range", [])
                    range_str = f", expected range ${exp_range[0]:.2f}–${exp_range[1]:.2f}" if len(exp_range) >= 2 else ""
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
            cross_score = cross_analysis.get("bearish_score", 0)
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
            "puts_score": item.get("score", 0),
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
            "moonshot_score": item.get("score", 0),
            "puts_risk": item.get("puts_analysis", {}).get("risk_level", "N/A"),
        })
    
    # 3. Conflict summaries (tickers in both Top 10s)
    for entry in cross_results.get("conflict_matrix", []):
        if entry.get("in_puts_top10") and entry.get("in_moonshot_top10"):
            symbol = entry["symbol"]
            summaries["conflict_summaries"].append({
                "symbol": symbol,
                "summary": (
                    f"⚡ {symbol} appears in BOTH engine Top 10 lists — "
                    f"PutsEngine bearish score {entry['puts_score']:.2f} vs "
                    f"Moonshot bullish score {entry['moonshot_score']:.2f}. "
                    f"This conflict signals extreme volatility; institutional "
                    f"microstructure is divided. Trade with caution and reduced size."
                ),
            })
    
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
    """Format summaries for Telegram message (plain text, max 4096 chars)."""
    lines = []
    lines.append("🏛️ *META ENGINE DAILY REPORT*")
    lines.append(f"_{datetime.now().strftime('%B %d, %Y — %I:%M %p ET')}_")
    lines.append("")
    
    # Final summary
    lines.append(summaries.get("final_summary", ""))
    lines.append("")
    
    # Top puts picks
    lines.append("🔴 *TOP PUT PICKS:*")
    for i, p in enumerate(summaries.get("puts_picks_summaries", [])[:5], 1):
        lines.append(f"{i}. *{p['symbol']}* (Score: {p['puts_score']:.2f})")
        # Truncate summary for Telegram
        short = p["summary"][:200] + "..." if len(p["summary"]) > 200 else p["summary"]
        lines.append(f"   {short}")
        lines.append("")
    
    # Top moonshot picks
    lines.append("🟢 *TOP MOONSHOT PICKS:*")
    for i, m in enumerate(summaries.get("moonshot_picks_summaries", [])[:5], 1):
        lines.append(f"{i}. *{m['symbol']}* (Score: {m['moonshot_score']:.2f})")
        short = m["summary"][:200] + "..." if len(m["summary"]) > 200 else m["summary"]
        lines.append(f"   {short}")
        lines.append("")
    
    # Conflicts
    conflicts = summaries.get("conflict_summaries", [])
    if conflicts:
        lines.append("⚡ *CONFLICT ZONES:*")
        for c in conflicts:
            lines.append(f"• {c['summary']}")
        lines.append("")
    
    lines.append("_⚠️ Not financial advice. Options involve substantial risk._")
    
    text = "\n".join(lines)
    
    # Telegram message limit is 4096 chars
    if len(text) > 4090:
        text = text[:4087] + "..."
    
    return text


def format_summaries_for_x(summaries: Dict[str, Any]) -> List[str]:
    """
    Format summaries for X/Twitter posts (280 chars per tweet).
    Returns a list of tweets (thread).
    """
    tweets = []
    
    # Tweet 1: Header
    now = datetime.now().strftime('%b %d')
    final = summaries.get("final_summary", "")
    n_puts = len(summaries.get("puts_picks_summaries", []))
    n_moon = len(summaries.get("moonshot_picks_summaries", []))
    
    tweet1 = (
        f"🏛️ Meta Engine Daily ({now})\n\n"
        f"🔴 {n_puts} PUT candidates\n"
        f"🟢 {n_moon} MOONSHOT candidates\n\n"
        f"Cross-engine analysis complete. Thread 🧵👇"
    )
    tweets.append(tweet1)
    
    # Tweet 2-3: Top puts
    for i, p in enumerate(summaries.get("puts_picks_summaries", [])[:3], 1):
        tweet = f"🔴 PUT #{i}: ${p['symbol']} (Score: {p['puts_score']:.2f})\n\n"
        # Fit summary into remaining chars
        remaining = 280 - len(tweet) - 20
        short = p["summary"][:remaining]
        if len(p["summary"]) > remaining:
            short = short[:short.rfind(' ')] + "..."
        tweet += short
        tweets.append(tweet)
    
    # Tweet 4-5: Top moonshots
    for i, m in enumerate(summaries.get("moonshot_picks_summaries", [])[:3], 1):
        tweet = f"🟢 MOONSHOT #{i}: ${m['symbol']} (Score: {m['moonshot_score']:.2f})\n\n"
        remaining = 280 - len(tweet) - 20
        short = m["summary"][:remaining]
        if len(m["summary"]) > remaining:
            short = short[:short.rfind(' ')] + "..."
        tweet += short
        tweets.append(tweet)
    
    # Final tweet: disclaimer
    tweets.append(
        "⚠️ Disclaimer: This is algorithmic signal analysis, not financial advice. "
        "Options trading involves substantial risk. Past performance ≠ future results.\n\n"
        "#Trading #Options #MetaEngine"
    )
    
    return tweets
