"""
Meta Engine Report Generator
==============================
Generates the full daily markdown report from pipeline output data.
Also provides HTML rendering and PDF conversion for email attachments.
"""

import json
import glob
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)


def generate_md_report(
    puts_picks: List[Dict],
    moon_picks: List[Dict],
    cross_data: Dict[str, Any],
    summaries: Dict[str, Any],
    output_dir: str,
    date_str: str = None,
) -> str:
    """
    Generate the full Meta Engine markdown report.
    
    Returns:
        Path to the generated .md file
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")
    
    out = Path(output_dir)
    
    # Build cross maps
    ptm_list = cross_data.get("puts_through_moonshot", [])
    mtp_list = cross_data.get("moonshot_through_puts", [])
    ptm_map = {item["symbol"]: item for item in ptm_list}
    mtp_map = {item["symbol"]: item for item in mtp_list}

    lines = []
    lines.append("# 🏛️ Meta Engine Daily Report")
    lines.append(f"**Date:** {datetime.now().strftime('%B %d, %Y %I:%M %p ET')}")
    lines.append(f"**Run Type:** Scheduled (9:35 AM / 3:15 PM ET)")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Overall summary
    lines.append("## 📊 Overall Market Summary")
    lines.append("")
    lines.append(summaries.get("final_summary", "N/A"))
    lines.append("")

    # Puts Engine Top 10
    lines.append("## 🔴 PutsEngine Top 10 (Bearish Picks)")
    lines.append("")
    lines.append("| # | Ticker | Price | Score | Signals | Engine Type |")
    lines.append("|---|--------|-------|-------|---------|-------------|")
    for i, p in enumerate(puts_picks, 1):
        sym = p["symbol"]
        price = p.get("price", 0)
        score = p["score"]
        sig_count = len(p.get("signals", []))
        etype = p.get("engine_type", "N/A")
        lines.append(f"| {i} | **{sym}** | ${price:.2f} | {score:.3f} | {sig_count} | {etype} |")
    lines.append("")

    # Moonshot Engine Top 10
    lines.append("## 🟢 Moonshot Top 10 (Bullish/Catalyst Picks)")
    lines.append("")
    lines.append("| # | Ticker | Price | Score | Signals | Sentiment | Target | Source |")
    lines.append("|---|--------|-------|-------|---------|-----------|--------|--------|")
    for i, p in enumerate(moon_picks, 1):
        sym = p["symbol"]
        price = p.get("price", 0)
        score = p["score"]
        sig_count = len(p.get("signals", []))
        sentiment = p.get("uw_sentiment", "N/A")
        target = p.get("target", 0)
        src = p.get("source", p.get("engine", "N/A"))
        target_str = f"${target:.2f}" if target else "N/A"
        lines.append(f"| {i} | **{sym}** | ${price:.2f} | {score:.3f} | {sig_count} | {sentiment} | {target_str} | {src} |")
    lines.append("")

    # Cross Analysis
    lines.append("## 🔄 Cross-Engine Analysis")
    lines.append("")

    # Puts analyzed by Moonshot
    lines.append("### PutsEngine Picks Analyzed by MoonshotEngine")
    lines.append("")
    lines.append("| Ticker | Puts Score | Moonshot Opp | Bullish Score | Data Source | Conflict? |")
    lines.append("|--------|-----------|-------------|--------------|-------------|-----------|")
    for p in puts_picks:
        sym = p["symbol"]
        cx = ptm_map.get(sym, {})
        ms_analysis = cx.get("moonshot_analysis", {})
        opp = ms_analysis.get("opportunity_level", "N/A")
        ms_score = ms_analysis.get("bullish_score", ms_analysis.get("mws_score", 0))
        if not isinstance(ms_score, (int, float)):
            ms_score = 0
        ds = ms_analysis.get("data_source", "N/A")
        conflict = "⚠️ YES" if opp == "HIGH" else ("⚡ MIXED" if opp == "MODERATE" else "No")
        lines.append(f"| **{sym}** | {p['score']:.3f} | {opp} | {ms_score:.2f} | {ds} | {conflict} |")
    lines.append("")

    # Moonshot analyzed by PutsEngine
    lines.append("### Moonshot Picks Analyzed by PutsEngine")
    lines.append("")
    lines.append("| Ticker | Moon Score | Puts Risk | Bearish Score | Signals | Breakdown |")
    lines.append("|--------|-----------|----------|--------------|---------|-----------|")
    for p in moon_picks:
        sym = p["symbol"]
        cx = mtp_map.get(sym, {})
        puts_analysis = cx.get("puts_analysis", {})
        risk = puts_analysis.get("risk_level", "N/A")
        ps = puts_analysis.get("bearish_score", 0)
        if not isinstance(ps, (int, float)):
            ps = 0
        nsigs = len(puts_analysis.get("signals", []))
        bd = puts_analysis.get("sub_scores", {})
        if isinstance(bd, dict):
            bd_parts = []
            for k, v in bd.items():
                if isinstance(v, (int, float)):
                    bd_parts.append(f"{k}={v:.2f}")
            bd_str = " ".join(bd_parts)
        else:
            bd_str = str(bd)[:40]
        lines.append(f"| **{sym}** | {p['score']:.3f} | {risk} | {ps:.2f} | {nsigs} signals | {bd_str} |")
    lines.append("")

    # Conflict matrix
    lines.append("### ⚠️ Conflict Matrix")
    lines.append("")
    conflicts = cross_data.get("conflict_matrix", [])
    if isinstance(conflicts, list):
        both_engines = [c for c in conflicts if c.get("in_puts_top10") and c.get("in_moonshot_top10")]
        puts_only = [c for c in conflicts if c.get("in_puts_top10") and not c.get("in_moonshot_top10")]
        moon_only = [c for c in conflicts if not c.get("in_puts_top10") and c.get("in_moonshot_top10")]
        if both_engines:
            lines.append("**⚠️ CONFLICT (Both Engines):**")
            for c in both_engines:
                lines.append(f"- **{c['symbol']}**: {c.get('verdict', 'N/A')}")
        else:
            lines.append("**⚠️ CONFLICT (Both Engines):** None — no overlapping tickers")
        lines.append("")
        lines.append(f"**🔴 Bearish Only (PutsEngine):** {' | '.join(c['symbol'] for c in puts_only)}")
        lines.append(f"**🟢 Bullish Only (Moonshot):** {' | '.join(c['symbol'] for c in moon_only)}")
    elif isinstance(conflicts, dict):
        high_conf = [s for s, v in conflicts.items() if v.get("conflict_level") == "HIGH"]
        mod_conf = [s for s, v in conflicts.items() if v.get("conflict_level") == "MODERATE"]
        lines.append(f"**HIGH Conflicts:** {' | '.join(high_conf) if high_conf else 'None'}")
        lines.append(f"**MODERATE Conflicts:** {' | '.join(mod_conf) if mod_conf else 'None'}")
    lines.append("")

    # Summaries
    lines.append("## 📝 3-Sentence Institutional Summaries")
    lines.append("")

    # PutsEngine pick summaries
    lines.append("### 🔴 Bearish Picks (PutsEngine)")
    lines.append("")
    for s in summaries.get("puts_picks_summaries", []):
        sym = s["symbol"]
        lines.append(f"**{sym}** (Puts: {s.get('puts_score', 'N/A')} | Moonshot: {s.get('moonshot_level', 'N/A')})")
        lines.append("")
        lines.append(f"> {s.get('summary', 'N/A')}")
        lines.append("")

    # Moonshot pick summaries
    lines.append("### 🟢 Bullish Picks (Moonshot)")
    lines.append("")
    for s in summaries.get("moonshot_picks_summaries", []):
        sym = s["symbol"]
        ms_score = s.get("moonshot_score", "N/A")
        if isinstance(ms_score, float):
            ms_score = f"{ms_score:.2f}"
        lines.append(f"**{sym}** (Moonshot: {ms_score} | Puts Risk: {s.get('puts_risk', 'N/A')})")
        lines.append("")
        lines.append(f"> {s.get('summary', 'N/A')}")
        lines.append("")

    # Conflict summaries — DETAILED institutional resolution
    conflict_sums = summaries.get("conflict_summaries", [])
    if conflict_sums:
        lines.append("### ⚡ Conflict Zone — Institutional Resolution")
        lines.append("")
        for s in conflict_sums:
            sym = s.get("symbol", "???")
            c_type = s.get("conflict_type", "Directional Divergence")
            dominant = s.get("dominant_thesis", "N/A")
            puts_sc = s.get("puts_score", 0)
            moon_sc = s.get("moon_score", 0)
            mws_sc = s.get("mws_score", 0)
            cur_price = s.get("current_price", 0)
            rsi_val = s.get("rsi", 0)
            move_pct = s.get("recent_move_pct", 0)
            bull_sens = s.get("bullish_sensors", 0)
            bear_sens = s.get("bearish_sensors", 0)
            exp_range = s.get("expected_range", [])
            recs = s.get("recommendations", {})
            
            lines.append(f"#### ⚡ {sym} — {c_type}")
            lines.append("")
            lines.append(f"| Metric | PutsEngine (Bear) | Moonshot (Bull) |")
            lines.append(f"|--------|-------------------|-----------------|")
            lines.append(f"| Score | {puts_sc:.2f} | {moon_sc:.2f} |")
            lines.append(f"| Current Price | ${cur_price:.2f} | ${cur_price:.2f} |")
            lines.append(f"| RSI | {'Overbought' if rsi_val > 65 else 'Neutral' if rsi_val > 35 else 'Oversold'} ({rsi_val:.1f}) | {'Overbought' if rsi_val > 65 else 'Neutral' if rsi_val > 35 else 'Oversold'} ({rsi_val:.1f}) |")
            if mws_sc > 0:
                lines.append(f"| MWS Score | — | {mws_sc:.0f}/100 |")
                lines.append(f"| MWS Sensors | {bear_sens} bearish | {bull_sens} bullish |")
            if move_pct != 0:
                lines.append(f"| 5-Day Move | {move_pct:+.1f}% | {move_pct:+.1f}% |")
            if exp_range and len(exp_range) >= 2:
                lines.append(f"| Expected Range | ${exp_range[0]:.2f}–${exp_range[1]:.2f} | ${exp_range[0]:.2f}–${exp_range[1]:.2f} |")
            lines.append(f"| **Dominant Thesis** | **{dominant}** | **{dominant}** |")
            lines.append("")
            
            # Full narrative summary
            lines.append(f"> {s.get('summary', 'N/A')}")
            lines.append("")
            
            # Trading recommendations for each thesis
            if recs:
                lines.append("**Trading Recommendations:**")
                lines.append("")
                if recs.get("if_bullish"):
                    lines.append(f"- 🟢 **If Bullish:** {recs['if_bullish']}")
                if recs.get("if_bearish"):
                    lines.append(f"- 🔴 **If Bearish:** {recs['if_bearish']}")
                if recs.get("neutral_play"):
                    lines.append(f"- ⚪ **Neutral/Theta:** {recs['neutral_play']}")
                lines.append("")

    # Generated files
    lines.append("## 📁 Generated Files")
    lines.append("")
    chart_files = glob.glob(str(out / f"meta_engine_chart_{date_str}*.png"))
    lines.append(f"- `puts_top10_{date_str}.json` — PutsEngine top 10 picks")
    lines.append(f"- `moonshot_top10_{date_str}.json` — Moonshot top 10 picks")
    lines.append(f"- `cross_analysis_{date_str}.json` — Full cross-engine analysis")
    lines.append(f"- `summaries_{date_str}.json` — 3-sentence summaries")
    for cf in chart_files:
        lines.append(f"- `{Path(cf).name}` — Technical analysis chart")
    lines.append("")
    lines.append("---")
    lines.append("*Generated by Meta Engine — Dual-engine institutional analysis*")

    report = "\n".join(lines)
    report_path = out / f"meta_engine_report_{date_str}.md"
    with open(report_path, "w") as f:
        f.write(report)
    
    logger.info(f"📄 Report saved: {report_path}")
    return str(report_path)


def render_md_to_html(md_content: str) -> str:
    """
    Convert markdown content to beautifully styled HTML for email.
    Uses markdown2 for conversion with a custom dark-themed stylesheet.
    """
    try:
        import markdown2
    except ImportError:
        # Fallback: wrap in <pre> if markdown2 not available
        return f"<html><body><pre>{md_content}</pre></body></html>"
    
    # Convert markdown to HTML
    html_body = markdown2.markdown(
        md_content,
        extras=["tables", "fenced-code-blocks", "header-ids", "break-on-newline"]
    )
    
    # Wrap in styled HTML template
    styled_html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
    body {{
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
        margin: 0; padding: 30px; background: #0f0f23; color: #e0e0e0;
        line-height: 1.6;
    }}
    .container {{ max-width: 900px; margin: 0 auto; }}
    h1 {{
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 25px 30px; border-radius: 12px; text-align: center;
        color: white; font-size: 26px; margin-bottom: 25px;
    }}
    h2 {{
        color: #4ecdc4; border-bottom: 2px solid #333; padding-bottom: 8px;
        margin-top: 30px; font-size: 20px;
    }}
    h3 {{ color: #ffd93d; font-size: 17px; margin-top: 20px; }}
    p {{ margin: 8px 0; }}
    strong {{ color: #fff; }}
    blockquote {{
        background: #16213e; border-left: 4px solid #667eea; border-radius: 6px;
        padding: 15px 18px; margin: 12px 0; font-size: 14px; color: #bbb;
        line-height: 1.7;
    }}
    table {{
        width: 100%; border-collapse: collapse; margin: 15px 0;
        font-size: 13px;
    }}
    th {{
        background: #16213e; color: #4ecdc4; padding: 10px 8px;
        text-align: left; font-weight: 600; border-bottom: 2px solid #333;
    }}
    td {{
        padding: 8px; border-bottom: 1px solid #222; color: #ccc;
    }}
    tr:hover td {{ background: #1a1a3e; }}
    hr {{ border: none; border-top: 1px solid #333; margin: 25px 0; }}
    em {{ color: #999; }}
    code {{
        background: #16213e; padding: 2px 6px; border-radius: 3px;
        font-size: 12px; color: #4ecdc4;
    }}
    ul {{ padding-left: 20px; }}
    li {{ margin: 4px 0; }}
    .disclaimer {{
        background: #ff6b6b11; border-left: 3px solid #ff6b6b;
        padding: 12px 15px; margin: 25px 0; font-size: 12px; color: #888;
    }}
    .footer {{
        text-align: center; padding: 20px; color: #555; font-size: 12px;
        margin-top: 30px; border-top: 1px solid #222;
    }}
</style>
</head>
<body>
<div class="container">
{html_body}

<div class="disclaimer">
<strong>⚠️ RISK DISCLAIMER:</strong> This report is generated by algorithmic signal analysis
engines and is NOT financial advice. Options trading involves substantial risk of loss.
Never risk more than you can afford to lose. Past performance does not guarantee future results.
</div>

<div class="footer">
🏛️ <strong>Meta Engine</strong> — Cross-Engine Institutional Signal Analysis<br>
PutsEngine (PUT Detection) × Moonshot (Momentum/Squeeze Detection)<br>
Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S ET')}
</div>
</div>
</body>
</html>"""
    
    return styled_html


def convert_md_to_pdf(md_content: str, output_path: str) -> bool:
    """
    Convert markdown content to a professional PDF report.
    Uses fpdf2 for pure-Python PDF generation.
    
    Returns:
        True if PDF was generated successfully
    """
    try:
        from fpdf import FPDF
    except ImportError:
        logger.warning("fpdf2 not installed — skipping PDF generation. Run: pip install fpdf2")
        return False
    
    try:
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.add_page()
        
        # Helper: clean Unicode chars that Helvetica doesn't support
        def _clean(text: str) -> str:
            return (text
                .replace("\u2014", "--")   # em-dash
                .replace("\u2013", "-")    # en-dash
                .replace("\u2018", "'")    # left single quote
                .replace("\u2019", "'")    # right single quote
                .replace("\u201c", '"')    # left double quote
                .replace("\u201d", '"')    # right double quote
                .replace("\u2026", "...")  # ellipsis
                .replace("\u2022", "*")    # bullet
                .replace("\u2212", "-")    # minus sign
                .replace("\u00d7", "x")    # multiplication sign
                .replace("\u2248", "~")    # approx equal
                .replace("\u2264", "<=")   # less than or equal
                .replace("\u2265", ">=")   # greater than or equal
                .replace("\u2260", "!=")   # not equal
                .encode("latin-1", errors="replace").decode("latin-1")
            )
        
        # Title
        pdf.set_font("Helvetica", "B", 20)
        pdf.set_text_color(102, 126, 234)  # Blue
        pdf.cell(0, 15, _clean("META ENGINE DAILY REPORT"), new_x="LMARGIN", new_y="NEXT", align="C")
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(150, 150, 150)
        pdf.cell(0, 8, _clean(datetime.now().strftime("%B %d, %Y - %I:%M %p ET")), new_x="LMARGIN", new_y="NEXT", align="C")
        pdf.cell(0, 6, _clean("Cross-Engine Analysis: PutsEngine x Moonshot"), new_x="LMARGIN", new_y="NEXT", align="C")
        pdf.ln(5)
        pdf.set_draw_color(100, 100, 100)
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(5)
        
        # Process markdown content line by line
        for line in md_content.split("\n"):
            stripped = line.strip()
            
            # Skip markdown-specific formatting
            if stripped.startswith("---"):
                pdf.set_draw_color(100, 100, 100)
                pdf.line(10, pdf.get_y(), 200, pdf.get_y())
                pdf.ln(3)
                continue
            
            if stripped.startswith("# "):
                # Already handled by title
                continue
            
            if stripped.startswith("## "):
                pdf.ln(3)
                pdf.set_font("Helvetica", "B", 14)
                title_text = stripped[3:].replace("📊", "[SUMMARY]").replace("🔴", "[PUTS]").replace("🟢", "[CALLS]").replace("🔄", "[CROSS]").replace("📝", "[ANALYSIS]").replace("📁", "[FILES]").replace("⚠️", "[!]").replace("🏛️", "")
                pdf.set_text_color(78, 205, 196)  # Teal
                pdf.cell(0, 10, _clean(title_text.strip()), new_x="LMARGIN", new_y="NEXT")
                pdf.ln(1)
                continue
            
            if stripped.startswith("### "):
                pdf.ln(2)
                pdf.set_font("Helvetica", "B", 11)
                subtitle = stripped[4:].replace("🔴", "[PUTS]").replace("🟢", "[CALLS]").replace("⚠️", "[!]")
                pdf.set_text_color(255, 217, 61)  # Yellow
                pdf.cell(0, 8, _clean(subtitle.strip()), new_x="LMARGIN", new_y="NEXT")
                pdf.ln(1)
                continue
            
            if stripped.startswith("|") and "---" in stripped:
                # Table separator line, skip
                continue
            
            if stripped.startswith("|"):
                # Table row
                pdf.set_font("Helvetica", "", 7)
                pdf.set_text_color(200, 200, 200)
                # Clean up markdown formatting
                clean_row = stripped.replace("**", "").replace("|", " | ").strip()
                if clean_row.startswith(" | "):
                    clean_row = clean_row[3:]
                if clean_row.endswith(" | "):
                    clean_row = clean_row[:-3]
                pdf.cell(0, 5, _clean(clean_row[:120]), new_x="LMARGIN", new_y="NEXT")
                continue
            
            if stripped.startswith("> "):
                # Blockquote (summary text)
                pdf.set_font("Helvetica", "I", 8)
                pdf.set_text_color(170, 170, 170)
                text = stripped[2:]
                pdf.multi_cell(0, 4.5, _clean(text))
                pdf.ln(2)
                continue
            
            if stripped.startswith("**") and stripped.endswith(")"):
                # Pick header like **GOLD** (Puts: 0.95 | Moonshot: MODERATE)
                pdf.set_font("Helvetica", "B", 9)
                pdf.set_text_color(255, 255, 255)
                clean_hdr = stripped.replace("**", "")
                pdf.cell(0, 7, _clean(clean_hdr), new_x="LMARGIN", new_y="NEXT")
                continue
            
            if stripped.startswith("- `"):
                # File list item
                pdf.set_font("Helvetica", "", 8)
                pdf.set_text_color(150, 150, 150)
                clean_item = stripped.replace("`", "").replace("- ", "  * ")
                pdf.cell(0, 5, _clean(clean_item), new_x="LMARGIN", new_y="NEXT")
                continue
            
            if stripped.startswith("*Generated"):
                pdf.ln(5)
                pdf.set_font("Helvetica", "I", 8)
                pdf.set_text_color(100, 100, 100)
                pdf.cell(0, 5, _clean(stripped.replace("*", "")), new_x="LMARGIN", new_y="NEXT", align="C")
                continue
            
            if stripped:
                # Regular text
                pdf.set_font("Helvetica", "", 9)
                pdf.set_text_color(200, 200, 200)
                clean_txt = stripped.replace("**", "")
                pdf.multi_cell(0, 5, _clean(clean_txt))
                pdf.ln(1)
        
        # Disclaimer
        pdf.ln(5)
        pdf.set_draw_color(255, 107, 107)
        pdf.set_font("Helvetica", "I", 7)
        pdf.set_text_color(150, 100, 100)
        pdf.multi_cell(0, 4, _clean(
            "RISK DISCLAIMER: This report is generated by algorithmic signal analysis engines "
            "and is NOT financial advice. Options trading involves substantial risk of loss. "
            "Never risk more than you can afford to lose."
        ))
        
        pdf.output(output_path)
        logger.info(f"📄 PDF saved: {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"PDF generation failed: {e}")
        return False
