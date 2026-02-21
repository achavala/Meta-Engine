"""
Meta Engine Email Sender
=========================
Sends the complete daily analysis report via email with:
  - Full .md report rendered as beautiful styled HTML email body
  - PDF attachment of the complete .md report
  - Technical analysis chart as inline image + attachment
  - Professional dark-themed formatting

Requirements:
  - Gmail App Password (not regular password)
  - See setup guide in README for credential configuration
"""

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def send_meta_email(
    summaries: Dict[str, Any],
    chart_path: Optional[str] = None,
    report_md_path: Optional[str] = None,
    smtp_server: str = "smtp.gmail.com",
    smtp_port: int = 587,
    smtp_user: str = "",
    smtp_password: str = "",
    recipient: str = "",
    gap_up_data: Optional[Dict[str, Any]] = None,
    five_x_data: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Send the Meta Engine analysis report via email.
    
    Sends:
      1. Full .md report as beautifully styled HTML email body
      2. PDF attachment of the full report
      3. Technical analysis chart (inline + attachment)
      4. Gap-Up Alerts section (if candidates detected)
    
    Args:
        summaries: Output from summary_generator.generate_all_summaries()
        chart_path: Path to the technical analysis chart PNG
        report_md_path: Path to the generated .md report file
        smtp_server: SMTP server address
        smtp_port: SMTP port
        smtp_user: SMTP username/email
        smtp_password: SMTP password (app password for Gmail)
        recipient: Email recipient
        gap_up_data: Output from gap_up_detector.detect_gap_ups() (optional)
        
    Returns:
        True if email sent successfully
    """
    if not all([smtp_user, smtp_password, recipient]):
        logger.warning("Email not configured — set META_SMTP_USER, META_SMTP_PASSWORD, META_ALERT_EMAIL")
        return False
    
    # Support multiple recipients (comma-separated)
    if isinstance(recipient, str):
        recipients = [r.strip() for r in recipient.split(",") if r.strip()]
    elif isinstance(recipient, list):
        recipients = recipient
    else:
        recipients = [str(recipient)]
    
    try:
        msg = MIMEMultipart("mixed")
        
        n_puts = len(summaries.get("puts_picks_summaries", []))
        n_moon = len(summaries.get("moonshot_picks_summaries", []))
        n_conflicts = len(summaries.get("conflict_summaries", []))
        
        # Determine market bias for subject line
        high_puts = sum(1 for p in summaries.get("puts_picks_summaries", []) if p.get("puts_score", 0) >= 0.55)
        high_moon = sum(1 for m in summaries.get("moonshot_picks_summaries", []) if m.get("moonshot_score", 0) >= 0.60)
        if high_puts > high_moon:
            bias = "BEARISH"
        elif high_moon > high_puts:
            bias = "BULLISH"
        else:
            bias = "MIXED"
        
        conflict_str = f" | {n_conflicts} CONFLICTS" if n_conflicts > 0 else ""
        
        msg["Subject"] = (
            f"🏛️ Meta Engine [{bias}]: {n_puts} Puts + {n_moon} Moonshots{conflict_str} — "
            f"{datetime.now().strftime('%b %d %Y %I:%M %p ET')}"
        )
        msg["From"] = smtp_user
        msg["To"] = ", ".join(recipients)
        
        # === EMAIL BODY: Full .md report as HTML ===
        html_content = _build_full_html_email(summaries, report_md_path, chart_path)
        text_content = _build_text_report(summaries)
        
        # === INJECT GAP-UP ALERTS (FEB 16) ===
        if gap_up_data and gap_up_data.get("candidates"):
            try:
                from engine_adapters.gap_up_detector import format_gap_up_html, format_gap_up_report
                gap_html = format_gap_up_html(gap_up_data)
                gap_text = format_gap_up_report(gap_up_data)
                if gap_html:
                    # Insert before closing </body> or at end
                    if "</body>" in html_content:
                        html_content = html_content.replace("</body>", gap_html + "</body>")
                    else:
                        html_content += gap_html
                if gap_text:
                    text_content = gap_text + "\n\n" + text_content
                logger.info(f"  🚀 Gap-up alerts injected into email ({len(gap_up_data['candidates'])} candidates)")
            except Exception as e:
                logger.warning(f"  ⚠️ Gap-up email injection failed: {e}")
        
        # === INJECT 5x POTENTIAL (FEB 16) ===
        if five_x_data and (five_x_data.get("call_potential") or five_x_data.get("put_potential")):
            try:
                from engine_adapters.five_x_potential import format_5x_potential_report
                five_x_text = format_5x_potential_report(five_x_data)
                n_calls = len(five_x_data.get("call_potential", []))
                n_puts = len(five_x_data.get("put_potential", []))

                # Build styled HTML section for 5x Potential
                five_x_html = (
                    '<div style="background-color:#0d1117;border:1px solid #4d3200;'
                    'border-radius:12px;padding:24px;margin:24px 0;">'
                    '<h2 style="color:#ffa726;margin:0 0 6px 0;font-size:20px;">'
                    '🔥 System 2 — 5x Potential Watchlist</h2>'
                    '<p style="color:#90a4ae;font-size:13px;margin:0 0 16px 0;">'
                    'Broad awareness of stocks with ≥5x options return potential. '
                    'These complement the Top Picks above by surfacing volatile, '
                    'sector-wave, high-beta names. For awareness, NOT trade execution.</p>'
                )

                # Call potential table
                if five_x_data.get("call_potential"):
                    five_x_html += (
                        '<h3 style="color:#00e676;font-size:15px;margin:16px 0 8px 0;">'
                        f'📈 Top CALL Potential ({n_calls})</h3>'
                        '<table style="width:100%;border-collapse:collapse;font-size:13px;background-color:#0d1117;">'
                        '<tr style="border-bottom:1px solid #2a2d45;">'
                        '<th style="color:#78909c;padding:6px 10px;text-align:left;background-color:#131629;">#</th>'
                        '<th style="color:#78909c;padding:6px 10px;text-align:left;background-color:#131629;">Symbol</th>'
                        '<th style="color:#78909c;padding:6px 10px;text-align:right;background-color:#131629;">5x Score</th>'
                        '<th style="color:#78909c;padding:6px 10px;text-align:left;background-color:#131629;">Sector</th>'
                        '<th style="color:#78909c;padding:6px 10px;text-align:left;background-color:#131629;">Source</th>'
                        '</tr>'
                    )
                    for i, c in enumerate(five_x_data["call_potential"][:15], 1):
                        sym = c.get("symbol", "?")
                        score = c.get("_5x_score", c.get("five_x_score", 0))
                        sector = c.get("_sector", c.get("sector", "—")) or "—"
                        src = c.get("_source", "—")
                        score_str = f"{score:.3f}" if isinstance(score, (int, float)) else str(score)
                        five_x_html += (
                            f'<tr style="border-bottom:1px solid #1a1d30;">'
                            f'<td style="color:#e0e0e0;padding:5px 10px;background-color:#0d1117;">{i}</td>'
                            f'<td style="color:#00e676;padding:5px 10px;font-weight:700;background-color:#0d1117;">{sym}</td>'
                            f'<td style="color:#e0e0e0;padding:5px 10px;text-align:right;background-color:#0d1117;">{score_str}</td>'
                            f'<td style="color:#90a4ae;padding:5px 10px;background-color:#0d1117;">{sector}</td>'
                            f'<td style="color:#78909c;padding:5px 10px;background-color:#0d1117;">{src}</td>'
                            f'</tr>'
                        )
                    five_x_html += '</table>'

                # Put potential table
                if five_x_data.get("put_potential"):
                    five_x_html += (
                        '<h3 style="color:#ff1744;font-size:15px;margin:16px 0 8px 0;">'
                        f'📉 Top PUT Potential ({n_puts})</h3>'
                        '<table style="width:100%;border-collapse:collapse;font-size:13px;background-color:#0d1117;">'
                        '<tr style="border-bottom:1px solid #2a2d45;">'
                        '<th style="color:#78909c;padding:6px 10px;text-align:left;background-color:#131629;">#</th>'
                        '<th style="color:#78909c;padding:6px 10px;text-align:left;background-color:#131629;">Symbol</th>'
                        '<th style="color:#78909c;padding:6px 10px;text-align:right;background-color:#131629;">5x Score</th>'
                        '<th style="color:#78909c;padding:6px 10px;text-align:left;background-color:#131629;">Sector</th>'
                        '<th style="color:#78909c;padding:6px 10px;text-align:left;background-color:#131629;">Source</th>'
                        '</tr>'
                    )
                    for i, c in enumerate(five_x_data["put_potential"][:15], 1):
                        sym = c.get("symbol", "?")
                        score = c.get("_5x_score", c.get("five_x_score", 0))
                        sector = c.get("_sector", c.get("sector", "—")) or "—"
                        src = c.get("_source", "—")
                        score_str = f"{score:.3f}" if isinstance(score, (int, float)) else str(score)
                        five_x_html += (
                            f'<tr style="border-bottom:1px solid #1a1d30;">'
                            f'<td style="color:#e0e0e0;padding:5px 10px;background-color:#0d1117;">{i}</td>'
                            f'<td style="color:#ff1744;padding:5px 10px;font-weight:700;background-color:#0d1117;">{sym}</td>'
                            f'<td style="color:#e0e0e0;padding:5px 10px;text-align:right;background-color:#0d1117;">{score_str}</td>'
                            f'<td style="color:#90a4ae;padding:5px 10px;background-color:#0d1117;">{sector}</td>'
                            f'<td style="color:#78909c;padding:5px 10px;background-color:#0d1117;">{src}</td>'
                            f'</tr>'
                        )
                    five_x_html += '</table>'

                # Sector wave watchlist
                wave_wl = five_x_data.get("sector_wave_watchlist", [])
                if wave_wl:
                    wave_names = []
                    for w in wave_wl:
                        if isinstance(w, dict):
                            wave_names.append(w.get("symbol", "?"))
                        elif isinstance(w, str):
                            wave_names.append(w)
                    if wave_names:
                        five_x_html += (
                            '<h3 style="color:#ffd740;font-size:15px;margin:16px 0 8px 0;">'
                            '🌊 Sector Wave Watchlist</h3>'
                            '<p style="color:#b0bec5;font-size:13px;">'
                            + " · ".join(f"<b>{n}</b>" for n in wave_names[:30])
                            + '</p>'
                        )

                # Two-system explainer
                five_x_html += (
                    '<div style="background-color:#0d1a2e;border:1px solid #1a3a5c;'
                    'border-radius:8px;padding:12px 16px;margin-top:16px;">'
                    '<p style="color:#90caf9;font-size:12px;margin:0;line-height:1.5;">'
                    '<b style="color:#90caf9;">🏛️ Two-System Architecture:</b> '
                    '<span style="color:#00e676;">System 1 (Top Picks)</span> = '
                    'ultra-selective Policy B v4 trades (80% WR target). '
                    '<span style="color:#ffa726;">System 2 (5x Watchlist)</span> = '
                    'broad awareness of ≥5x potential movers (86% coverage). '
                    'Both run at every 9:35 AM &amp; 3:15 PM scan.</p>'
                    '</div>'
                )

                five_x_html += '</div>'

                if "</body>" in html_content:
                    html_content = html_content.replace("</body>", five_x_html + "</body>")
                else:
                    html_content += five_x_html
                if five_x_text:
                    text_content = five_x_text + "\n\n" + text_content
                logger.info(f"  🔥 5x Potential injected into email ({n_calls} calls, {n_puts} puts)")
            except Exception as e:
                logger.warning(f"  ⚠️ 5x Potential email injection failed: {e}")

        # Create alternative part (text + html)
        msg_alt = MIMEMultipart("alternative")
        msg_alt.attach(MIMEText(text_content, "plain", "utf-8"))
        msg_alt.attach(MIMEText(html_content, "html", "utf-8"))
        
        # Wrap alternative in related (for inline images)
        msg_related = MIMEMultipart("related")
        msg_related.attach(msg_alt)
        
        # Attach chart as inline image
        if chart_path and Path(chart_path).exists():
            with open(chart_path, "rb") as f:
                img_data = f.read()
            img = MIMEImage(img_data, name=Path(chart_path).name)
            img.add_header("Content-ID", "<meta_chart>")
            img.add_header("Content-Disposition", "inline", filename=Path(chart_path).name)
            msg_related.attach(img)
        
        msg.attach(msg_related)
        
        # === ATTACHMENT 1: PDF of the full report ===
        pdf_attached = False
        if report_md_path and Path(report_md_path).exists():
            pdf_path = _generate_pdf_attachment(report_md_path)
            if pdf_path and Path(pdf_path).exists():
                with open(pdf_path, "rb") as f:
                    pdf_data = f.read()
                pdf_attachment = MIMEBase("application", "pdf")
                pdf_attachment.set_payload(pdf_data)
                encoders.encode_base64(pdf_attachment)
                pdf_name = Path(pdf_path).name
                pdf_attachment.add_header("Content-Disposition", "attachment", filename=pdf_name)
                msg.attach(pdf_attachment)
                pdf_attached = True
                logger.info(f"  📎 PDF attached: {pdf_name}")
        
        # === ATTACHMENT 2: Chart image as downloadable file ===
        if chart_path and Path(chart_path).exists():
            with open(chart_path, "rb") as f:
                chart_data = f.read()
            chart_attachment = MIMEBase("application", "octet-stream")
            chart_attachment.set_payload(chart_data)
            encoders.encode_base64(chart_attachment)
            chart_attachment.add_header(
                "Content-Disposition", "attachment",
                filename=Path(chart_path).name
            )
            msg.attach(chart_attachment)
        
        # === SEND ===
        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
            server.starttls(context=context)
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, recipients, msg.as_string())
        
        logger.info(f"✅ Email sent to {', '.join(recipients)} (HTML body + {'PDF + ' if pdf_attached else ''}chart)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to send email: {e}")
        return False


def _build_full_html_email(
    summaries: Dict[str, Any],
    report_md_path: Optional[str] = None,
    chart_path: Optional[str] = None,
) -> str:
    """
    Build the complete HTML email body.
    
    Priority:
      1. If report_md_path exists → render the full .md to styled HTML
      2. Otherwise → build HTML from summaries data directly
    """
    # Try to render from .md file first (full report)
    if report_md_path and Path(report_md_path).exists():
        try:
            with open(report_md_path, "r") as f:
                md_content = f.read()
            
            from analysis.report_generator import render_md_to_html
            html = render_md_to_html(md_content)
            
            # Inject chart image reference if available
            if chart_path:
                chart_img_html = """
                <div style="text-align: center; margin: 25px 0;">
                    <h2 style="color: #4ecdc4;">📈 Technical Analysis Dashboard</h2>
                    <img src="cid:meta_chart" alt="Meta Engine Technical Chart"
                         style="max-width: 100%; border-radius: 10px; border: 1px solid #333;">
                </div>
                """
                # Insert before the disclaimer
                html = html.replace(
                    '<div class="disclaimer">',
                    chart_img_html + '<div class="disclaimer">'
                )
            
            logger.info(f"  📧 Email body: Full .md report rendered as HTML ({len(md_content)} chars)")
            return html
            
        except Exception as e:
            logger.warning(f"Failed to render .md to HTML: {e} — falling back to summaries-based email")
    
    # Fallback: build from summaries data
    return _build_html_from_summaries(summaries, chart_path)


def _build_html_from_summaries(summaries: Dict[str, Any], chart_path: Optional[str] = None) -> str:
    """Build HTML email from summaries data (fallback if .md not available)."""
    now = datetime.now()
    
    # All styles are INLINE — Gmail / Outlook strip <style> blocks entirely.
    _S = {
        "body": (
            "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;"
            "margin:0;padding:20px;background-color:#0f0f23;color:#e0e0e0;"
        ),
        "container": "max-width:800px;margin:0 auto;",
        "header": (
            "background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);"
            "padding:30px;border-radius:12px;margin-bottom:25px;text-align:center;"
        ),
        "section": "background-color:#1a1a2e;border-radius:10px;padding:20px;margin-bottom:20px;border-left:4px solid",
        "pick": "background-color:#16213e;border-radius:8px;padding:15px;margin:12px 0;",
        "pick_header": "margin-bottom:8px;",
        "pick_symbol": "font-size:18px;font-weight:bold;color:#ffffff;",
        "score_puts": "padding:4px 12px;border-radius:20px;font-size:13px;font-weight:bold;background-color:#3d1c1c;color:#ff6b6b;",
        "score_moon": "padding:4px 12px;border-radius:20px;font-size:13px;font-weight:bold;background-color:#1c3d3a;color:#4ecdc4;",
        "pick_summary": "font-size:14px;line-height:1.6;color:#bbbbbb;",
        "final_summary": (
            "background-color:#16213e;padding:20px;border-radius:10px;"
            "font-size:15px;line-height:1.7;border:1px solid #3a3f6a;color:#e0e0e0;"
        ),
        "disclaimer": (
            "background-color:#2a1515;border-left:3px solid #ff6b6b;"
            "padding:12px 15px;margin:20px 0;font-size:12px;color:#999999;"
        ),
        "footer": "text-align:center;padding:20px;color:#888888;font-size:12px;",
    }

    html = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"></head>
    <body style="{_S['body']}">
        <div style="{_S['container']}">
            <div style="{_S['header']}">
                <h1 style="color:#ffffff;margin:0;font-size:28px;">🏛️ META ENGINE DAILY REPORT</h1>
                <p style="color:#dddddd;margin:8px 0 0 0;font-size:14px;">{now.strftime('%B %d, %Y — %I:%M %p ET')}</p>
                <p style="color:#dddddd;margin:4px 0 0 0;font-size:14px;">Cross-Engine Analysis: PutsEngine × Moonshot</p>
            </div>

            <div style="{_S['section']} #667eea;">
                <h2 style="color:#ffffff;margin-top:0;font-size:20px;">📊 Executive Summary</h2>
                <div style="{_S['final_summary']}">
                    {summaries.get('final_summary', 'Analysis in progress...')}
                </div>
            </div>
    """

    # Market Direction Section
    try:
        from analysis.market_direction_predictor import MarketDirectionPredictor
        predictor = MarketDirectionPredictor()
        hour = now.hour
        timeframe = "today" if hour < 12 else "tomorrow"
        prediction = predictor.predict_market_direction(timeframe=timeframe)
        
        direction_label = prediction.get("direction_label", "⚪ Flat")
        confidence_pct = prediction.get("confidence_pct", 0)
        rationale = prediction.get("rationale", "")
        is_choppy = prediction.get("is_choppy", False)
        composite = prediction.get("composite_score", 0)
        header_text = "Market Direction Today" if timeframe == "today" else "Tomorrow Market Direction"
        
        # Color based on direction
        if "Green" in direction_label or "🟢" in direction_label:
            dir_color = "#4ecdc4"
            dir_border = "#4ecdc4"
        elif "Red" in direction_label or "🔴" in direction_label:
            dir_color = "#ff6b6b"
            dir_border = "#ff6b6b"
        else:
            dir_color = "#ffd93d"
            dir_border = "#ffd93d"
        
        signals_html = ""
        for s in prediction.get("signals", [])[:8]:
            signals_html += f"<li style='color: #bbb; margin: 4px 0;'>{s}</li>"
        
        html += f"""
            <div style="{_S['section']} {dir_border};">
                <h2 style="color:#ffffff;margin-top:0;font-size:20px;">🌤️ {header_text}</h2>
                <div style="background: linear-gradient(135deg, #16213e, #1a1a2e); 
                            padding: 20px; border-radius: 10px; border: 1px solid {dir_border}44;">
                    <div style="font-size: 24px; font-weight: bold; color: {dir_color}; margin-bottom: 10px;">
                        {direction_label}
                    </div>
                    <div style="font-size: 14px; color: #bbb;">
                        Confidence: {confidence_pct:.0f}% | Composite: {composite:+.4f}
                        {'| ⚠️ Choppy' if is_choppy else ''}
                    </div>
                    <div style="font-size: 14px; color: #ddd; margin-top: 10px;">
                        {rationale}
                    </div>
                    <ul style="margin-top: 10px; padding-left: 20px;">
                        {signals_html}
                    </ul>
                </div>
            </div>
        """
    except Exception as e:
        logger.debug(f"Market direction for email skipped: {e}")
    
    # Chart
    if chart_path:
        html += """
            <div style="text-align: center; margin: 20px 0;">
                <h2 style="color: #fff;">📈 Technical Analysis Dashboard</h2>
                <img src="cid:meta_chart" alt="Meta Engine Technical Chart"
                     style="max-width: 100%; border-radius: 10px; border: 1px solid #333;">
            </div>
        """
    
    # PutsEngine Picks
    puts_picks = summaries.get("puts_picks_summaries", [])
    if puts_picks:
        html += f'<div style="{_S["section"]} #ff6b6b;"><h2 style="color:#ffffff;margin-top:0;font-size:20px;">🔴 PutsEngine Top Picks (Bearish)</h2>'
        for i, pick in enumerate(puts_picks, 1):
            moon_level = pick.get("moonshot_level", "N/A")
            label_colors = {"HIGH": "#ff6b6b", "MODERATE": "#ffd93d", "LOW": "#4ecdc4"}.get(moon_level, "#4ecdc4")
            html += f"""
                <div style="{_S['pick']}">
                    <div style="{_S['pick_header']}">
                        <span style="{_S['pick_symbol']}">#{i} {pick['symbol']}</span>
                        <span style="{_S['score_puts']}">PUT Score: {pick['puts_score']:.2f}</span>
                    </div>
                    <div style="{_S['pick_summary']}">{pick['summary']}</div>
                    <span style="font-size:12px;padding:3px 8px;border-radius:4px;display:inline-block;margin-top:8px;color:{label_colors};">Moonshot Counter-Signal: {moon_level}</span>
                </div>
            """
        html += "</div>"

    # Moonshot Picks
    moon_picks = summaries.get("moonshot_picks_summaries", [])
    if moon_picks:
        html += f'<div style="{_S["section"]} #4ecdc4;"><h2 style="color:#ffffff;margin-top:0;font-size:20px;">🟢 Moonshot Top Picks (Bullish)</h2>'
        for i, pick in enumerate(moon_picks, 1):
            puts_risk = pick.get("puts_risk", "N/A")
            label_colors = {"HIGH": "#ff6b6b", "MODERATE": "#ffd93d", "LOW": "#4ecdc4"}.get(puts_risk, "#4ecdc4")
            html += f"""
                <div style="{_S['pick']}">
                    <div style="{_S['pick_header']}">
                        <span style="{_S['pick_symbol']}">#{i} {pick['symbol']}</span>
                        <span style="{_S['score_moon']}">Moonshot: {pick['moonshot_score']:.2f}</span>
                    </div>
                    <div style="{_S['pick_summary']}">{pick['summary']}</div>
                    <span style="font-size:12px;padding:3px 8px;border-radius:4px;display:inline-block;margin-top:8px;color:{label_colors};">PutsEngine Risk: {puts_risk}</span>
                </div>
            """
        html += "</div>"

    # Conflicts
    conflicts = summaries.get("conflict_summaries", [])
    if conflicts:
        html += f'<div style="{_S["section"]} #ffd93d;"><h2 style="color:#ffffff;margin-top:0;font-size:20px;">⚡ Conflict Zones</h2>'
        for c in conflicts:
            html += f"""<div style="background-color:#33300a;color:#ffd93d;padding:10px 15px;border-radius:6px;margin:8px 0;">
                <strong style="color:#ffd93d;">{c['symbol']}</strong>: {c['summary']}
            </div>"""
        html += "</div>"

    html += f"""
            <div style="{_S['disclaimer']}">
                <strong style="color:#ff6b6b;">⚠️ RISK DISCLAIMER:</strong> This report is generated by algorithmic
                signal analysis engines and is NOT financial advice. Options trading involves
                substantial risk of loss.
            </div>
            <div style="{_S['footer']}">
                <p style="color:#888888;">🏛️ <strong style="color:#e0e0e0;">Meta Engine</strong> — Cross-Engine Institutional Signal Analysis</p>
                <p style="color:#888888;">Report generated: {now.strftime('%Y-%m-%d %H:%M:%S ET')}</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html


def _build_text_report(summaries: Dict[str, Any]) -> str:
    """Build plain text fallback."""
    lines = []
    lines.append("=" * 60)
    lines.append("META ENGINE DAILY REPORT")
    lines.append(f"   {datetime.now().strftime('%B %d, %Y — %I:%M %p ET')}")
    lines.append("=" * 60)
    lines.append("")
    lines.append("EXECUTIVE SUMMARY:")
    lines.append(summaries.get("final_summary", ""))
    lines.append("")
    
    lines.append("PUTSENGINE TOP PICKS:")
    lines.append("-" * 40)
    for i, p in enumerate(summaries.get("puts_picks_summaries", []), 1):
        lines.append(f"\n#{i} {p['symbol']} (Score: {p['puts_score']:.2f})")
        lines.append(f"   Moonshot Counter-Signal: {p.get('moonshot_level', 'N/A')}")
        lines.append(f"   {p['summary']}")
    
    lines.append("")
    lines.append("MOONSHOT TOP PICKS:")
    lines.append("-" * 40)
    for i, m in enumerate(summaries.get("moonshot_picks_summaries", []), 1):
        lines.append(f"\n#{i} {m['symbol']} (Score: {m['moonshot_score']:.2f})")
        lines.append(f"   PutsEngine Risk: {m.get('puts_risk', 'N/A')}")
        lines.append(f"   {m['summary']}")
    
    conflicts = summaries.get("conflict_summaries", [])
    if conflicts:
        lines.append("")
        lines.append("CONFLICT ZONES:")
        lines.append("-" * 40)
        for c in conflicts:
            lines.append(f"  {c['symbol']}: {c['summary']}")
    
    lines.append("")
    lines.append("DISCLAIMER: Not financial advice. Options involve substantial risk.")
    lines.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S ET')}")
    
    return "\n".join(lines)


def _generate_pdf_attachment(report_md_path: str) -> Optional[str]:
    """Generate a PDF from the .md report file."""
    try:
        from analysis.report_generator import convert_md_to_pdf
        
        md_path = Path(report_md_path)
        pdf_path = md_path.with_suffix(".pdf")
        
        with open(md_path, "r") as f:
            md_content = f.read()
        
        success = convert_md_to_pdf(md_content, str(pdf_path))
        if success:
            return str(pdf_path)
        return None
        
    except Exception as e:
        logger.warning(f"PDF generation failed: {e}")
        return None
