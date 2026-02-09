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
) -> bool:
    """
    Send the Meta Engine analysis report via email.
    
    Sends:
      1. Full .md report as beautifully styled HTML email body
      2. PDF attachment of the full report
      3. Technical analysis chart (inline + attachment)
    
    Args:
        summaries: Output from summary_generator.generate_all_summaries()
        chart_path: Path to the technical analysis chart PNG
        report_md_path: Path to the generated .md report file
        smtp_server: SMTP server address
        smtp_port: SMTP port
        smtp_user: SMTP username/email
        smtp_password: SMTP password (app password for Gmail)
        recipient: Email recipient
        
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
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
                   margin: 0; padding: 20px; background: #0f0f23; color: #e0e0e0; }}
            .container {{ max-width: 800px; margin: 0 auto; }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                       padding: 30px; border-radius: 12px; margin-bottom: 25px; text-align: center; }}
            .header h1 {{ color: white; margin: 0; font-size: 28px; }}
            .header p {{ color: #ddd; margin: 8px 0 0 0; font-size: 14px; }}
            .section {{ background: #1a1a2e; border-radius: 10px; padding: 20px; margin-bottom: 20px; 
                        border-left: 4px solid; }}
            .section-puts {{ border-left-color: #ff6b6b; }}
            .section-moonshot {{ border-left-color: #4ecdc4; }}
            .section-conflict {{ border-left-color: #ffd93d; }}
            .section-meta {{ border-left-color: #667eea; }}
            .section h2 {{ color: #fff; margin-top: 0; font-size: 20px; }}
            .pick {{ background: #16213e; border-radius: 8px; padding: 15px; margin: 12px 0; }}
            .pick-header {{ display: flex; justify-content: space-between; align-items: center; 
                            margin-bottom: 8px; }}
            .pick-symbol {{ font-size: 18px; font-weight: bold; }}
            .pick-score {{ padding: 4px 12px; border-radius: 20px; font-size: 13px; font-weight: bold; }}
            .score-high {{ background: #ff6b6b33; color: #ff6b6b; }}
            .score-moon {{ background: #4ecdc433; color: #4ecdc4; }}
            .pick-summary {{ font-size: 14px; line-height: 1.6; color: #bbb; }}
            .cross-label {{ font-size: 12px; padding: 3px 8px; border-radius: 4px; 
                            display: inline-block; margin-top: 8px; }}
            .label-low {{ background: #4ecdc422; color: #4ecdc4; }}
            .label-moderate {{ background: #ffd93d22; color: #ffd93d; }}
            .label-high {{ background: #ff6b6b22; color: #ff6b6b; }}
            .final-summary {{ background: linear-gradient(135deg, #16213e, #1a1a2e); 
                              padding: 20px; border-radius: 10px; font-size: 15px; 
                              line-height: 1.7; border: 1px solid #667eea44; }}
            .disclaimer {{ background: #ff6b6b11; border-left: 3px solid #ff6b6b; 
                           padding: 12px 15px; margin: 20px 0; font-size: 12px; color: #888; }}
            .footer {{ text-align: center; padding: 20px; color: #555; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🏛️ META ENGINE DAILY REPORT</h1>
                <p>{now.strftime('%B %d, %Y — %I:%M %p ET')}</p>
                <p>Cross-Engine Analysis: PutsEngine × Moonshot</p>
            </div>

            <div class="section section-meta">
                <h2>📊 Executive Summary</h2>
                <div class="final-summary">
                    {summaries.get('final_summary', 'Analysis in progress...')}
                </div>
            </div>
    """
    
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
        html += """<div class="section section-puts"><h2>🔴 PutsEngine Top Picks (Bearish)</h2>"""
        for i, pick in enumerate(puts_picks, 1):
            moon_level = pick.get("moonshot_level", "N/A")
            label_class = {"HIGH": "label-high", "MODERATE": "label-moderate", "LOW": "label-low"}.get(moon_level, "label-low")
            html += f"""
                <div class="pick">
                    <div class="pick-header">
                        <span class="pick-symbol">#{i} {pick['symbol']}</span>
                        <span class="pick-score score-high">PUT Score: {pick['puts_score']:.2f}</span>
                    </div>
                    <div class="pick-summary">{pick['summary']}</div>
                    <span class="cross-label {label_class}">Moonshot Counter-Signal: {moon_level}</span>
                </div>
            """
        html += "</div>"
    
    # Moonshot Picks
    moon_picks = summaries.get("moonshot_picks_summaries", [])
    if moon_picks:
        html += """<div class="section section-moonshot"><h2>🟢 Moonshot Top Picks (Bullish)</h2>"""
        for i, pick in enumerate(moon_picks, 1):
            puts_risk = pick.get("puts_risk", "N/A")
            label_class = {"HIGH": "label-high", "MODERATE": "label-moderate", "LOW": "label-low"}.get(puts_risk, "label-low")
            html += f"""
                <div class="pick">
                    <div class="pick-header">
                        <span class="pick-symbol">#{i} {pick['symbol']}</span>
                        <span class="pick-score score-moon">Moonshot: {pick['moonshot_score']:.2f}</span>
                    </div>
                    <div class="pick-summary">{pick['summary']}</div>
                    <span class="cross-label {label_class}">PutsEngine Risk: {puts_risk}</span>
                </div>
            """
        html += "</div>"
    
    # Conflicts
    conflicts = summaries.get("conflict_summaries", [])
    if conflicts:
        html += """<div class="section section-conflict"><h2>⚡ Conflict Zones</h2>"""
        for c in conflicts:
            html += f"""<div style="background: #ffd93d22; color: #ffd93d; padding: 10px 15px; border-radius: 6px; margin: 8px 0;">
                <strong>{c['symbol']}</strong>: {c['summary']}
            </div>"""
        html += "</div>"
    
    html += f"""
            <div class="disclaimer">
                <strong>⚠️ RISK DISCLAIMER:</strong> This report is generated by algorithmic 
                signal analysis engines and is NOT financial advice. Options trading involves 
                substantial risk of loss.
            </div>
            <div class="footer">
                <p>🏛️ <strong>Meta Engine</strong> — Cross-Engine Institutional Signal Analysis</p>
                <p>Report generated: {now.strftime('%Y-%m-%d %H:%M:%S ET')}</p>
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
