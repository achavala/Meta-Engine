"""
Meta Engine Telegram Sender
=============================
Sends focused alerts via Telegram Bot API:
  - 3-Sentence Institutional Summaries (all picks)
  - Conflict Matrix
  - NO chart, NO executive summary (email handles the full report)

Setup:
1. Create a bot via @BotFather on Telegram
2. Get your bot token
3. Get your chat_id (send /start to your bot, then check
   https://api.telegram.org/bot<TOKEN>/getUpdates)
4. Set META_TELEGRAM_BOT_TOKEN and META_TELEGRAM_CHAT_ID in .env
"""

import requests
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}"
MAX_MESSAGE_LENGTH = 4096


def send_telegram_message(
    message: str,
    bot_token: str = "",
    chat_id: str = "",
    parse_mode: str = "HTML",
) -> bool:
    """
    Send a text message via Telegram Bot API.
    Uses HTML parse mode by default (more reliable than Markdown for complex formatting).
    """
    if not bot_token or not chat_id:
        logger.warning("Telegram not configured — set META_TELEGRAM_BOT_TOKEN and META_TELEGRAM_CHAT_ID")
        return False
    
    try:
        url = f"{TELEGRAM_API_BASE.format(token=bot_token)}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        }
        
        resp = requests.post(url, json=payload, timeout=15)
        
        if resp.status_code == 200:
            logger.info("✅ Telegram message sent")
            return True
        else:
            logger.error(f"Telegram API error: {resp.status_code} — {resp.text[:200]}")
            # If HTML fails, try plain text
            if parse_mode:
                logger.info("Retrying without formatting...")
                payload["parse_mode"] = None
                # Strip HTML tags
                import re
                clean_text = re.sub(r'<[^>]+>', '', message)
                payload["text"] = clean_text
                resp2 = requests.post(url, json=payload, timeout=15)
                if resp2.status_code == 200:
                    logger.info("✅ Telegram message sent (plain text)")
                    return True
            return False
            
    except Exception as e:
        logger.error(f"❌ Telegram send failed: {e}")
        return False


def _send_multiple_messages(
    messages: List[str],
    bot_token: str,
    chat_id: str,
) -> bool:
    """Send a list of messages sequentially (for long content that exceeds 4096 chars)."""
    import time
    
    all_sent = True
    for i, msg in enumerate(messages):
        success = send_telegram_message(msg, bot_token, chat_id, parse_mode="HTML")
        if not success:
            all_sent = False
            logger.warning(f"  Message {i+1}/{len(messages)} failed")
        if i < len(messages) - 1:
            time.sleep(0.5)  # Small delay between messages
    
    return all_sent


def _format_telegram_summaries(summaries: Dict[str, Any]) -> List[str]:
    """
    Format summaries for Telegram: ONLY 3-sentence institutional summaries + conflict matrix.
    Splits into multiple messages if content exceeds 4096 chars.
    
    Returns:
        List of message strings, each ≤ 4096 chars
    """
    messages = []
    
    # ===== MESSAGE 1: Header + Market Direction + Conflict Matrix =====
    msg1_lines = []
    msg1_lines.append("🏛️ <b>META ENGINE — INSTITUTIONAL SUMMARIES</b>")
    msg1_lines.append(f"<i>{datetime.now().strftime('%B %d, %Y -- %I:%M %p ET')}</i>")
    msg1_lines.append("")

    # Market Direction Prediction
    try:
        from analysis.market_direction_predictor import MarketDirectionPredictor
        predictor = MarketDirectionPredictor()
        hour = datetime.now().hour
        timeframe = "today" if hour < 12 else "tomorrow"
        prediction = predictor.predict_market_direction(timeframe=timeframe)
        direction_text = predictor.format_for_telegram(prediction)
        msg1_lines.append(direction_text)
        msg1_lines.append("")
        msg1_lines.append("━━━━━━━━━━━━━━━━━━━━")
        msg1_lines.append("")
    except Exception as e:
        import logging as _log
        _log.getLogger(__name__).debug(f"Market direction for Telegram skipped: {e}")

    # Conflict Matrix
    conflicts = summaries.get("conflict_summaries", [])
    if conflicts:
        msg1_lines.append("⚠️ <b>CONFLICT ZONES (Both Engines):</b>")
        msg1_lines.append("")
        for c in conflicts:
            sym = c.get("symbol", "???")
            msg1_lines.append(f"⚡ <b>{sym}</b>: {_clean_for_telegram(c.get('summary', 'N/A'))}")
            msg1_lines.append("")
    else:
        msg1_lines.append("✅ <b>No conflict zones</b> — no overlapping tickers between engines")
        msg1_lines.append("")
    
    msg1_lines.append("━━━━━━━━━━━━━━━━━━━━")
    messages.append("\n".join(msg1_lines))
    
    # ===== MESSAGES 2+: Puts Summaries =====
    puts_summaries = summaries.get("puts_picks_summaries", [])
    if puts_summaries:
        current_msg_lines = []
        current_msg_lines.append("🔴 <b>BEARISH PICKS (PutsEngine)</b>")
        current_msg_lines.append("")
        
        for i, p in enumerate(puts_summaries, 1):
            sym = p["symbol"]
            score = p.get("puts_score", 0)
            moon_lvl = p.get("moonshot_level", "N/A")
            summary_text = _clean_for_telegram(p.get("summary", "N/A"))
            
            pick_block = []
            pick_block.append(f"<b>#{i} {sym}</b> | Puts: {score:.2f} | Moon: {moon_lvl}")
            pick_block.append(f"<i>{summary_text}</i>")
            pick_block.append("")
            
            pick_text = "\n".join(pick_block)
            current_text = "\n".join(current_msg_lines)
            
            # Check if adding this pick would exceed limit
            if len(current_text) + len(pick_text) + 50 > MAX_MESSAGE_LENGTH:
                # Send current message and start new one
                messages.append(current_text)
                current_msg_lines = []
                current_msg_lines.append("🔴 <b>BEARISH PICKS (cont.)</b>")
                current_msg_lines.append("")
            
            current_msg_lines.extend(pick_block)
        
        if current_msg_lines:
            messages.append("\n".join(current_msg_lines))
    
    # ===== MESSAGES N+: Moonshot Summaries =====
    moon_summaries = summaries.get("moonshot_picks_summaries", [])
    if moon_summaries:
        current_msg_lines = []
        current_msg_lines.append("🟢 <b>BULLISH PICKS (Moonshot)</b>")
        current_msg_lines.append("")
        
        for i, m in enumerate(moon_summaries, 1):
            sym = m["symbol"]
            score = m.get("moonshot_score", 0)
            puts_risk = m.get("puts_risk", "N/A")
            summary_text = _clean_for_telegram(m.get("summary", "N/A"))
            
            pick_block = []
            pick_block.append(f"<b>#{i} {sym}</b> | Moon: {score:.2f} | Puts Risk: {puts_risk}")
            pick_block.append(f"<i>{summary_text}</i>")
            pick_block.append("")
            
            pick_text = "\n".join(pick_block)
            current_text = "\n".join(current_msg_lines)
            
            if len(current_text) + len(pick_text) + 50 > MAX_MESSAGE_LENGTH:
                messages.append(current_text)
                current_msg_lines = []
                current_msg_lines.append("🟢 <b>BULLISH PICKS (cont.)</b>")
                current_msg_lines.append("")
            
            current_msg_lines.extend(pick_block)
        
        if current_msg_lines:
            messages.append("\n".join(current_msg_lines))
    
    # ===== FINAL MESSAGE: Disclaimer =====
    messages.append(
        "⚠️ <i>Not financial advice. Options involve substantial risk of loss. "
        "Past performance does not guarantee future results.</i>\n\n"
        "🏛️ <i>Meta Engine — Cross-Engine Institutional Analysis</i>"
    )
    
    return messages


def _clean_for_telegram(text: str) -> str:
    """Clean text for Telegram HTML format — escape special chars."""
    # Telegram HTML supports: <b>, <i>, <code>, <pre>, <a>
    # Must escape &, <, > that are NOT part of allowed HTML tags
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")
    return text


def send_meta_telegram(
    summaries: Dict[str, Any],
    chart_path: Optional[str] = None,
    bot_token: str = "",
    chat_id: str = "",
    gap_up_data: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Send the Meta Engine alert via Telegram.
    
    Sends:
      1. Conflict Matrix
      2. All 3-sentence institutional summaries (puts + moonshots)
      3. Gap-Up Alerts section (if candidates detected)
    
    Does NOT send: chart, executive summary, tables (those are in the email).
    
    Args:
        summaries: Output from summary_generator.generate_all_summaries()
        chart_path: Path to chart (unused — email handles chart delivery)
        bot_token: Telegram bot token
        chat_id: Target chat ID
        gap_up_data: Output from gap_up_detector.detect_gap_ups() (optional)
        
    Returns:
        True if all messages sent successfully
    """
    if not bot_token or not chat_id:
        logger.warning("Telegram not configured — set META_TELEGRAM_BOT_TOKEN and META_TELEGRAM_CHAT_ID")
        return False
    
    # Format the focused Telegram messages
    messages = _format_telegram_summaries(summaries)

    # FEB 16: Append gap-up alerts as a separate message
    if gap_up_data and gap_up_data.get("candidates"):
        try:
            from engine_adapters.gap_up_detector import format_gap_up_report
            gap_text = format_gap_up_report(gap_up_data)
            if gap_text:
                # Wrap in HTML for Telegram
                gap_msg = f"<pre>{_clean_for_telegram(gap_text)}</pre>"
                if len(gap_msg) > MAX_MESSAGE_LENGTH:
                    # Truncate if too long
                    gap_msg = gap_msg[:MAX_MESSAGE_LENGTH - 10] + "</pre>"
                messages.append(gap_msg)
                logger.info(
                    f"  🚀 Gap-up alerts appended to Telegram "
                    f"({len(gap_up_data['candidates'])} candidates)"
                )
        except Exception as e:
            logger.warning(f"  ⚠️ Gap-up Telegram formatting failed: {e}")
    
    logger.info(f"  📱 Sending {len(messages)} Telegram messages (summaries + conflicts + gap-ups)")
    
    # Send all messages
    return _send_multiple_messages(messages, bot_token, chat_id)
