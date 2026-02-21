"""
Meta Engine Configuration
========================
Loads settings from environment variables and .env file.
Inherits API keys from PutsEngine and TradeNova.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from Meta Engine directory first, then fallback to parent engines
META_DIR = Path(__file__).parent
load_dotenv(META_DIR / ".env")
load_dotenv(Path.home() / "PutsEngine" / ".env", override=False)
load_dotenv(Path.home() / "TradeNova" / ".env", override=False)


class MetaConfig:
    """Configuration for Meta Engine"""

    # ========== PATHS TO EXISTING ENGINES ==========
    PUTSENGINE_PATH = str(Path.home() / "PutsEngine")
    TRADENOVA_PATH = str(Path.home() / "TradeNova")
    META_ENGINE_PATH = str(META_DIR)

    # ========== API KEYS (inherited from existing engines) ==========
    ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
    ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
    ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets/v2")
    POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "") or os.getenv("MASSIVE_API_KEY", "")
    UNUSUAL_WHALES_API_KEY = os.getenv("UNUSUAL_WHALES_API_KEY", "")

    # ========== EMAIL SETTINGS ==========
    SMTP_SERVER = os.getenv("META_SMTP_SERVER", "") or os.getenv("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT = int(os.getenv("META_SMTP_PORT", "") or os.getenv("SMTP_PORT", "587"))
    SMTP_USER = os.getenv("META_SMTP_USER", "") or os.getenv("SMTP_USER", "") or os.getenv("PUTSENGINE_EMAIL_SENDER", "")
    SMTP_PASSWORD = os.getenv("META_SMTP_PASSWORD", "") or os.getenv("SMTP_PASSWORD", "") or os.getenv("PUTSENGINE_EMAIL_PASSWORD", "")
    ALERT_EMAIL = os.getenv("META_ALERT_EMAIL", "") or os.getenv("ALERT_EMAIL", "") or os.getenv("PUTSENGINE_EMAIL_RECIPIENT", "")

    # ========== TELEGRAM SETTINGS ==========
    TELEGRAM_BOT_TOKEN = os.getenv("META_TELEGRAM_BOT_TOKEN", "") or os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID = os.getenv("META_TELEGRAM_CHAT_ID", "") or os.getenv("TELEGRAM_CHAT_ID", "")

    # ========== X/TWITTER SETTINGS ==========
    X_API_KEY = os.getenv("X_API_KEY", "")
    X_API_SECRET = os.getenv("X_API_SECRET", "")
    X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN", "")
    X_ACCESS_TOKEN_SECRET = os.getenv("X_ACCESS_TOKEN_SECRET", "")
    X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN", "")

    # ========== SCHEDULE ==========
    # Three daily scans:
    #   0. 8:30 AM  — Pre-Market Scan: Pre-market movers, overnight gaps,
    #                 early institutional flow. Read-through before open.
    #   1. 9:35 AM  — Morning Scan: Post-open data, gap detection,
    #                 opening range forming. Trades execute here.
    #   2. 3:15 PM  — Afternoon Scan: Full intraday data, power hour setup.
    #                 Trades execute here.
    RUN_TIME_PREMARKET_ET = os.getenv("META_RUN_TIME_PRE", "08:30")  # Pre-market
    RUN_TIME_ET = os.getenv("META_RUN_TIME", "09:35")  # Morning (post-open)
    RUN_TIME_PM_ET = os.getenv("META_RUN_TIME_PM", "15:15")  # Afternoon
    RUN_TIMES_ET = [RUN_TIME_PREMARKET_ET, RUN_TIME_ET, RUN_TIME_PM_ET]
    TIMEZONE = "US/Eastern"

    # ========== ENGINE SETTINGS ==========
    TOP_N_PICKS = 10  # Top 10 from each engine
    
    # Analysis query template (the institutional-grade query)
    ANALYSIS_QUERY_TEMPLATE = (
        "Please give me realistic expectation on {ticker} for next day or 2 day "
        "with daily price changes, please do detailed analysis and provide details "
        "as detailed as possible, want to analyze like 30 years experience in trading "
        "and PhD in technical like 30+ yrs trading + PhD quant + institutional "
        "microstructure lens"
    )

    # ========== OUTPUT PATHS ==========
    OUTPUT_DIR = str(META_DIR / "output")
    LOGS_DIR = str(META_DIR / "logs")

    @classmethod
    def validate(cls) -> dict:
        """Validate configuration and return status"""
        return {
            "apis": {
                "alpaca": bool(cls.ALPACA_API_KEY and cls.ALPACA_SECRET_KEY),
                "polygon": bool(cls.POLYGON_API_KEY),
                "unusual_whales": bool(cls.UNUSUAL_WHALES_API_KEY),
            },
            "email": {
                "configured": bool(cls.SMTP_USER and cls.SMTP_PASSWORD and cls.ALERT_EMAIL),
                "server": cls.SMTP_SERVER,
                "recipient": cls.ALERT_EMAIL,
            },
            "telegram": {
                "configured": bool(cls.TELEGRAM_BOT_TOKEN and cls.TELEGRAM_CHAT_ID),
            },
            "x_twitter": {
                "configured": bool(cls.X_API_KEY and cls.X_ACCESS_TOKEN),
            },
            "engines": {
                "putsengine_exists": Path(cls.PUTSENGINE_PATH).exists(),
                "tradenova_exists": Path(cls.TRADENOVA_PATH).exists(),
            },
        }
