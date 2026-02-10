"""
Meta Engine — Streamlit Trading Dashboard
===========================================
Professional institutional-grade trading dashboard.
Port: 8511

Usage:
    streamlit run trading/streamlit_dashboard.py --server.port 8511
"""

import json
import os
import sys
import glob
import sqlite3
import time
from datetime import datetime, date, timedelta
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import requests
import pytz

# ── Paths ─────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

DB_PATH = PROJECT_ROOT / "data" / "trades.db"
OUTPUT_DIR = PROJECT_ROOT / "output"
LOGS_DIR = PROJECT_ROOT / "logs"
EST = pytz.timezone("US/Eastern")

# ── Alpaca client (lightweight) ───────────────────────
ALPACA_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET = os.getenv("ALPACA_SECRET_KEY", "")
ALPACA_BASE = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets").rstrip("/")
if ALPACA_BASE.endswith("/v2"):
    ALPACA_BASE = ALPACA_BASE[:-3]
ALPACA_HEADERS = {
    "APCA-API-KEY-ID": ALPACA_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET,
}
IS_PAPER = "paper" in ALPACA_BASE.lower()


# ═══════════════════════════════════════════════════════
#  Data helpers
# ═══════════════════════════════════════════════════════
@st.cache_data(ttl=10)
def get_alpaca_account():
    try:
        r = requests.get(f"{ALPACA_BASE}/v2/account", headers=ALPACA_HEADERS, timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}


@st.cache_data(ttl=10)
def get_alpaca_positions():
    try:
        r = requests.get(f"{ALPACA_BASE}/v2/positions", headers=ALPACA_HEADERS, timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception:
        return []


def get_db_conn():
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


@st.cache_data(ttl=10)
def get_all_trades():
    conn = get_db_conn()
    if not conn:
        return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT * FROM trades ORDER BY created_at DESC", conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=10)
def get_open_trades():
    conn = get_db_conn()
    if not conn:
        return pd.DataFrame()
    try:
        df = pd.read_sql(
            "SELECT * FROM trades WHERE status IN ('open','filled','pending') ORDER BY created_at DESC",
            conn,
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=10)
def get_closed_trades():
    conn = get_db_conn()
    if not conn:
        return pd.DataFrame()
    try:
        df = pd.read_sql(
            "SELECT * FROM trades WHERE status='closed' ORDER BY closed_at DESC", conn
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def load_latest_run():
    files = sorted(glob.glob(str(OUTPUT_DIR / "meta_engine_run_*.json")))
    if not files:
        return {}
    with open(files[-1]) as f:
        return json.load(f)


def load_latest_cross():
    path = OUTPUT_DIR / "cross_analysis_latest.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def load_latest_summaries():
    path = OUTPUT_DIR / "summaries_latest.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def get_market_status():
    """Determine market status based on current EST time."""
    now = datetime.now(EST)
    weekday = now.weekday()
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)

    if weekday >= 5:  # Weekend
        days_to_mon = 7 - weekday
        next_open = (now + timedelta(days=days_to_mon)).replace(
            hour=9, minute=30, second=0, microsecond=0
        )
        delta = next_open - now
        hours, remainder = divmod(int(delta.total_seconds()), 3600)
        mins = remainder // 60
        return "WAITING FOR MARKET", f"{hours}h {mins}m", next_open
    elif now < market_open:
        delta = market_open - now
        hours, remainder = divmod(int(delta.total_seconds()), 3600)
        mins = remainder // 60
        return "WAITING FOR MARKET", f"{hours}h {mins}m", market_open
    elif now <= market_close:
        delta = market_close - now
        hours, remainder = divmod(int(delta.total_seconds()), 3600)
        mins = remainder // 60
        return "MARKET OPEN", f"{hours}h {mins}m remaining", market_close
    else:
        # After hours — next day
        next_day = now + timedelta(days=1)
        if next_day.weekday() >= 5:
            days_to_mon = 7 - now.weekday()
            next_day = now + timedelta(days=days_to_mon)
        next_open = next_day.replace(hour=9, minute=30, second=0, microsecond=0)
        delta = next_open - now
        hours, remainder = divmod(int(delta.total_seconds()), 3600)
        mins = remainder // 60
        return "WAITING FOR MARKET", f"{hours}h {mins}m", next_open


# ═══════════════════════════════════════════════════════
#  Page config
# ═══════════════════════════════════════════════════════
st.set_page_config(
    page_title="Meta Engine Trading",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────
st.markdown(
    """
<style>
    /* Dark theme overrides */
    .stApp {background-color: #0a0e17;}
    section[data-testid="stSidebar"] {background-color: #131722;}
    .block-container {padding-top: 1rem;}

    /* Market status bar */
    .market-bar {
        background: linear-gradient(90deg, #1a1f2e 0%, #131722 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 8px;
        padding: 14px 24px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 20px;
    }
    .market-status {
        font-weight: 700;
        font-size: 16px;
    }
    .status-open {color: #00e676;}
    .status-waiting {color: #ffa726;}
    .market-time {color: #78909c; font-size: 13px;}

    /* Summary cards */
    .metric-card {
        background: rgba(26, 31, 46, 0.85);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 12px;
        padding: 18px 22px;
        text-align: center;
    }
    .metric-label {
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 1px;
        color: #78909c;
        margin-bottom: 4px;
    }
    .metric-value {
        font-size: 28px;
        font-weight: 700;
    }
    .metric-green {color: #00e676;}
    .metric-red {color: #ff1744;}
    .metric-blue {color: #448aff;}
    .metric-gold {color: #ffd740;}

    /* Position badge */
    .badge-call {
        background: rgba(0,230,118,0.12);
        color: #00e676;
        padding: 2px 10px;
        border-radius: 4px;
        font-size: 12px;
        font-weight: 600;
    }
    .badge-put {
        background: rgba(255,23,68,0.12);
        color: #ff1744;
        padding: 2px 10px;
        border-radius: 4px;
        font-size: 12px;
        font-weight: 600;
    }

    /* Info box */
    .info-box {
        background: rgba(68,138,255,0.08);
        border: 1px solid rgba(68,138,255,0.2);
        border-radius: 8px;
        padding: 12px 20px;
        color: #90caf9;
        font-size: 14px;
    }

    /* Hide Streamlit defaults */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
</style>
""",
    unsafe_allow_html=True,
)


# ═══════════════════════════════════════════════════════
#  Sidebar
# ═══════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## ⚙️ Configuration")

    mode_label = "PAPER" if IS_PAPER else "LIVE"
    st.markdown(
        f'<div style="background:{"rgba(68,138,255,0.15)" if IS_PAPER else "rgba(255,23,68,0.15)"}; '
        f'color:{"#448aff" if IS_PAPER else "#ff1744"}; padding:6px 14px; border-radius:6px; '
        f'text-align:center; font-weight:700; font-size:14px; margin-bottom:16px;">'
        f"🔗 Alpaca {mode_label} Trading</div>",
        unsafe_allow_html=True,
    )

    st.markdown("---")
    st.markdown("##### 📊 Trading Parameters")
    contracts_display = st.number_input("Contracts per Trade", value=5, min_value=1, max_value=50, disabled=True)
    st.caption("Set in `trading/executor.py`")

    tp_mult = st.slider("Take Profit (×)", 1.5, 10.0, 3.0, 0.5, disabled=True)
    sl_pct = st.slider("Stop Loss (%)", 10, 80, 50, 5, disabled=True)
    st.caption("Exit rules applied on each scan")

    st.markdown("---")
    st.markdown("##### 📅 Schedule")
    st.markdown("• **AM scan:** 9:35 AM ET")
    st.markdown("• **PM scan:** 3:15 PM ET")
    st.markdown("• **Days:** Mon – Fri")

    st.markdown("---")
    st.markdown("##### 🔗 Quick Links")
    st.markdown("[📧 Email Reports](mailto:akkayya.chavala@gmail.com)")
    st.markdown("[🐦 X/Twitter Posts](https://x.com)")

    st.markdown("---")
    auto_refresh = st.checkbox("Auto-refresh (10s)", value=True)
    if st.button("🔄 Refresh Now"):
        st.cache_data.clear()
        st.rerun()


# ═══════════════════════════════════════════════════════
#  Main content – tabs
# ═══════════════════════════════════════════════════════
tabs = st.tabs(
    [
        "📊 Trading Dashboard",
        "📈 Current Picks",
        "📋 Trade History",
        "📉 KPI & Performance",
        "🌦️ Market Weather",
        "📜 Logs",
    ]
)

# ─────────────────────────────────────────────────────
#  TAB 1: Trading Dashboard
# ─────────────────────────────────────────────────────
with tabs[0]:
    st.markdown("# Live Trading Activity")

    now_est = datetime.now(EST)
    st.caption(
        f"🔄 Auto-refreshing every 10 seconds | Last updated: "
        f"{now_est.strftime('%I:%M:%S %p EST')}"
    )

    # ── Market status bar ─────────────────────────────
    status_text, countdown, next_event = get_market_status()
    is_open = status_text == "MARKET OPEN"
    status_cls = "status-open" if is_open else "status-waiting"
    dot = "🟢" if is_open else "🟡"

    next_str = next_event.strftime("%A %I:%M %p EST") if next_event else ""
    close_str = now_est.replace(hour=16, minute=0).strftime("%I:%M %p EST")

    st.markdown(
        f"""<div class="market-bar">
        <div>
            <span class="market-status {status_cls}">{dot} {status_text} &nbsp; {countdown}</span>
        </div>
        <div class="market-time">
            {"Next: " + next_str + " | " if not is_open else ""}Closes: {close_str} | Time: {now_est.strftime('%I:%M:%S %p EST')}
        </div>
        </div>""",
        unsafe_allow_html=True,
    )

    # ── Portfolio summary ─────────────────────────────
    account = get_alpaca_account()
    equity = float(account.get("equity", 0))
    cash = float(account.get("cash", 0))
    buying_power = float(account.get("buying_power", 0))
    last_equity = float(account.get("last_equity", equity))
    day_pnl = equity - last_equity
    day_pnl_pct = (day_pnl / last_equity * 100) if last_equity > 0 else 0

    all_trades_df = get_all_trades()
    total_trades = len(all_trades_df) if not all_trades_df.empty else 0
    today_str = date.today().isoformat()
    today_trades = (
        all_trades_df[all_trades_df["scan_date"] == today_str]
        if not all_trades_df.empty
        else pd.DataFrame()
    )
    today_pnl_db = today_trades["pnl"].sum() if not today_trades.empty else 0

    st.markdown("#### Your Portfolio")
    col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
    with col1:
        pnl_color = "metric-green" if day_pnl >= 0 else "metric-red"
        pnl_sign = "+" if day_pnl >= 0 else ""
        st.markdown(
            f"""<div class="metric-card">
            <div class="metric-label">Portfolio Value</div>
            <div class="metric-value">${equity:,.2f}
            <span class="{pnl_color}" style="font-size:16px">{pnl_sign}{day_pnl_pct:.2f}%</span></div>
            <div style="color:#78909c;font-size:12px;margin-top:4px">{now_est.strftime('%B %d, %I:%M %p EST')}</div>
            </div>""",
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            f"""<div class="metric-card">
            <div class="metric-label">Completed Trades</div>
            <div class="metric-value metric-blue">{total_trades}</div>
            </div>""",
            unsafe_allow_html=True,
        )
    with col3:
        td_color = "metric-green" if today_pnl_db >= 0 else "metric-red"
        td_sign = "+" if today_pnl_db >= 0 else ""
        st.markdown(
            f"""<div class="metric-card">
            <div class="metric-label">Today's P&L</div>
            <div class="metric-value {td_color}">${td_sign}{today_pnl_db:,.2f}</div>
            </div>""",
            unsafe_allow_html=True,
        )
    with col4:
        st.markdown(
            f"""<div class="metric-card">
            <div class="metric-label">Buying Power</div>
            <div class="metric-value metric-blue">${buying_power:,.0f}</div>
            </div>""",
            unsafe_allow_html=True,
        )

    st.markdown("")

    # ── Current Positions ─────────────────────────────
    st.markdown("#### Current Positions")
    positions = get_alpaca_positions()
    open_df = get_open_trades()

    if not positions and open_df.empty:
        st.markdown(
            '<div class="info-box">No open positions — waiting for trades</div>',
            unsafe_allow_html=True,
        )
    else:
        if positions:
            pos_data = []
            for p in positions:
                unrealized = float(p.get("unrealized_pl", 0))
                mkt_val = float(p.get("market_value", 0))
                avg_entry = float(p.get("avg_entry_price", 0))
                cur_price = float(p.get("current_price", 0))
                qty = int(p.get("qty", 0))
                pnl_pct = float(p.get("unrealized_plpc", 0)) * 100

                pos_data.append(
                    {
                        "Symbol": p.get("symbol", ""),
                        "Side": p.get("side", "").upper(),
                        "Qty": qty,
                        "Avg Entry": f"${avg_entry:.2f}",
                        "Current": f"${cur_price:.2f}",
                        "Mkt Value": f"${mkt_val:.2f}",
                        "P&L": f"${unrealized:+,.2f}",
                        "P&L %": f"{pnl_pct:+.1f}%",
                    }
                )
            st.dataframe(pd.DataFrame(pos_data), use_container_width=True, hide_index=True)
        elif not open_df.empty:
            display_cols = [
                "symbol",
                "option_type",
                "option_symbol",
                "strike_price",
                "expiry_date",
                "entry_price",
                "current_price",
                "contracts",
                "pnl",
                "pnl_pct",
                "status",
            ]
            existing = [c for c in display_cols if c in open_df.columns]
            st.dataframe(open_df[existing], use_container_width=True, hide_index=True)

    # ── Trade Statistics ──────────────────────────────
    st.markdown("#### Trade Statistics")
    closed_df = get_closed_trades()

    c1, c2, c3, c4 = st.columns(4)
    if not closed_df.empty:
        wins = len(closed_df[closed_df["pnl"] > 0])
        losses = len(closed_df[closed_df["pnl"] <= 0])
        total_closed = wins + losses
        win_rate = (wins / total_closed * 100) if total_closed > 0 else 0
        total_pnl = closed_df["pnl"].sum()
    else:
        wins = losses = total_closed = 0
        win_rate = 0.0
        total_pnl = 0.0

    c1.metric("Total Trades", total_trades)
    c2.metric("Win Rate", f"{win_rate:.1f}%")
    pnl_prefix = "+" if total_pnl >= 0 else ""
    c3.metric("Total P&L", f"${pnl_prefix}{total_pnl:,.2f}")
    c4.metric("Open Positions", len(positions) if positions else (len(open_df) if not open_df.empty else 0))


# ─────────────────────────────────────────────────────
#  TAB 2: Current Picks
# ─────────────────────────────────────────────────────
with tabs[1]:
    st.markdown("# 📈 Latest Engine Picks")

    cross = load_latest_cross()
    latest_run = load_latest_run()
    run_ts = latest_run.get("timestamp", "")

    if run_ts:
        st.caption(f"From scan: {run_ts}")

    col_p, col_m = st.columns(2)

    with col_p:
        st.markdown("### 🔴 PutsEngine Top 10 (Bearish)")
        puts_cross = cross.get("puts_through_moonshot", [])
        if puts_cross:
            rows = []
            for i, p in enumerate(puts_cross, 1):
                moon_a = p.get("moonshot_analysis", {})
                rows.append(
                    {
                        "#": i,
                        "Symbol": p["symbol"],
                        "Score": f'{p.get("score", 0):.3f}',
                        "Price": f'${p.get("price", 0):.2f}',
                        "Signals": len(p.get("signals", [])),
                        "Moonshot Level": moon_a.get("opportunity_level", "N/A"),
                        "Gap Alert": "⚠️" if p.get("overnight_gap_alert") else "",
                    }
                )
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.info("No puts picks available yet.")

    with col_m:
        st.markdown("### 🟢 Moonshot Top 10 (Bullish)")
        moon_cross = cross.get("moonshot_through_puts", [])
        if moon_cross:
            rows = []
            for i, p in enumerate(moon_cross, 1):
                puts_a = p.get("puts_analysis", {})
                rows.append(
                    {
                        "#": i,
                        "Symbol": p["symbol"],
                        "Score": f'{p.get("score", 0):.3f}',
                        "Price": f'${p.get("price", 0):.2f}',
                        "Signals": len(p.get("signals", [])),
                        "Puts Risk": puts_a.get("risk_level", "N/A"),
                        "Gap Alert": "⚠️" if p.get("overnight_gap_alert") else "",
                    }
                )
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.info("No moonshot picks available yet.")

    # Conflict matrix
    st.markdown("---")
    st.markdown("### 🔀 Conflict Matrix")
    conflicts = cross.get("conflict_matrix", [])
    if conflicts:
        st.dataframe(pd.DataFrame(conflicts), use_container_width=True, hide_index=True)
    else:
        st.info("No conflicts detected.")

    # Overnight gap alerts
    alerts = [
        item.get("overnight_gap_alert")
        for item in (
            cross.get("puts_through_moonshot", [])
            + cross.get("moonshot_through_puts", [])
        )
        if item.get("overnight_gap_alert")
    ]
    if alerts:
        st.markdown("### ⚠️ Overnight Gap Alerts")
        for a in alerts:
            st.warning(a)


# ─────────────────────────────────────────────────────
#  TAB 3: Trade History
# ─────────────────────────────────────────────────────
with tabs[2]:
    st.markdown("# 📋 Trade History (6 Months)")

    all_df = get_all_trades()
    if all_df.empty:
        st.markdown(
            '<div class="info-box">No trade history yet — trades will appear after the first market-hours scan</div>',
            unsafe_allow_html=True,
        )
    else:
        # Filters
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            status_filter = st.multiselect(
                "Status",
                options=all_df["status"].unique().tolist(),
                default=all_df["status"].unique().tolist(),
            )
        with fc2:
            type_filter = st.multiselect(
                "Type",
                options=all_df["option_type"].unique().tolist() if "option_type" in all_df.columns else [],
                default=all_df["option_type"].unique().tolist() if "option_type" in all_df.columns else [],
            )
        with fc3:
            session_filter = st.multiselect(
                "Session",
                options=all_df["session"].unique().tolist() if "session" in all_df.columns else [],
                default=all_df["session"].unique().tolist() if "session" in all_df.columns else [],
            )

        filtered = all_df.copy()
        if status_filter:
            filtered = filtered[filtered["status"].isin(status_filter)]
        if type_filter and "option_type" in filtered.columns:
            filtered = filtered[filtered["option_type"].isin(type_filter)]
        if session_filter and "session" in filtered.columns:
            filtered = filtered[filtered["session"].isin(session_filter)]

        display_cols = [
            "scan_date", "session", "symbol", "option_type", "option_symbol",
            "strike_price", "entry_price", "exit_price", "contracts",
            "pnl", "pnl_pct", "exit_reason", "status", "source_engine",
        ]
        existing = [c for c in display_cols if c in filtered.columns]
        st.dataframe(filtered[existing], use_container_width=True, hide_index=True)

        st.caption(f"Showing {len(filtered)} of {len(all_df)} total trades")


# ─────────────────────────────────────────────────────
#  TAB 4: KPI & Performance
# ─────────────────────────────────────────────────────
with tabs[3]:
    st.markdown("# 📉 KPI & Performance")

    closed_df = get_closed_trades()

    if closed_df.empty:
        st.info("No closed trades yet — performance data will appear after positions are closed.")
    else:
        # Summary metrics
        k1, k2, k3, k4, k5 = st.columns(5)
        wins = len(closed_df[closed_df["pnl"] > 0])
        losses = len(closed_df[closed_df["pnl"] <= 0])
        total_c = wins + losses
        wr = (wins / total_c * 100) if total_c > 0 else 0
        avg_win = closed_df[closed_df["pnl"] > 0]["pnl"].mean() if wins > 0 else 0
        avg_loss = closed_df[closed_df["pnl"] <= 0]["pnl"].mean() if losses > 0 else 0
        total_pnl_c = closed_df["pnl"].sum()
        best = closed_df.loc[closed_df["pnl"].idxmax()] if not closed_df.empty else None
        worst = closed_df.loc[closed_df["pnl"].idxmin()] if not closed_df.empty else None

        k1.metric("Win Rate", f"{wr:.1f}%")
        k2.metric("Avg Win", f"${avg_win:+,.2f}")
        k3.metric("Avg Loss", f"${avg_loss:+,.2f}")
        k4.metric("Best Trade", f'{best["symbol"]} ${best["pnl"]:+,.2f}' if best is not None else "—")
        k5.metric("Worst Trade", f'{worst["symbol"]} ${worst["pnl"]:+,.2f}' if worst is not None else "—")

        st.markdown("---")

        # Cumulative P&L chart
        st.markdown("### Cumulative P&L")
        if "closed_at" in closed_df.columns:
            pnl_series = closed_df.sort_values("closed_at")
            pnl_series["cumulative_pnl"] = pnl_series["pnl"].cumsum()

            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=pnl_series["closed_at"],
                    y=pnl_series["cumulative_pnl"],
                    mode="lines+markers",
                    fill="tozeroy",
                    line=dict(
                        color="#00e676" if pnl_series["cumulative_pnl"].iloc[-1] >= 0 else "#ff1744",
                        width=2,
                    ),
                    fillcolor="rgba(0,230,118,0.1)"
                    if pnl_series["cumulative_pnl"].iloc[-1] >= 0
                    else "rgba(255,23,68,0.1)",
                    marker=dict(size=4),
                )
            )
            fig.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=350,
                margin=dict(l=20, r=20, t=10, b=20),
                xaxis=dict(gridcolor="rgba(255,255,255,0.04)"),
                yaxis=dict(gridcolor="rgba(255,255,255,0.04)", tickprefix="$"),
            )
            st.plotly_chart(fig, use_container_width=True)

        # Win/Loss donut
        c_left, c_right = st.columns(2)
        with c_left:
            st.markdown("### Win / Loss Distribution")
            fig_wl = go.Figure(
                go.Pie(
                    labels=["Wins", "Losses"],
                    values=[wins, losses],
                    hole=0.6,
                    marker=dict(colors=["#00e676", "#ff1744"]),
                    textinfo="label+value",
                    textfont=dict(size=14),
                )
            )
            fig_wl.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                height=300,
                margin=dict(l=10, r=10, t=10, b=10),
                showlegend=False,
            )
            st.plotly_chart(fig_wl, use_container_width=True)

        with c_right:
            st.markdown("### Puts vs Calls Performance")
            if "option_type" in closed_df.columns:
                by_type = (
                    closed_df.groupby("option_type")
                    .agg(trades=("pnl", "count"), total_pnl=("pnl", "sum"), avg_pnl=("pnl", "mean"))
                    .reset_index()
                )
                fig_type = go.Figure()
                colors = {"put": "#ff1744", "call": "#00e676"}
                for _, row in by_type.iterrows():
                    fig_type.add_trace(
                        go.Bar(
                            x=[row["option_type"].upper()],
                            y=[row["total_pnl"]],
                            name=row["option_type"].upper(),
                            marker_color=colors.get(row["option_type"], "#448aff"),
                            text=f"${row['total_pnl']:+,.0f}",
                            textposition="auto",
                        )
                    )
                fig_type.update_layout(
                    template="plotly_dark",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    height=300,
                    margin=dict(l=10, r=10, t=10, b=10),
                    showlegend=False,
                    yaxis=dict(tickprefix="$"),
                )
                st.plotly_chart(fig_type, use_container_width=True)
            else:
                st.info("Option type data not available.")


# ─────────────────────────────────────────────────────
#  TAB 5: Market Weather
# ─────────────────────────────────────────────────────
with tabs[4]:
    st.markdown("# 🌦️ Market Weather")

    summaries = load_latest_summaries()
    final_summary = summaries.get("final_summary", "")
    if final_summary:
        st.markdown(final_summary)
    else:
        st.info("No market summary available yet — run a scan first.")

    st.markdown("---")

    # Notifications status
    latest_run = load_latest_run()
    notifs = latest_run.get("notifications", {})
    trading_info = latest_run.get("trading", {})

    st.markdown("### 📡 System Status")
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Email", "✅ Sent" if notifs.get("email") else "⏸️")
    s2.metric("Telegram", "✅ Sent" if notifs.get("telegram") else "⏸️")
    s3.metric("X/Twitter", "✅ Posted" if notifs.get("x_twitter") else "⏸️")
    trades_placed = trading_info.get("trades_placed", 0)
    s4.metric("Trading", f"✅ {trades_placed} orders" if trades_placed > 0 else "⏸️ Pending")

    st.markdown("---")
    st.markdown("### 📊 Pick Summaries")

    puts_sums = summaries.get("puts_picks_summaries", [])
    moon_sums = summaries.get("moonshot_picks_summaries", [])

    cp, cm = st.columns(2)
    with cp:
        st.markdown("##### 🔴 Put Picks")
        for ps in puts_sums[:5]:
            with st.expander(f"**{ps.get('symbol', '?')}**"):
                st.write(ps.get("summary", ""))
    with cm:
        st.markdown("##### 🟢 Moonshot Picks")
        for ms in moon_sums[:5]:
            with st.expander(f"**{ms.get('symbol', '?')}**"):
                st.write(ms.get("summary", ""))


# ─────────────────────────────────────────────────────
#  TAB 6: Logs
# ─────────────────────────────────────────────────────
with tabs[5]:
    st.markdown("# 📜 Recent Logs")

    log_files = sorted(LOGS_DIR.glob("meta_engine_*.log"), reverse=True)
    if log_files:
        selected_log = st.selectbox("Log file", [f.name for f in log_files])
        log_path = LOGS_DIR / selected_log
        try:
            lines = log_path.read_text().split("\n")
            n_lines = st.slider("Lines to show", 50, min(len(lines), 500), 100)
            st.code("\n".join(lines[-n_lines:]), language="log")
        except Exception as e:
            st.error(f"Error reading log: {e}")
    else:
        st.info("No log files found yet.")


# ═══════════════════════════════════════════════════════
#  Auto-refresh
# ═══════════════════════════════════════════════════════
if auto_refresh:
    time.sleep(10)
    st.rerun()
