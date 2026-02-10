"""
Meta Engine — Trading Dashboard
=================================
Flask web dashboard for trade history, P&L tracking, and position
monitoring.  Runs on port 5050.

Usage:
    python -m trading.dashboard          # from Meta Engine dir
    python trading/dashboard.py          # direct

Features:
    • Dark institutional theme
    • Real-time P&L and position updates
    • 6-month trade history with filters
    • Chart.js performance charts
    • Auto-refresh every 30 s
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Ensure project root on path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from flask import Flask, jsonify, render_template_string, request

from trading.trade_db import TradeDB
from trading.executor import AlpacaClient

logger = logging.getLogger("meta_engine.dashboard")

app = Flask(__name__)
db = TradeDB()

DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "5050"))

# ═══════════════════════════════════════════════════════
# HTML Template (embedded — no separate file needed)
# ═══════════════════════════════════════════════════════
DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Meta Engine — Trading Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
:root{--bg:#0a0e17;--bg2:#131722;--card:rgba(26,31,46,.85);--border:rgba(255,255,255,.06);
--text:#e0e0e0;--muted:#78909c;--green:#00e676;--red:#ff1744;--blue:#448aff;--gold:#ffd740}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh}
.topbar{background:var(--bg2);border-bottom:1px solid var(--border);padding:14px 32px;
display:flex;justify-content:space-between;align-items:center}
.topbar h1{font-size:20px;font-weight:700;letter-spacing:-.5px}
.topbar h1 span{color:var(--blue)}
.topbar .meta{font-size:12px;color:var(--muted);display:flex;gap:16px;align-items:center}
.badge{padding:3px 10px;border-radius:4px;font-size:11px;font-weight:600;text-transform:uppercase}
.badge.paper{background:rgba(68,138,255,.15);color:var(--blue)}
.badge.live{background:rgba(255,23,68,.15);color:var(--red)}
.container{max-width:1400px;margin:0 auto;padding:24px 32px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:28px}
.card{background:var(--card);backdrop-filter:blur(12px);border:1px solid var(--border);
border-radius:12px;padding:20px 24px;position:relative;overflow:hidden}
.card::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;border-radius:12px 12px 0 0}
.card.green::before{background:var(--green)}.card.red::before{background:var(--red)}
.card.blue::before{background:var(--blue)}.card.gold::before{background:var(--gold)}
.card .label{font-size:11px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:6px}
.card .value{font-size:28px;font-weight:700}
.card .value.positive{color:var(--green)}.card .value.negative{color:var(--red)}
.charts{display:grid;grid-template-columns:2fr 1fr;gap:16px;margin-bottom:28px}
.chart-card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px}
.chart-card h3{font-size:14px;font-weight:600;margin-bottom:16px;color:var(--muted)}
.section{background:var(--card);border:1px solid var(--border);border-radius:12px;
padding:20px 24px;margin-bottom:20px}
.section h3{font-size:14px;font-weight:600;margin-bottom:16px;display:flex;align-items:center;gap:8px}
.section h3 .count{background:var(--blue);color:#fff;padding:2px 8px;border-radius:10px;font-size:11px}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;padding:10px 12px;color:var(--muted);font-size:11px;text-transform:uppercase;
letter-spacing:.5px;border-bottom:1px solid var(--border)}
td{padding:10px 12px;border-bottom:1px solid var(--border)}
tr:hover td{background:rgba(255,255,255,.02)}
.sym{font-weight:600;color:#fff}.call{color:var(--green)}.put{color:var(--red)}
.pnl-pos{color:var(--green);font-weight:600}.pnl-neg{color:var(--red);font-weight:600}
.status{padding:2px 8px;border-radius:4px;font-size:11px;font-weight:500}
.status.open{background:rgba(68,138,255,.15);color:var(--blue)}
.status.closed{background:rgba(120,144,156,.15);color:var(--muted)}
.status.filled{background:rgba(0,230,118,.15);color:var(--green)}
.status.cancelled{background:rgba(255,23,68,.1);color:var(--red)}
.status.pending{background:rgba(255,215,64,.12);color:var(--gold)}
.empty{text-align:center;padding:40px;color:var(--muted);font-size:14px}
.refresh-dot{width:8px;height:8px;border-radius:50%;background:var(--green);display:inline-block;
animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
@media(max-width:900px){.charts{grid-template-columns:1fr}.cards{grid-template-columns:repeat(2,1fr)}}
</style>
</head>
<body>
<div class="topbar">
  <h1>🏛️ Meta Engine <span>Trading</span></h1>
  <div class="meta">
    <span class="badge" id="mode">PAPER</span>
    <span id="equity">—</span>
    <span><span class="refresh-dot"></span> Auto-refresh</span>
    <span id="clock"></span>
  </div>
</div>

<div class="container">
  <!-- Summary Cards -->
  <div class="cards" id="cards">
    <div class="card blue"><div class="label">Total P&L</div><div class="value" id="total-pnl">$0.00</div></div>
    <div class="card green"><div class="label">Today P&L</div><div class="value" id="today-pnl">$0.00</div></div>
    <div class="card gold"><div class="label">Win Rate</div><div class="value" id="win-rate">0%</div></div>
    <div class="card blue"><div class="label">Open Positions</div><div class="value" id="open-pos">0</div></div>
    <div class="card blue"><div class="label">Total Trades</div><div class="value" id="total-trades">0</div></div>
  </div>

  <!-- Charts -->
  <div class="charts">
    <div class="chart-card"><h3>Cumulative P&L</h3><canvas id="pnlChart"></canvas></div>
    <div class="chart-card"><h3>Win / Loss</h3><canvas id="wlChart"></canvas></div>
  </div>

  <!-- Open Positions -->
  <div class="section">
    <h3>📈 Open Positions <span class="count" id="open-count">0</span></h3>
    <div id="open-table"></div>
  </div>

  <!-- Trade History -->
  <div class="section">
    <h3>📋 Trade History (6 months) <span class="count" id="hist-count">0</span></h3>
    <div id="hist-table"></div>
  </div>
</div>

<script>
let pnlChart, wlChart;

function fmt(n){return n>=0?`$${n.toFixed(2)}`:`-$${Math.abs(n).toFixed(2)}`}
function pct(n){return `${n>=0?'+':''}${n.toFixed(1)}%`}
function cls(n){return n>=0?'pnl-pos':'pnl-neg'}

function updateClock(){
  document.getElementById('clock').textContent=new Date().toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
}
setInterval(updateClock,1000);updateClock();

async function loadData(){
  try{
    const [sumR,posR,histR,pnlR,acctR]=await Promise.all([
      fetch('/api/summary'),fetch('/api/positions'),
      fetch('/api/trades'),fetch('/api/pnl-chart'),fetch('/api/account')
    ]);
    const sum=await sumR.json(), pos=await posR.json(),
          hist=await histR.json(), pnl=await pnlR.json(), acct=await acctR.json();

    // Account
    if(acct.equity){
      document.getElementById('equity').textContent=`Equity: $${parseFloat(acct.equity).toLocaleString()}`;
      document.getElementById('mode').textContent=acct.is_paper?'PAPER':'LIVE';
      document.getElementById('mode').className='badge '+(acct.is_paper?'paper':'live');
    }

    // Cards
    const tp=sum.total_pnl||0, tdp=sum.today_pnl||0;
    const tpEl=document.getElementById('total-pnl');
    tpEl.textContent=fmt(tp);tpEl.className='value '+(tp>=0?'positive':'negative');
    const tdEl=document.getElementById('today-pnl');
    tdEl.textContent=fmt(tdp);tdEl.className='value '+(tdp>=0?'positive':'negative');
    document.getElementById('win-rate').textContent=`${sum.win_rate||0}%`;
    document.getElementById('open-pos').textContent=sum.open_positions||0;
    document.getElementById('total-trades').textContent=sum.total_trades||0;

    // Open positions table
    document.getElementById('open-count').textContent=pos.length;
    if(pos.length===0){
      document.getElementById('open-table').innerHTML='<div class="empty">No open positions</div>';
    }else{
      let h='<table><tr><th>Symbol</th><th>Type</th><th>Strike</th><th>Expiry</th><th>Entry</th><th>Current</th><th>P&L</th><th>P&L%</th><th>Contracts</th><th>Status</th></tr>';
      pos.forEach(t=>{
        const p=t.pnl||0,pp=t.pnl_pct||0;
        h+=`<tr><td class="sym">${t.symbol}</td><td class="${t.option_type}">${t.option_type.toUpperCase()}</td>
        <td>$${(t.strike_price||0).toFixed(0)}</td><td>${t.expiry_date||'—'}</td>
        <td>$${(t.entry_price||0).toFixed(2)}</td><td>$${(t.current_price||0).toFixed(2)}</td>
        <td class="${cls(p)}">${fmt(p)}</td><td class="${cls(pp)}">${pct(pp)}</td>
        <td>${t.contracts||5}</td><td><span class="status ${t.status}">${t.status}</span></td></tr>`;
      });
      h+='</table>';
      document.getElementById('open-table').innerHTML=h;
    }

    // History table
    document.getElementById('hist-count').textContent=hist.length;
    if(hist.length===0){
      document.getElementById('hist-table').innerHTML='<div class="empty">No trade history yet — trades will appear after the first scan</div>';
    }else{
      let h='<table><tr><th>Date</th><th>Session</th><th>Symbol</th><th>Type</th><th>Strike</th><th>Entry</th><th>Exit</th><th>P&L</th><th>P&L%</th><th>Reason</th><th>Status</th></tr>';
      hist.slice(0,100).forEach(t=>{
        const p=t.pnl||0,pp=t.pnl_pct||0;
        h+=`<tr><td>${t.scan_date||''}</td><td>${t.session||''}</td>
        <td class="sym">${t.symbol}</td><td class="${t.option_type}">${(t.option_type||'').toUpperCase()}</td>
        <td>$${(t.strike_price||0).toFixed(0)}</td><td>$${(t.entry_price||0).toFixed(2)}</td>
        <td>$${(t.exit_price||0).toFixed(2)}</td><td class="${cls(p)}">${fmt(p)}</td>
        <td class="${cls(pp)}">${pct(pp)}</td><td>${t.exit_reason||'—'}</td>
        <td><span class="status ${t.status}">${t.status}</span></td></tr>`;
      });
      h+='</table>';
      document.getElementById('hist-table').innerHTML=h;
    }

    // P&L Chart
    if(pnl.length>0){
      const labels=pnl.map(d=>d.scan_date), data=pnl.map(d=>d.cumulative_pnl);
      if(pnlChart)pnlChart.destroy();
      const ctx=document.getElementById('pnlChart').getContext('2d');
      const grad=ctx.createLinearGradient(0,0,0,250);
      const last=data[data.length-1]||0;
      if(last>=0){grad.addColorStop(0,'rgba(0,230,118,.3)');grad.addColorStop(1,'rgba(0,230,118,0)');}
      else{grad.addColorStop(0,'rgba(255,23,68,.3)');grad.addColorStop(1,'rgba(255,23,68,0)');}
      pnlChart=new Chart(ctx,{type:'line',data:{labels,datasets:[{data,
        borderColor:last>=0?'#00e676':'#ff1744',backgroundColor:grad,
        fill:true,tension:.3,pointRadius:2,borderWidth:2}]},
        options:{responsive:true,plugins:{legend:{display:false}},
        scales:{x:{ticks:{color:'#78909c',font:{size:10}},grid:{color:'rgba(255,255,255,.04)'}},
        y:{ticks:{color:'#78909c',callback:v=>'$'+v},grid:{color:'rgba(255,255,255,.04)'}}}}});
    }

    // Win/Loss chart
    const w=sum.wins||0,l=sum.losses||0;
    if(wlChart)wlChart.destroy();
    wlChart=new Chart(document.getElementById('wlChart'),{type:'doughnut',
      data:{labels:['Wins','Losses'],datasets:[{data:[w,l],
      backgroundColor:['#00e676','#ff1744'],borderWidth:0}]},
      options:{responsive:true,cutout:'70%',plugins:{legend:{labels:{color:'#e0e0e0'}}}}});
  }catch(e){console.error('Dashboard load error:',e)}
}

loadData();
setInterval(loadData,30000); // refresh every 30s
</script>
</body>
</html>
"""


# ═══════════════════════════════════════════════════════
# Flask Routes
# ═══════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)


@app.route("/api/summary")
def api_summary():
    stats = db.get_summary_stats()
    return jsonify(stats)


@app.route("/api/positions")
def api_positions():
    return jsonify(db.get_open_positions())


@app.route("/api/trades")
def api_trades():
    days = int(request.args.get("days", 180))
    return jsonify(db.get_recent_trades(days))


@app.route("/api/pnl-chart")
def api_pnl_chart():
    return jsonify(db.get_daily_pnl_series())


@app.route("/api/account")
def api_account():
    try:
        client = AlpacaClient()
        acct = client.get_account()
        return jsonify({
            "equity": acct.get("equity", "0"),
            "buying_power": acct.get("buying_power", "0"),
            "cash": acct.get("cash", "0"),
            "is_paper": client.is_paper,
        })
    except Exception as e:
        return jsonify({"error": str(e), "is_paper": True})


# ═══════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════

def start_dashboard(port: int = None):
    """Start the dashboard Flask server."""
    port = port or DASHBOARD_PORT
    logger.info(f"🖥️  Trading Dashboard starting on http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    start_dashboard()
