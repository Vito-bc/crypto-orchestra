"""
Crypto Orchestra — Live Trading Dashboard
Run: streamlit run app.py
Demo: streamlit run app.py -- --demo
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parent
DEMO_MODE = "--demo" in sys.argv

TRADE_FILE    = ROOT / ("demo" if DEMO_MODE else "logs") / "trade_history.jsonl"
POSITIONS_FILE = ROOT / "logs" / "open_positions.json"
DECISIONS_FILE = ROOT / ("demo" if DEMO_MODE else "logs") / "agent_decisions.jsonl"

PAPER_START = 10_000.0

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Crypto Orchestra",
    page_icon="🎼",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
  /* Global */
  html, body, [class*="css"] { font-family: 'JetBrains Mono', monospace; }
  .block-container { padding: 1.5rem 2rem 2rem; }

  /* KPI cards */
  .kpi-card {
    background: #161b22;
    border: 1px solid #21262d;
    border-radius: 10px;
    padding: 1.1rem 1.4rem;
    text-align: center;
  }
  .kpi-label { color: #8b949e; font-size: 0.72rem; letter-spacing: .08em; text-transform: uppercase; margin-bottom: .3rem; }
  .kpi-value { font-size: 1.7rem; font-weight: 700; line-height: 1.1; }
  .kpi-sub   { color: #8b949e; font-size: 0.75rem; margin-top: .25rem; }
  .green { color: #00ff88; }
  .red   { color: #ff4d4d; }
  .white { color: #e6edf3; }
  .dim   { color: #8b949e; }

  /* Live dot */
  .live-dot {
    display: inline-block; width: 8px; height: 8px;
    background: #00ff88; border-radius: 50%;
    animation: pulse 1.5s infinite;
    margin-right: 6px; vertical-align: middle;
  }
  @keyframes pulse {
    0%,100% { opacity: 1; } 50% { opacity: 0.3; }
  }

  /* Section headers */
  .section-title {
    color: #8b949e; font-size: 0.7rem; letter-spacing: .12em;
    text-transform: uppercase; margin: 1.6rem 0 .6rem;
    border-bottom: 1px solid #21262d; padding-bottom: .4rem;
  }

  /* Trade table */
  .trade-row {
    background: #161b22; border-radius: 6px;
    padding: .55rem 1rem; margin-bottom: .3rem;
    display: flex; align-items: center; gap: 1rem;
    border-left: 3px solid transparent;
  }
  .trade-win  { border-left-color: #00ff88; }
  .trade-loss { border-left-color: #ff4d4d; }

  /* Agent badges */
  .badge {
    display: inline-block; padding: .15rem .5rem;
    border-radius: 4px; font-size: .68rem; font-weight: 600;
    letter-spacing: .04em; margin: .1rem;
  }
  .badge-buy     { background: #0d3a1f; color: #00ff88; border: 1px solid #00ff8844; }
  .badge-sell    { background: #3a0d0d; color: #ff4d4d; border: 1px solid #ff4d4d44; }
  .badge-neutral { background: #1c2128; color: #8b949e; border: 1px solid #30363d; }

  /* Scrollable trade list */
  .trade-list { max-height: 420px; overflow-y: auto; padding-right: 4px; }
  .trade-list::-webkit-scrollbar { width: 4px; }
  .trade-list::-webkit-scrollbar-track { background: #0e1117; }
  .trade-list::-webkit-scrollbar-thumb { background: #30363d; border-radius: 2px; }

  div[data-testid="metric-container"] { display: none; }
</style>
""", unsafe_allow_html=True)


# ── Data loaders ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def load_trades() -> list[dict]:
    if not TRADE_FILE.exists():
        return []
    return [json.loads(l) for l in TRADE_FILE.read_text("utf-8").splitlines() if l.strip()]


@st.cache_data(ttl=60)
def load_decisions() -> list[dict]:
    if not DECISIONS_FILE.exists():
        return []
    return [json.loads(l) for l in DECISIONS_FILE.read_text("utf-8").splitlines() if l.strip()]


def load_positions() -> list[dict]:
    if not POSITIONS_FILE.exists() or DEMO_MODE:
        return []
    try:
        data = json.loads(POSITIONS_FILE.read_text("utf-8"))
        return [p for p in data if p.get("status") == "OPEN"]
    except Exception:
        return []


# ── Helpers ───────────────────────────────────────────────────────────────────

def equity_series(trades: list[dict]) -> list[float]:
    eq = [PAPER_START]
    for t in sorted(trades, key=lambda x: x.get("exit_time", "")):
        eq.append(eq[-1] + t["pnl_usd"])
    return eq


def color(v: float) -> str:
    return "#00ff88" if v >= 0 else "#ff4d4d"


def sign(v: float) -> str:
    return f"+${v:.2f}" if v >= 0 else f"-${abs(v):.2f}"


def pct_str(v: float) -> str:
    return f"{v:+.2f}%"


# ── Header ────────────────────────────────────────────────────────────────────

col_logo, col_status = st.columns([4, 1])
with col_logo:
    mode_tag = "DEMO MODE" if DEMO_MODE else "PAPER TRADING"
    st.markdown(f"""
    <h1 style="margin:0; font-size:1.8rem; color:#e6edf3; letter-spacing:.04em;">
      🎼 CRYPTO ORCHESTRA
    </h1>
    <p style="margin:.2rem 0 0; color:#8b949e; font-size:.8rem; letter-spacing:.06em;">
      AI-DRIVEN MULTI-AGENT TRADING SYSTEM &nbsp;·&nbsp; {mode_tag}
    </p>
    """, unsafe_allow_html=True)
with col_status:
    now_utc = datetime.now(timezone.utc).strftime("%H:%M UTC")
    st.markdown(f"""
    <div style="text-align:right; padding-top:.6rem;">
      <span class="live-dot"></span>
      <span style="color:#00ff88; font-size:.8rem; font-weight:600;">LIVE</span>
      <br><span style="color:#8b949e; font-size:.7rem;">{now_utc}</span>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<hr style='border-color:#21262d; margin:.6rem 0 1.2rem'>", unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────

trades    = load_trades()
decisions = load_decisions()
positions = load_positions()

wins   = [t for t in trades if t["pnl_usd"] > 0]
losses = [t for t in trades if t["pnl_usd"] <= 0]
total_pnl  = sum(t["pnl_usd"] for t in trades)
total_fees = sum(t.get("entry_fee_usd", 0) + t.get("exit_fee_usd", 0) for t in trades)
wr         = len(wins) / len(trades) * 100 if trades else 0
avg_win    = sum(t["pnl_usd"] for t in wins)   / len(wins)   if wins   else 0
avg_loss   = sum(t["pnl_usd"] for t in losses) / len(losses) if losses else 0
pf         = abs(avg_win / avg_loss) if avg_loss and avg_win else 0.0
equity_now = PAPER_START + total_pnl

eq = equity_series(trades)
peak = max(eq) if eq else PAPER_START
max_dd = max((peak - v) / peak * 100 for v in eq) if len(eq) > 1 else 0.0

action_counts = {}
for d in decisions:
    a = d.get("action", "HOLD")
    action_counts[a] = action_counts.get(a, 0) + 1

# ── KPI row ───────────────────────────────────────────────────────────────────

k1, k2, k3, k4, k5, k6 = st.columns(6)

def kpi(col, label, value, sub="", value_class="white"):
    col.markdown(f"""
    <div class="kpi-card">
      <div class="kpi-label">{label}</div>
      <div class="kpi-value {value_class}">{value}</div>
      <div class="kpi-sub">{sub}</div>
    </div>
    """, unsafe_allow_html=True)

equity_cls = "green" if equity_now >= PAPER_START else "red"
pnl_cls    = "green" if total_pnl >= 0 else "red"
wr_cls     = "green" if wr >= 45 else "red"
pf_cls     = "green" if pf >= 1.2 else ("white" if pf >= 0.8 else "red")
dd_cls     = "green" if max_dd < 2 else ("white" if max_dd < 5 else "red")

kpi(k1, "Paper Equity",  f"${equity_now:,.2f}",  f"started ${PAPER_START:,.0f}", equity_cls)
kpi(k2, "Net P&L",       sign(total_pnl),         f"{total_pnl/PAPER_START*100:+.3f}%", pnl_cls)
kpi(k3, "Win Rate",      f"{wr:.1f}%",            f"{len(wins)}W / {len(losses)}L", wr_cls)
kpi(k4, "Profit Factor", f"{pf:.2f}",             "≥1.2 target", pf_cls)
kpi(k5, "Max Drawdown",  f"{max_dd:.2f}%",        f"{len(trades)} closed trades", dd_cls)
kpi(k6, "Decisions",     f"{len(decisions):,}",   f"BUY {action_counts.get('BUY',0)} · SELL {action_counts.get('SELL',0)}", "dim")

# ── Equity curve ──────────────────────────────────────────────────────────────

st.markdown('<div class="section-title">Equity Curve</div>', unsafe_allow_html=True)

if len(eq) > 1:
    dates = ["Start"]
    for t in sorted(trades, key=lambda x: x.get("exit_time", "")):
        try:
            dt = datetime.fromisoformat(t["exit_time"])
            dates.append(dt.strftime("%b %d %H:%M"))
        except Exception:
            dates.append("?")

    fig = go.Figure()

    # Fill area
    fill_color = "rgba(0,255,136,0.06)" if eq[-1] >= PAPER_START else "rgba(255,77,77,0.06)"
    line_color  = "#00ff88" if eq[-1] >= PAPER_START else "#ff4d4d"

    fig.add_trace(go.Scatter(
        x=dates, y=eq,
        mode="lines+markers",
        line=dict(color=line_color, width=2.5, shape="spline"),
        marker=dict(size=5, color=line_color, symbol="circle"),
        fill="tozeroy",
        fillcolor=fill_color,
        hovertemplate="<b>%{x}</b><br>Equity: $%{y:,.2f}<extra></extra>",
    ))
    fig.add_hline(y=PAPER_START, line_dash="dash", line_color="#30363d", line_width=1)

    fig.update_layout(
        height=280,
        paper_bgcolor="#0e1117",
        plot_bgcolor="#0e1117",
        margin=dict(l=0, r=0, t=0, b=0),
        xaxis=dict(showgrid=False, color="#8b949e", tickfont=dict(size=10)),
        yaxis=dict(
            showgrid=True, gridcolor="#161b22", color="#8b949e",
            tickformat="$,.0f", tickfont=dict(size=10),
        ),
        hovermode="x unified",
        hoverlabel=dict(bgcolor="#161b22", bordercolor="#30363d", font_color="#e6edf3"),
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.markdown('<p style="color:#8b949e; text-align:center; padding:2rem;">Waiting for first closed trade…</p>', unsafe_allow_html=True)

# ── Two columns: trades + agents ──────────────────────────────────────────────

left, right = st.columns([3, 2])

# ── Recent trades ─────────────────────────────────────────────────────────────
with left:
    st.markdown('<div class="section-title">Recent Trades</div>', unsafe_allow_html=True)
    if not trades:
        st.markdown('<p style="color:#8b949e;">No closed trades yet.</p>', unsafe_allow_html=True)
    else:
        recent = sorted(trades, key=lambda x: x.get("exit_time", ""), reverse=True)[:20]
        rows_html = ""
        for t in recent:
            pnl   = t["pnl_usd"]
            cls   = "trade-win" if pnl >= 0 else "trade-loss"
            c     = "#00ff88" if pnl >= 0 else "#ff4d4d"
            arrow = "▲" if pnl >= 0 else "▼"
            try:
                dt = datetime.fromisoformat(t["exit_time"]).strftime("%b %d %H:%M")
            except Exception:
                dt = "?"
            reason_badge = {
                "TAKE_PROFIT": '<span style="color:#00ff88;font-size:.7rem;">✓ TARGET</span>',
                "STOP_LOSS":   '<span style="color:#ff4d4d;font-size:.7rem;">✗ STOP</span>',
                "MAX_HOLD":    '<span style="color:#e3b341;font-size:.7rem;">⏱ MAX HOLD</span>',
            }.get(t.get("reason", ""), t.get("reason", ""))

            rows_html += f"""
            <div class="trade-row {cls}">
              <span style="color:#8b949e;font-size:.72rem;min-width:80px;">{dt}</span>
              <span style="color:#e6edf3;font-size:.78rem;min-width:70px;font-weight:600;">{t.get('asset','?')}</span>
              <span style="color:#8b949e;font-size:.72rem;min-width:90px;">${t.get('entry_price',0):,.1f} → ${t.get('exit_price',0):,.1f}</span>
              <span style="flex:1;">{reason_badge}</span>
              <span style="color:{c};font-weight:700;font-size:.85rem;">{arrow} {sign(pnl)}</span>
              <span style="color:#8b949e;font-size:.7rem;">{t.get('hold_hours',0):.0f}h</span>
            </div>"""
        st.markdown(f'<div class="trade-list">{rows_html}</div>', unsafe_allow_html=True)

# ── Agent breakdown + by-asset ────────────────────────────────────────────────
with right:
    st.markdown('<div class="section-title">Agent Signal Breakdown</div>', unsafe_allow_html=True)

    # Collect per-agent stats from BUY decisions
    agent_stats: dict[str, dict] = {}
    buy_decisions = [d for d in decisions if d.get("action") == "BUY"]
    for d in buy_decisions:
        for v in d.get("votes", []):
            ag = v.get("agent", "?")
            sig = v.get("signal", "NEUTRAL")
            if ag not in agent_stats:
                agent_stats[ag] = {"BUY": 0, "SELL": 0, "NEUTRAL": 0, "conf": []}
            agent_stats[ag][sig] = agent_stats[ag].get(sig, 0) + 1
            agent_stats[ag]["conf"].append(v.get("confidence", 0))

    if agent_stats:
        agent_order = ["macro", "technical", "whale", "sentiment", "risk"]
        weights_map = {"macro": 0.30, "technical": 0.25, "whale": 0.20, "sentiment": 0.15, "risk": 0.10}
        for ag in agent_order:
            if ag not in agent_stats:
                continue
            s = agent_stats[ag]
            total_ag = s["BUY"] + s["SELL"] + s["NEUTRAL"]
            buy_pct  = s["BUY"] / total_ag * 100 if total_ag else 0
            avg_conf = sum(s["conf"]) / len(s["conf"]) if s["conf"] else 0
            w = weights_map.get(ag, 0)
            bar_w = int(buy_pct)
            st.markdown(f"""
            <div style="margin-bottom:.7rem;">
              <div style="display:flex;justify-content:space-between;margin-bottom:.2rem;">
                <span style="color:#e6edf3;font-size:.78rem;font-weight:600;text-transform:capitalize;">{ag}</span>
                <span style="color:#8b949e;font-size:.7rem;">w={w:.0%} · conf {avg_conf:.0%}</span>
              </div>
              <div style="background:#21262d;border-radius:3px;height:6px;margin-bottom:.25rem;">
                <div style="background:#00ff88;width:{bar_w}%;height:6px;border-radius:3px;"></div>
              </div>
              <div style="font-size:.68rem;color:#8b949e;">
                BUY {s['BUY']} · SELL {s['SELL']} · NEUTRAL {s['NEUTRAL']}
              </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.markdown('<p style="color:#8b949e;font-size:.8rem;">No BUY signals recorded yet.</p>', unsafe_allow_html=True)

    # By asset
    st.markdown('<div class="section-title">Performance by Asset</div>', unsafe_allow_html=True)
    if trades:
        assets = sorted(set(t["asset"] for t in trades))
        fig2 = go.Figure()
        asset_colors = {"BTC-USD": "#f7931a", "ETH-USD": "#627eea", "SOL-USD": "#9945ff", "ZEC-USD": "#f4b728"}
        for asset in assets:
            sub = [t for t in trades if t["asset"] == asset]
            pnl = sum(t["pnl_usd"] for t in sub)
            fig2.add_trace(go.Bar(
                x=[asset], y=[pnl],
                marker_color=asset_colors.get(asset, "#00ff88"),
                marker_line_width=0,
                text=[f"{sign(pnl)}"],
                textposition="outside",
                textfont=dict(color="#e6edf3", size=11),
                hovertemplate=f"<b>{asset}</b><br>P&L: {sign(pnl)}<br>Trades: {len(sub)}<extra></extra>",
            ))
        fig2.update_layout(
            height=200,
            paper_bgcolor="#0e1117",
            plot_bgcolor="#0e1117",
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis=dict(showgrid=False, color="#8b949e", tickfont=dict(size=11)),
            yaxis=dict(showgrid=True, gridcolor="#161b22", color="#8b949e",
                       tickformat="$,.0f", tickfont=dict(size=10), zeroline=True, zerolinecolor="#30363d"),
            showlegend=False,
            bargap=0.35,
        )
        st.plotly_chart(fig2, use_container_width=True)

# ── Open positions ────────────────────────────────────────────────────────────

if positions:
    st.markdown('<div class="section-title">Open Positions</div>', unsafe_allow_html=True)
    pos_cols = st.columns(len(positions))
    for i, p in enumerate(positions):
        entry  = p.get("entry_price", 0)
        stop   = p.get("stop_price", 0)
        target = p.get("target_price", 0)
        qty    = p.get("qty_usd", 0)
        try:
            held = (datetime.now(timezone.utc) - datetime.fromisoformat(p["entry_time"])).total_seconds() / 3600
        except Exception:
            held = 0
        risk_pct   = (entry - stop) / entry * 100 if entry else 0
        reward_pct = (target - entry) / entry * 100 if entry else 0
        with pos_cols[i]:
            st.markdown(f"""
            <div class="kpi-card" style="text-align:left;">
              <div style="display:flex;justify-content:space-between;margin-bottom:.6rem;">
                <span style="color:#e6edf3;font-weight:700;">{p.get('asset','?')}</span>
                <span style="color:#e3b341;font-size:.7rem;">● OPEN · {held:.1f}h</span>
              </div>
              <div style="font-size:.78rem;color:#8b949e;line-height:1.8;">
                Entry &nbsp;&nbsp;: <span style="color:#e6edf3;">${entry:,.2f}</span><br>
                Stop &nbsp;&nbsp;&nbsp;: <span style="color:#ff4d4d;">${stop:,.2f} ({risk_pct:.1f}%)</span><br>
                Target &nbsp;: <span style="color:#00ff88;">${target:,.2f} ({reward_pct:.1f}%)</span><br>
                Size &nbsp;&nbsp;&nbsp;: <span style="color:#e6edf3;">${qty:.0f}</span>
              </div>
            </div>
            """, unsafe_allow_html=True)

# ── Footer ────────────────────────────────────────────────────────────────────

st.markdown("<hr style='border-color:#21262d; margin:1.5rem 0 .8rem'>", unsafe_allow_html=True)
c1, c2, c3 = st.columns(3)
with c1:
    st.markdown('<span style="color:#8b949e;font-size:.72rem;">🎼 Crypto Orchestra · AI Multi-Agent System</span>', unsafe_allow_html=True)
with c2:
    st.markdown('<span style="color:#8b949e;font-size:.72rem;text-align:center;display:block;">BTC · ETH · SOL · ZEC</span>', unsafe_allow_html=True)
with c3:
    mode_lbl = "📊 Demo Data" if DEMO_MODE else "🔴 Live Paper Trading"
    st.markdown(f'<span style="color:#8b949e;font-size:.72rem;text-align:right;display:block;">{mode_lbl}</span>', unsafe_allow_html=True)

# ── Auto-refresh ──────────────────────────────────────────────────────────────

if not DEMO_MODE:
    time.sleep(60)
    st.rerun()
