# backtesting/backtest.py
# Uses FREE public Coinbase historical data — no API key needed
# Tests RSI + MACD + Bollinger strategy on 90 days of real price history

import ccxt
import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator
from ta.trend import MACD, EMAIndicator
from ta.volatility import BollingerBands
from datetime import datetime

# ─── SETTINGS (must match your live .env settings) ───────────────────────────
FEE_RATE        = 0.006   # 0.6% Coinbase fee
TRADE_SIZE_PCT  = 0.02    # 2% of balance per trade
STOP_LOSS_PCT   = 0.03    # 3% stop loss
TAKE_PROFIT_PCT = 0.08    # 8% take profit
START_BALANCE   = 10000   # Virtual $10,000

# ─── SYMBOLS TO TEST ─────────────────────────────────────────────────────────
SYMBOLS = ['BTC-USD', 'ETH-USD']

def fetch_historical(symbol, timeframe='1h', days=365):
    import yfinance as yf
    from datetime import datetime, timedelta

    print(f"Fetching {days} days of {symbol} data from Yahoo Finance...")

    end = datetime.now()
    start = end - timedelta(days=days)

    ticker = yf.download(symbol, start=start, end=end, 
                         interval='1h', progress=False, 
                         auto_adjust=True)

    if ticker.empty:
        print(f"  No data for {symbol}")
        return None

    # Fix multi-level columns
    if isinstance(ticker.columns, pd.MultiIndex):
        ticker.columns = ticker.columns.get_level_values(0)
    
    # Lowercase all columns
    ticker.columns = [c.lower() for c in ticker.columns]
    
    # Reset index — Datetime becomes a column
    df = ticker.reset_index()
    print("DEBUG columns:", df.columns.tolist())
    print("DEBUG head:", df.head(2))
    # Rename whatever the time column is called
    for col in ['Datetime', 'datetime', 'date', 'index']:

        if col in df.columns:
            df = df.rename(columns={col: 'time'})
            break

    df = df[['time', 'open', 'high', 'low', 'close', 'volume']]
    df = df.dropna()

    print(f"  Got {len(df)} candles from {df['time'].iloc[0].date()} to {df['time'].iloc[-1].date()}")
    return df

    
def calculate_indicators(df):
    """
    Calculate all technical indicators.
    Pure math — same input always gives same output.
    """
    # RSI — momentum oscillator
    df['rsi'] = RSIIndicator(df['close'], window=14).rsi()

    # MACD — trend following
    macd = MACD(df['close'])
    df['macd_diff'] = macd.macd_diff()
    df['macd_prev'] = df['macd_diff'].shift(1)

    # Bollinger Bands — volatility
    bb = BollingerBands(df['close'], window=20, window_dev=2)
    df['bb_upper'] = bb.bollinger_hband()
    df['bb_lower'] = bb.bollinger_lband()
    df['bb_mid']   = bb.bollinger_mavg()
    df['bb_pct']   = (df['close'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'])

    # EMA trend filter — 50 and 200
    df['ema50']  = EMAIndicator(df['close'], window=50).ema_indicator()
    df['ema200'] = EMAIndicator(df['close'], window=200).ema_indicator()
    df['trend']  = np.where(df['ema50'] > df['ema200'], 'bull', 'bear')

    return df

def get_signal(row, prev_row):
    if pd.isna(row['rsi']) or pd.isna(row['macd_diff']) or pd.isna(row['bb_pct']) or pd.isna(row['ema50']):
        return 'HOLD'

    # Only trade WITH the trend
    uptrend   = row['ema50'] > row['ema200']
    downtrend = row['ema50'] < row['ema200']

    # BUY — uptrend + 2 of 3 signals
    buy_signals = 0
    if row['rsi'] < 45:                                    buy_signals += 1
    if row['macd_diff'] > 0 and row['macd_prev'] <= 0:     buy_signals += 1
    if row['bb_pct'] < 0.35:                               buy_signals += 1

    # SELL — downtrend + 2 of 3 signals
    sell_signals = 0
    if row['rsi'] > 55:                                    sell_signals += 1
    if row['macd_diff'] < 0 and row['macd_prev'] >= 0:     sell_signals += 1
    if row['bb_pct'] > 0.65:                               sell_signals += 1

    if uptrend   and buy_signals  >= 2:   return 'BUY'
    if sell_signals >= 2:                 return 'SELL'
    return 'HOLD'


def run_backtest(symbol, timeframe='1h', days=90):
    """
    Run full backtest with IDENTICAL logic to live trading system.
    Same position sizing, same fees, same stops — results are meaningful.
    """
    df = fetch_historical(symbol, timeframe, days)
    if df is None:
        return None

    df = calculate_indicators(df)

    # ─── SIMULATION ──────────────────────────────────────────────────────────
    balance   = START_BALANCE
    position  = None
    trades    = []
    equity    = []  # Track balance over time

    for i in range(1, len(df)):
        row      = df.iloc[i]
        prev_row = df.iloc[i - 1]
        price    = row['close']

        equity.append({'time': row['time'], 'balance': balance})

        # ── CHECK EXITS FIRST (stop-loss / take-profit) ────────────────────
        if position:
            pnl_pct = (price - position['entry']) / position['entry']

            hit_stop   = pnl_pct <= -STOP_LOSS_PCT
            hit_target = pnl_pct >= TAKE_PROFIT_PCT

            if hit_stop or hit_target:
                usd_returned = position['qty'] * price
                fee          = usd_returned * FEE_RATE
                net_returned = usd_returned - fee
                pnl_usd      = net_returned - position['cost']
                balance     += net_returned

                trades.append({
                    'type':       'SELL',
                    'symbol':     symbol,
                    'entry':      position['entry'],
                    'exit':       price,
                    'pnl_usd':    pnl_usd,
                    'pnl_pct':    pnl_pct,
                    'exit_time':  row['time'],
                    'entry_time': position['entry_time'],
                    'reason':     'STOP_LOSS' if hit_stop else 'TAKE_PROFIT',
                })
                position = None
                continue

        # ── CHECK ENTRY SIGNALS ────────────────────────────────────────────
        signal = get_signal(row, prev_row)

        if signal == 'BUY' and position is None:
            usd_amount = balance * TRADE_SIZE_PCT   # Always exactly 2%
            fee        = usd_amount * FEE_RATE
            total_cost = usd_amount + fee

            if total_cost > balance:
                continue

            balance  -= total_cost
            qty       = usd_amount / price
            position  = {
                'qty':        qty,
                'entry':      price,
                'cost':       usd_amount,
                'entry_time': row['time'],
            }
            trades.append({
                'type':   'BUY',
                'symbol': symbol,
                'price':  price,
                'time':   row['time'],
            })

        elif signal == 'SELL' and position is not None:
            usd_returned = position['qty'] * price
            fee          = usd_returned * FEE_RATE
            net_returned = usd_returned - fee
            pnl_usd      = net_returned - position['cost']
            pnl_pct      = (price - position['entry']) / position['entry']
            balance     += net_returned

            trades.append({
                'type':       'SELL',
                'symbol':     symbol,
                'entry':      position['entry'],
                'exit':       price,
                'pnl_usd':    pnl_usd,
                'pnl_pct':    pnl_pct,
                'exit_time':  row['time'],
                'entry_time': position['entry_time'],
                'reason':     'SIGNAL',
            })
            position = None

    # Close any open position at end
    if position:
        price        = df['close'].iloc[-1]
        usd_returned = position['qty'] * price
        fee          = usd_returned * FEE_RATE
        net_returned = usd_returned - fee
        pnl_usd      = net_returned - position['cost']
        pnl_pct      = (price - position['entry']) / position['entry']
        balance     += net_returned
        trades.append({
            'type': 'SELL', 'symbol': symbol,
            'entry': position['entry'], 'exit': price,
            'pnl_usd': pnl_usd, 'pnl_pct': pnl_pct,
            'reason': 'END_OF_TEST',
        })

    # ─── CALCULATE RESULTS ───────────────────────────────────────────────────
    sell_trades = [t for t in trades if t['type'] == 'SELL']
    wins        = [t for t in sell_trades if t.get('pnl_usd', 0) > 0]
    losses      = [t for t in sell_trades if t.get('pnl_usd', 0) <= 0]

    total_return  = (balance - START_BALANCE) / START_BALANCE * 100
    win_rate      = len(wins) / len(sell_trades) * 100 if sell_trades else 0
    avg_win       = sum(t['pnl_usd'] for t in wins) / len(wins) if wins else 0
    avg_loss      = sum(t['pnl_usd'] for t in losses) / len(losses) if losses else 0
    profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')

    # Max drawdown
    eq_values  = [e['balance'] for e in equity]
    peak       = START_BALANCE
    max_dd     = 0
    for val in eq_values:
        if val > peak:
            peak = val
        dd = (peak - val) / peak
        if dd > max_dd:
            max_dd = dd

    # ─── PRINT RESULTS ───────────────────────────────────────────────────────
    print(f"\n{'='*55}")
    print(f"  BACKTEST RESULTS: {symbol} | {timeframe} | {days} days")
    print(f"{'='*55}")
    print(f"  Starting Balance:  ${START_BALANCE:,.2f}")
    print(f"  Final Balance:     ${balance:,.2f}")
    print(f"  Total Return:      {total_return:+.2f}%")
    print(f"  {'─'*45}")
    print(f"  Total Trades:      {len(sell_trades)}")
    print(f"  Win Rate:          {win_rate:.1f}%  (need >55%)")
    print(f"  Profit Factor:     {profit_factor:.2f}  (need >1.3)")
    print(f"  Max Drawdown:      {max_dd*100:.1f}%  (need <8%)")
    print(f"  {'─'*45}")
    print(f"  Avg Win:           ${avg_win:+.2f}")
    print(f"  Avg Loss:          ${avg_loss:+.2f}")
    print(f"  {'─'*45}")

    # Pass/Fail assessment
    passed = (
        win_rate > 55 and
        profit_factor > 1.3 and
        max_dd < 0.08 and
        total_return > 0 and
        len(sell_trades) >= 5
    )
    print(f"  VERDICT: {'✅ PASSED — strategy shows promise' if passed else '❌ FAILED — refine before live trading'}")
    print(f"{'='*55}\n")

    return {
        'symbol': symbol,
        'return': total_return,
        'trades': len(sell_trades),
        'win_rate': win_rate,
        'profit_factor': profit_factor,
        'max_drawdown': max_dd * 100,
        'final_balance': balance,
        'passed': passed,
    }

if __name__ == '__main__':
    print("\n🚀 Starting Crypto Orchestra Backtester")
    print("Using FREE Coinbase public data — no API key needed\n")

    results = []
    for symbol in SYMBOLS:
        result = run_backtest(symbol, timeframe='1h', days=365)
        if result:
            results.append(result)

    # Overall summary
    print("\n📊 OVERALL SUMMARY")
    print("="*55)
    all_passed = all(r['passed'] for r in results)
    for r in results:
        status = "✅ PASS" if r['passed'] else "❌ FAIL"
        print(f"  {r['symbol']:12} {status}  Return: {r['return']:+.1f}%  WinRate: {r['win_rate']:.0f}%")

    print(f"\n  Overall: {'✅ Strategy ready for paper trading!' if all_passed else '❌ Needs refinement before paper trading'}")
    print("="*55)
