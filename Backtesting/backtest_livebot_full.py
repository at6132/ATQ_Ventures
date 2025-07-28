import pandas as pd
import numpy as np
import asyncio
from collections import deque
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import multiprocessing as mp
from functools import lru_cache
import warnings
warnings.filterwarnings('ignore')

# --- Configuration ---
PARAMS = {
    'position_pct': 25,  # 25% position size
    'leverage': 25,      # 25x leverage
    'tp': 0.0025,        # 0.25% take profit
    'sl': 0.0015,        # 0.15% stop loss
    'fees': 0.0005,      # 0.05% round trip fees
    'min_seconds_between_entries': 5,
    'trade_duration_timeout': 300  # 5 minutes
}

# --- Global State ---
state = {
    'position': None,
    'trade_info': None,
    'armed': None,
    'last_entry_at': None,
    'vodd_history': [],
    'vet_compression': False
}

bars = deque(maxlen=120)
trades = []

# --- Optimized Helper Functions ---
def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {msg}")

@lru_cache(maxsize=10000)
def check_vodd_streak_cached(vodd_tuple):
    """Cached version of VODD streak check"""
    if len(vodd_tuple) < 3:
        return None
    last3 = vodd_tuple[-3:]
    if last3[0] < last3[1] < last3[2] and all(x > 0.4 for x in last3) and last3[2] > 0.5:
        return 'long'
    if last3[0] > last3[1] > last3[2] and all(x < -0.35 for x in last3) and last3[2] < -0.45:
        return 'short'
    return None

def check_vodd_streak(vodd_scores):
    """Check for 3-in-a-row VODD streak"""
    return check_vodd_streak_cached(tuple(vodd_scores[-3:]))

def get_vodd_score_for_candle_vectorized(candle_ts, vodd_df):
    """Vectorized VODD score lookup"""
    candle_time = pd.to_datetime(candle_ts, unit='s')
    idx = vodd_df['timestamp'].searchsorted(candle_time, side='right') - 1
    if idx < 0:
        return None, None
    row = vodd_df.iloc[idx]
    return row['direction_score'], row['timestamp']

def check_vet_trigger_vectorized(candle, bars_list):
    """Vectorized VET trigger check"""
    if len(bars_list) < 120:
        return False, None
    
    # Convert to numpy arrays for faster computation
    temp = pd.DataFrame(list(bars_list)[-120:])
    closes = temp['close'].values
    highs = temp['high'].values
    lows = temp['low'].values
    trade_counts = temp['trade_count'].values
    
    # Vectorized returns calculation
    returns = np.diff(closes) / (closes[:-1] + 1e-8)
    
    if len(returns) < 61:
        return False, None
    
    # Vectorized volatility calculation
    vol_60s = np.std(returns[-60:])
    
    # Vectorized rolling calculations
    vol_history = []
    for i in range(61, len(returns)+1):
        vol_history.append(np.std(returns[max(0, i-60):i]))
    
    if len(vol_history) < 10:
        return False, None
    
    volz = (vol_60s - np.mean(vol_history)) / (np.std(vol_history) + 1e-8)
    range_60s = np.mean(highs[-60:] - lows[-60:])
    
    # Vectorized range calculations
    range_history = []
    for i in range(61, len(highs)+1):
        range_history.append(np.mean(highs[max(0, i-60):i] - lows[max(0, i-60):i]))
    
    range_z = (range_60s - np.mean(range_history)) / (np.std(range_history) + 1e-8)
    avg_trade_count_60s = np.mean(trade_counts[-60:])
    
    # Vectorized trade count calculations
    tc_history = []
    for i in range(61, len(trade_counts)+1):
        tc_history.append(np.mean(trade_counts[max(0, i-60):i]))
    
    tc_z = (trade_counts[-1] - avg_trade_count_60s) / (np.std(tc_history) + 1e-8)
    return_now = returns[-1] if len(returns) > 0 else 0
    
    # Check for compression
    if volz < -0.5 and range_z < -0.5 and tc_z < -0.5:
        state['vet_compression'] = True
    
    # Check for expansion
    if state['vet_compression'] and (range_z > 1.0 or tc_z > 1.0 or abs(return_now) > 0.001):
        state['vet_compression'] = False
        if return_now > 0:
            return True, 'long'
        elif return_now < 0:
            return True, 'short'
    
    return False, None

async def process_candle_batch(candle_batch, vodd_df):
    """Process a batch of candles asynchronously"""
    results = []
    
    for candle in candle_batch:
        # Check VET trigger
        vet_triggered, vet_side = check_vet_trigger_vectorized(candle, bars)
        
        # Get VODD score
        vodd_score, vodd_time = get_vodd_score_for_candle_vectorized(candle['timestamp'], vodd_df)
        
        results.append({
            'candle': candle,
            'vet_triggered': vet_triggered,
            'vet_side': vet_side,
            'vodd_score': vodd_score,
            'vodd_time': vodd_time
        })
    
    return results

def handle_candle_vodd_vet_optimized(candle, vodd_df, vet_triggered, vet_side, vodd_score):
    """Optimized candle handler"""
    global state
    
    ts = candle['timestamp']
    
    # Entry cooldown
    if state.get('last_entry_at') and ts - state['last_entry_at'] < PARAMS['min_seconds_between_entries']:
        return
    
    if vodd_score is None:
        return
    
    # Add to VODD history
    state['vodd_history'].append(vodd_score)
    if len(state['vodd_history']) > 400:
        state['vodd_history'] = state['vodd_history'][-400:]
    
    # Check for VODD streak
    streak_side = check_vodd_streak(state['vodd_history'])
    if streak_side and state['armed'] is None and state['position'] is None:
        state['armed'] = {'side': streak_side, 'timestamp': ts}
        log(f"[VODD] Armed for {streak_side.upper()} at {ts}")
    
    # Check for VET trigger
    if state['armed'] and vet_triggered and state['position'] is None:
        if vet_side == state['armed']['side'] and abs(ts - state['armed']['timestamp']) <= 10:
            if (vet_side == 'long' and vodd_score > 0.25) or (vet_side == 'short' and vodd_score < -0.25):
                log(f"[ENTRY] VODD streak + VET ({vet_side.upper()}) confirmed at {ts}")
                enter_trade(vet_side, candle['close'], ts)
                state['armed'] = None
            else:
                log(f"[ENTRY BLOCKED] VODD confo level not met at VET: {vodd_score}")
                state['armed'] = None
        elif abs(ts - state['armed']['timestamp']) > 10:
            log(f"[VODD] Armed state expired (10s timeout)")
            state['armed'] = None

def enter_trade(side, entry_price, entry_time):
    """Enter a trade"""
    global state
    
    if state['position'] is not None:
        log("[ENTRY BLOCKED] Already in position.")
        return
    
    state['position'] = side
    state['trade_info'] = {
        'entry_price': entry_price,
        'created_at': entry_time,
        'side': side,
        'tp': PARAMS['tp'],
        'sl': PARAMS['sl']
    }
    
    log(f"[TRADE ENTERED] {side.upper()} at {entry_price}")

def check_exit_conditions_optimized(candle):
    """Optimized exit condition checker"""
    global state
    
    if state['position'] is None or state['trade_info'] is None:
        return
    
    side = state['position']
    info = state['trade_info']
    entry = info['entry_price']
    current_price = candle['close']
    current_time = candle['timestamp']
    
    # Check trade duration timeout
    trade_duration = current_time - info['created_at']
    if trade_duration >= PARAMS['trade_duration_timeout']:
        log(f"[DURATION TIMEOUT] {side.upper()} trade timeout after 5 minutes")
        exit_trade(side, current_price, current_time, 'Duration Timeout')
        return
    
    # Check stop loss
    if side == 'long':
        sl_price = entry * (1 - PARAMS['sl'])
        if current_price <= sl_price:
            log(f"[SL] LONG hit SL: {current_price:.5f} <= {sl_price:.5f}")
            exit_trade(side, current_price, current_time, 'Stop Loss')
            return
    else:  # short
        sl_price = entry * (1 + PARAMS['sl'])
        if current_price >= sl_price:
            log(f"[SL] SHORT hit SL: {current_price:.5f} >= {sl_price:.5f}")
            exit_trade(side, current_price, current_time, 'Stop Loss')
            return
    
    # Check take profit
    if side == 'long':
        tp_price = entry * (1 + PARAMS['tp'])
        if current_price >= tp_price:
            log(f"[TP] LONG hit TP: {current_price:.5f} >= {tp_price:.5f}")
            exit_trade(side, current_price, current_time, 'Take Profit')
            return
    else:  # short
        tp_price = entry * (1 - PARAMS['tp'])
        if current_price <= tp_price:
            log(f"[TP] SHORT hit TP: {current_price:.5f} <= {tp_price:.5f}")
            exit_trade(side, current_price, current_time, 'Take Profit')
            return

def exit_trade(side, exit_price, exit_time, reason):
    """Exit a trade"""
    global state
    
    if state['position'] is None:
        return
    
    entry_price = state['trade_info']['entry_price']
    entry_time = state['trade_info']['created_at']
    
    # Calculate P&L
    if side == 'long':
        pnl_pct = (exit_price - entry_price) / entry_price
    else:  # short
        pnl_pct = (entry_price - exit_price) / entry_price
    
    # Apply fees
    pnl_pct -= PARAMS['fees']
    
    # Calculate actual P&L in USDT
    position_size = PARAMS['position_pct'] / 100  # 25% = 0.25
    notional_value = position_size * PARAMS['leverage']  # 0.25 * 25 = 6.25
    pnl_usdt = pnl_pct * notional_value
    
    trade_record = {
        'entry_time': entry_time,
        'exit_time': exit_time,
        'side': side,
        'entry_price': entry_price,
        'exit_price': exit_price,
        'pnl_pct': pnl_pct,
        'pnl_usdt': pnl_usdt,
        'reason': reason,
        'duration_seconds': exit_time - entry_time
    }
    
    trades.append(trade_record)
    
    log(f"[TRADE EXITED] {side.upper()} at {exit_price} - P&L: {pnl_pct*100:.3f}% ({pnl_usdt:.4f} USDT) - {reason}")
    
    # Reset state
    state['position'] = None
    state['trade_info'] = None
    state['last_entry_at'] = exit_time

async def run_backtest_async():
    """Run the backtest with async optimizations"""
    global bars, state
    
    log("Starting optimized backtest...")
    
    # Load data
    try:
        data_df = pd.read_csv('data.csv')
        direction_df = pd.read_csv('direction_score.csv')
    except FileNotFoundError as e:
        log(f"Error: {e}")
        return
    
    # Process direction_score.csv
    direction_df.columns = ['timestamp', 'direction_score']
    direction_df['timestamp'] = pd.to_datetime(direction_df['timestamp'], format='%Y-%m-%d %H:%M:%S')
    direction_df['direction_score'] = pd.to_numeric(direction_df['direction_score'], errors='coerce')
    direction_df = direction_df.dropna()
    
    # Process data.csv
    if len(data_df.columns) == 8:
        data_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'trade_count', 'notional']
    elif len(data_df.columns) == 12:
        data_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'ignore1', 'ignore2', 'trade_count', 'ignore3', 'ignore4', 'ignore5']
    else:
        data_df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'trade_count']
    
    data_df = data_df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'trade_count']]
    data_df = data_df.dropna()
    
    log(f"Loaded {len(data_df)} candles and {len(direction_df)} VODD scores")
    
    # Convert to list of dictionaries for faster processing
    candles = []
    for idx, row in data_df.iterrows():
        candle = {
            'timestamp': float(row['timestamp']),
            'open': float(row['open']),
            'high': float(row['high']),
            'low': float(row['low']),
            'close': float(row['close']),
            'volume': float(row['volume']),
            'trade_count': int(row['trade_count'])
        }
        candles.append(candle)
    
    # Process candles in batches for better performance
    batch_size = 100
    total_candles = len(candles)
    
    for i in range(0, total_candles, batch_size):
        batch = candles[i:i+batch_size]
        
        if i % 1000 == 0:
            log(f"Processing batch {i//batch_size + 1}/{(total_candles + batch_size - 1)//batch_size} ({i/total_candles*100:.1f}%)")
        
        # Process batch
        for candle in batch:
            # Add to bars
            bars.append(candle)
            
            # Get VODD score
            vodd_score, vodd_time = get_vodd_score_for_candle_vectorized(candle['timestamp'], direction_df)
            
            # Check VET trigger
            vet_triggered, vet_side = check_vet_trigger_vectorized(candle, bars)
            
            # Handle VODD+VET logic
            handle_candle_vodd_vet_optimized(candle, direction_df, vet_triggered, vet_side, vodd_score)
            
            # Check exit conditions
            check_exit_conditions_optimized(candle)
    
    # Close any remaining position at the end
    if state['position'] is not None:
        last_candle = candles[-1]
        exit_trade(state['position'], last_candle['close'], last_candle['timestamp'], 'End of Data')
    
    # Calculate results
    calculate_results()

def calculate_results():
    """Calculate and display backtest results"""
    if not trades:
        log("No trades executed")
        return
    
    df = pd.DataFrame(trades)
    
    # Basic stats
    total_trades = len(df)
    winning_trades = len(df[df['pnl_usdt'] > 0])
    losing_trades = len(df[df['pnl_usdt'] < 0])
    win_rate = winning_trades / total_trades * 100
    
    total_pnl = df['pnl_usdt'].sum()
    avg_pnl = df['pnl_usdt'].mean()
    max_profit = df['pnl_usdt'].max()
    max_loss = df['pnl_usdt'].min()
    
    # Drawdown calculation
    cumulative_pnl = df['pnl_usdt'].cumsum()
    running_max = cumulative_pnl.expanding().max()
    drawdown = (cumulative_pnl - running_max) / running_max * 100
    max_drawdown = drawdown.min()
    
    # Consecutive losses
    consecutive_losses = 0
    max_consecutive_losses = 0
    for pnl in df['pnl_usdt']:
        if pnl < 0:
            consecutive_losses += 1
            max_consecutive_losses = max(max_consecutive_losses, consecutive_losses)
        else:
            consecutive_losses = 0
    
    # Trade duration stats
    avg_duration = df['duration_seconds'].mean()
    
    # Print results
    print("\n" + "="*60)
    print("BACKTEST RESULTS")
    print("="*60)
    print(f"Total Trades: {total_trades}")
    print(f"Winning Trades: {winning_trades}")
    print(f"Losing Trades: {losing_trades}")
    print(f"Win Rate: {win_rate:.2f}%")
    print(f"Total P&L: {total_pnl:.4f} USDT")
    print(f"Average P&L per Trade: {avg_pnl:.4f} USDT")
    print(f"Max Profit: {max_profit:.4f} USDT")
    print(f"Max Loss: {max_loss:.4f} USDT")
    print(f"Max Drawdown: {max_drawdown:.2f}%")
    print(f"Max Consecutive Losses: {max_consecutive_losses}")
    print(f"Average Trade Duration: {avg_duration:.1f} seconds")
    
    # ROI calculation (assuming 1 USDT account)
    roi = total_pnl * 100  # Since position size is 25% of account
    print(f"ROI: {roi:.2f}%")
    
    # Trade breakdown by reason
    print("\nTrade Breakdown by Exit Reason:")
    reason_counts = df['reason'].value_counts()
    for reason, count in reason_counts.items():
        print(f"  {reason}: {count} trades")
    
    # Save trades to CSV
    df.to_csv('backtest_trades.csv', index=False)
    print(f"\nTrades saved to 'backtest_trades.csv'")

def run_backtest():
    """Synchronous wrapper for async backtest"""
    asyncio.run(run_backtest_async())

if __name__ == "__main__":
    run_backtest() 