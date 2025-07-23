import asyncio
import websockets
import json
import time
import requests
import numpy as np
from collections import deque, defaultdict
from dateutil import parser

# --- Config ---
KRAKEN_WS = 'wss://ws.kraken.com/v2'
SYMBOL = 'BTC/USDT'
API = 'http://localhost:8000'  # Paper trading backend
PARAMS = {
    'efficiency': 0.6296476872180908,
    'spike': 1.6204901077650953,
    'volz': 0.18480024200040857,
    'volz_fade': 0.43995860492385874,
    'tp': 0.001797536477942073,
    'trailing': 0.0005036613263692868,
    'timeout': 279.4239361683253,
    'risk_per_trade': 0.05,
    'leverage': 100,
}

# --- State ---
bar = None
bars = deque(maxlen=400)
trade_counts = deque(maxlen=400)
vols = deque(maxlen=400)

# --- Feature & Strategy Logic ---
def run_strategy():
    if len(bars) < 61:
        return
    df = list(bars)
    last = df[-1]
    # Features
    efficiency = abs(last['close'] - last['open']) / (last['high'] - last['low'] + 1e-8)
    trade_count_60s = np.mean([b['trade_count'] for b in df[-60:]])
    trade_spike = last['trade_count'] / (trade_count_60s + 1e-8)
    returns = np.array([b['close'] for b in df])
    returns = np.diff(returns) / returns[:-1]
    vol_60s = np.std(returns[-60:])
    vol_300s = np.std(returns[-300:]) if len(returns) >= 300 else np.std(returns)
    volz = (vol_60s - np.mean(returns[-300:])) / (np.std(returns[-300:]) + 1e-8) if len(returns) >= 300 else 0
    # Check open position
    pos = get_open_position()
    # Entry logic
    if not pos:
        # Long
        if efficiency > PARAMS['efficiency'] and trade_spike > PARAMS['spike'] and volz > PARAMS['volz']:
            place_order('buy', last['close'])
        # Short
        elif efficiency > PARAMS['efficiency'] and trade_spike > PARAMS['spike'] and volz < PARAMS['volz_fade']:
            place_order('sell', last['close'])
    else:
        # Exit logic
        entry = pos['entry_price']
        price = last['close']
        side = pos['side']
        if side == 'long':
            ret = (price - entry) / entry
            peak = max(price, entry)
        else:
            ret = (entry - price) / entry
            peak = min(price, entry)
        # Take profit
        if ret >= PARAMS['tp']:
            close_position(pos, price)
        # Trailing stop
        elif (peak - price) / entry >= PARAMS['trailing']:
            close_position(pos, price)
        # Timeout
        else:
            created_at = pos['created_at']
            if isinstance(created_at, str):
                try:
                    created_at = float(created_at)
                except ValueError:
                    from dateutil import parser
                    created_at = parser.isoparse(created_at).timestamp()
            elif isinstance(created_at, float):
                pass
            else:
                created_at = float(created_at)
            if time.time() - created_at >= PARAMS['timeout']:
                close_position(pos, price)

def update_ohlcv_bar(candle):
    # Convert candle fields to match previous bar structure
    bars.append({
        'open': float(candle['open']),
        'high': float(candle['high']),
        'low': float(candle['low']),
        'close': float(candle['close']),
        'volume': float(candle['volume']),
        'trade_count': int(candle['trades']),
        'timestamp': candle['interval_begin']
    })
    if len(bars) > 61:
        run_strategy()

# --- Paper Trading API ---
def get_open_position():
    r = requests.get(f'{API}/positions')
    positions = r.json()
    return positions[0] if positions else None

def place_order(side, price):
    # 5% of balance, 100x leverage
    acc = requests.get(f'{API}/account').json()
    balance = acc['balance']
    qty = (balance * PARAMS['risk_per_trade'] * PARAMS['leverage']) / price
    print(f"Placing {side.upper()} order: price={price}, qty={qty:.6f}, leverage={PARAMS['leverage']}")
    order = {
        'symbol': 'BTCUSDT',
        'side': side,
        'type': 'market',
        'price': price,
        'quantity': qty,
        'leverage': PARAMS['leverage'],
    }
    requests.post(f'{API}/orders', json=order)


def close_position(pos, price):
    side = 'sell' if pos['side'] == 'long' else 'buy'
    qty = pos['quantity']
    print(f"Attempting to close: symbol={pos['symbol']}, side={side}, qty={qty}, price={price}")
    order = {
        'symbol': pos['symbol'],
        'side': side,
        'type': 'reduce_only',  # Use reduce_only for close orders
        'price': price,
        'quantity': qty,
        'leverage': pos['leverage'],
    }
    print("Close order payload:", order)
    import requests
    resp = requests.post(f'{API}/orders', json=order)
    print("Close order response:", resp.status_code, resp.text)

# --- Entrypoint for FastAPI ---

def start_kraken_live():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(kraken_ws_loop()) 