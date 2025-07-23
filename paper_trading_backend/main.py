from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional, Literal
from uuid import uuid4
import datetime
import threading
from .binance_live import start_binance_live
import time
import pandas as pd
import os
from .kraken_live import bars
import requests
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Models ---
class OrderRequest(BaseModel):
    symbol: str
    side: Literal['buy', 'sell']
    type: Literal['market', 'limit', 'stop', 'post_only', 'reduce_only', 'oco']
    price: Optional[float] = None
    quantity: float
    leverage: int = 1
    tp: Optional[float] = None
    sl: Optional[float] = None
    client_order_id: Optional[str] = None

class Order(BaseModel):
    order_id: str
    symbol: str
    side: str
    type: str
    price: Optional[float]
    quantity: float
    leverage: int
    tp: Optional[float]
    sl: Optional[float]
    status: Literal['open', 'filled', 'cancelled']
    created_at: datetime.datetime
    filled_at: Optional[datetime.datetime] = None
    client_order_id: Optional[str] = None

class Position(BaseModel):
    symbol: str
    side: Literal['long', 'short']
    entry_price: float
    quantity: float
    leverage: int
    unrealized_pnl: float
    liquidation_price: float
    margin: float
    created_at: datetime.datetime

class Trade(BaseModel):
    trade_id: str
    order_id: str
    symbol: str
    side: str
    price: float
    quantity: float
    leverage: int
    pnl: float
    timestamp: datetime.datetime

class Account(BaseModel):
    balance: float
    equity: float
    margin_used: float
    margin_available: float
    positions: List[Position]
    open_orders: List[Order]
    realized_pnl: float
    unrealized_pnl: float

# --- In-memory state ---
ACCOUNT = {
    'balance': 100000.0,
    'realized_pnl': 0.0,
    'positions': {},  # symbol -> Position
    'orders': {},     # order_id -> Order
    'trades': [],     # List[Trade]
}

ACCOUNT_STATE_FILE = 'account_state.json'

def save_account_state():
    state = {
        'balance': ACCOUNT['balance'],
        'realized_pnl': ACCOUNT['realized_pnl'],
        'positions': ACCOUNT['positions'],
        'orders': ACCOUNT['orders'],
    }
    with open(ACCOUNT_STATE_FILE, 'w') as f:
        json.dump(state, f, default=str)

def load_account_state():
    import os
    import pandas as pd
    # Reconstruct balance from trades_log.csv
    initial_balance = 100000.0
    trade_pnl = 0.0
    if os.path.exists('trades_log.csv'):
        try:
            df = pd.read_csv('trades_log.csv')
            closed = df[df['exit_price'].notnull() & (df['exit_price'] != '')]
            closed['pnl'] = closed['pnl'].astype(float)
            trade_pnl = closed['pnl'].sum()
        except Exception as e:
            print(f"Error reading trades_log.csv: {e}")
    ACCOUNT['balance'] = initial_balance + trade_pnl
    # Load realized_pnl for analytics
    ACCOUNT['realized_pnl'] = trade_pnl
    # Load positions and orders from account_state.json if exists
    ACCOUNT['positions'] = {}
    ACCOUNT['orders'] = {}
    if os.path.exists(ACCOUNT_STATE_FILE):
        with open(ACCOUNT_STATE_FILE, 'r') as f:
            state = json.load(f)
            # Only restore open positions and orders
            ACCOUNT['positions'] = state.get('positions', {})
            ACCOUNT['orders'] = state.get('orders', {})

# Load state on startup
load_account_state()

# --- Helper functions ---
def get_equity():
    # Equity = cash + margin in open positions + unrealized PnL
    cash = ACCOUNT['balance']
    margin = sum(p['margin'] for p in ACCOUNT['positions'].values())
    unrealized = sum(p['unrealized_pnl'] for p in ACCOUNT['positions'].values())
    return cash + margin + unrealized

def get_margin_used():
    return sum(p['margin'] for p in ACCOUNT['positions'].values())

def get_margin_available():
    return get_equity() - get_margin_used()

def calc_liquidation_price(entry_price, leverage, side):
    # Simple cross margin liquidation formula (ignores funding/fees for now)
    if side == 'long':
        return entry_price * (1 - 1 / leverage)
    else:
        return entry_price * (1 + 1 / leverage)

def update_unrealized_pnl(symbol, price):
    pos = ACCOUNT['positions'].get(symbol)
    if not pos:
        return
    if pos['side'] == 'long':
        pnl = (price - pos['entry_price']) * pos['quantity'] * pos['leverage']
    else:
        pnl = (pos['entry_price'] - price) * pos['quantity'] * pos['leverage']
    pos['unrealized_pnl'] = pnl
    pos['liquidation_price'] = calc_liquidation_price(pos['entry_price'], pos['leverage'], pos['side'])

def check_liquidation(symbol, price):
    pos = ACCOUNT['positions'].get(symbol)
    if not pos:
        return False
    if pos['side'] == 'long' and price <= pos['liquidation_price']:
        return True
    if pos['side'] == 'short' and price >= pos['liquidation_price']:
        return True
    return False

def update_all_unrealized_pnl():
    while True:
        if bars:
            current_price = bars[-1]['close']
            for p in ACCOUNT['positions'].values():
                qty = p['quantity']
                entry = p['entry_price']
                side = p['side']
                # leverage = p['leverage']  # Not used in PnL calculation
                if side == 'long':
                    pnl = (current_price - entry) * qty
                else:
                    pnl = (entry - current_price) * qty
                p['unrealized_pnl'] = pnl
        time.sleep(1)

TELEGRAM_BOT_TOKEN = '8156711122:AAFYoW3ESDlxAjSfHO_DkjabgKZ3aUc3oRI'
TELEGRAM_CHAT_ID = '7168811895'
TELEGRAM_API_URL = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}'

last_update_id = None

def send_telegram_message(text):
    url = f"{TELEGRAM_API_URL}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown"
    }
    try:
        requests.post(url, data=payload, timeout=5)
    except Exception as e:
        print(f"Telegram send error: {e}")

def poll_telegram():
    global last_update_id
    print("[Telegram Poller] Started.")
    while True:
        try:
            resp = requests.get(f"{TELEGRAM_API_URL}/getUpdates", timeout=10)
            data = resp.json()
            if not data.get('ok'):
                time.sleep(10)
                continue
            for update in data['result']:
                update_id = update['update_id']
                if last_update_id is not None and update_id <= last_update_id:
                    continue
                last_update_id = update_id
                message = update.get('message', {})
                text = message.get('text', '').strip()
                if not text:
                    continue
                if text == '/balance':
                    # If no open positions, reconstruct balance from trade log
                    if not ACCOUNT['positions']:
                        import os, pandas as pd
                        initial_balance = 100000.0
                        trade_pnl = 0.0
                        if os.path.exists('trades_log.csv'):
                            try:
                                df = pd.read_csv('trades_log.csv')
                                closed = df[df['exit_price'].notnull() & (df['exit_price'] != '')]
                                closed['pnl'] = closed['pnl'].astype(float)
                                trade_pnl = closed['pnl'].sum()
                            except Exception as e:
                                send_telegram_message(f"Error reading trades_log.csv: {e}")
                        bal = initial_balance + trade_pnl
                        send_telegram_message(f"💰 Balance: {bal:.2f}")
                    else:
                        send_telegram_message(f"💰 Balance: {get_equity():.2f}")
                elif text == '/analytics':
                    try:
                        df = pd.read_csv('trades_log.csv')
                        # Only count trades with both entry and exit price
                        closed = df[df['exit_price'].notnull() & (df['exit_price'] != '')]
                        closed = closed.copy()
                        closed['timestamp'] = pd.to_datetime(closed['timestamp'])
                        closed['pnl'] = closed['pnl'].astype(float)
                        today = pd.Timestamp.now().normalize()
                        today_trades = closed[closed['timestamp'] >= today]
                        total_pnl = closed['pnl'].sum()
                        total_win = (closed['pnl'] > 0).sum()
                        total_trades = len(closed)
                        total_winrate = (total_win / total_trades * 100) if total_trades else 0
                        daily_pnl = today_trades['pnl'].sum()
                        daily_win = (today_trades['pnl'] > 0).sum()
                        daily_trades = len(today_trades)
                        daily_winrate = (daily_win / daily_trades * 100) if daily_trades else 0
                        msg = (
                            f"📊 *Analytics*\n"
                            f"Today: PnL = {daily_pnl:.2f}, Winrate = {daily_winrate:.1f}% ({daily_win}/{daily_trades})\n"
                            f"Total: PnL = {total_pnl:.2f}, Winrate = {total_winrate:.1f}% ({total_win}/{total_trades})"
                        )
                        send_telegram_message(msg)
                    except Exception as e:
                        send_telegram_message(f"Error reading analytics: {e}")
        except Exception as e:
            print(f"Telegram polling error: {e}")
        time.sleep(10)

def close_position(pos, price):
    side = 'sell' if pos['side'] == 'long' else 'buy'
    qty = pos['quantity']
    entry_price = pos['entry_price']
    leverage = pos['leverage']
    margin = pos['margin']
    # Calculate realized PnL (no leverage multiplier)
    if pos['side'] == 'long':
        pnl = (price - entry_price) * qty
    else:
        pnl = (entry_price - price) * qty
    print(f"Closing {pos['side']} position: entry={entry_price}, exit={price}, qty={qty:.6f}, pnl={pnl:.2f}, leverage={leverage}")
    t = {
        'trade_id': str(uuid4()),
        'order_id': '',  # Not tracked in close_position, can be filled if needed
        'symbol': pos['symbol'],
        'side': pos['side'],
        'entry_price': entry_price,
        'exit_price': price,
        'quantity': qty,
        'leverage': leverage,
        'pnl': pnl,
        'timestamp': datetime.datetime.utcnow()
    }
    ACCOUNT['trades'].append(t)
    # Save to CSV
    import pandas as pd, os
    trade_df = pd.DataFrame([t])
    file_exists = os.path.isfile('trades_log.csv')
    trade_df.to_csv('trades_log.csv', mode='a', header=not file_exists, index=False)
    # Update cash and realized PnL
    ACCOUNT['balance'] += margin + pnl
    ACCOUNT['realized_pnl'] += pnl
    # Telegram alert for close
    send_telegram_message(
        f"✅ *Closed*: {pos['symbol']} {pos['side'].upper()} @ {price}\n"
        f"Entry: {entry_price}\nExit: {price}\nQty: {qty:.4f}x{leverage}\nPnL: {pnl:.2f}\n"
        f"Balance: {get_equity():.2f}"
    )
    # Remove position
    del ACCOUNT['positions'][pos['symbol']]
    save_account_state()

# --- Endpoints ---
@app.on_event('startup')
def start_live_trading():
    # Run the Binance live trading engine in a background thread
    threading.Thread(target=start_binance_live, daemon=True).start()
    # Start unrealized PnL updater
    threading.Thread(target=update_all_unrealized_pnl, daemon=True).start()
    # Start the Telegram poller in a background thread on startup
    threading.Thread(target=poll_telegram, daemon=True).start()

@app.get('/account', response_model=Account)
def get_account():
    positions = [Position(**p) for p in ACCOUNT['positions'].values()]
    open_orders = [Order(**o) for o in ACCOUNT['orders'].values() if o['status'] == 'open']
    unrealized = sum(p.unrealized_pnl for p in positions)
    margin_used = sum(p.margin for p in positions)
    equity = ACCOUNT['balance'] + unrealized
    margin_available = equity - margin_used
    return Account(
        balance=equity,  # <-- report equity as balance
        equity=equity,
        margin_used=margin_used,
        margin_available=margin_available,
        positions=positions,
        open_orders=open_orders,
        realized_pnl=ACCOUNT['realized_pnl'],
        unrealized_pnl=unrealized
    )

@app.get('/positions', response_model=List[Position])
def get_positions():
    return [Position(**p) for p in ACCOUNT['positions'].values()]

@app.get('/orders', response_model=List[Order])
def get_orders():
    return [Order(**o) for o in ACCOUNT['orders'].values()]

@app.get('/orders/{order_id}', response_model=Order)
def get_order(order_id: str):
    o = ACCOUNT['orders'].get(order_id)
    if not o:
        raise HTTPException(404, 'Order not found')
    return Order(**o)

@app.delete('/orders/{order_id}')
def cancel_order(order_id: str):
    o = ACCOUNT['orders'].get(order_id)
    if not o or o['status'] != 'open':
        raise HTTPException(404, 'Order not open')
    o['status'] = 'cancelled'
    return {'result': 'cancelled'}

@app.get('/trades', response_model=List[Trade])
def get_trades():
    return [Trade(**t) for t in ACCOUNT['trades']]

@app.get('/trades/{trade_id}', response_model=Trade)
def get_trade(trade_id: str):
    for t in ACCOUNT['trades']:
        if t['trade_id'] == trade_id:
            return Trade(**t)
    raise HTTPException(404, 'Trade not found')

@app.get('/balances')
def get_balances():
    equity = get_equity()
    return {
        'balance': equity,  # <-- report equity as balance
        'equity': equity,
        'margin_used': get_margin_used(),
        'margin_available': get_margin_available(),
        'realized_pnl': ACCOUNT['realized_pnl'],
        'unrealized_pnl': sum(p['unrealized_pnl'] for p in ACCOUNT['positions'].values())
    }

@app.post('/orders', response_model=Order)
def create_order(order: OrderRequest):
    now = time.time()
    order_id = str(uuid4())
    fill_price = order.price if order.price else 30000.0  # TODO: use real price feed
    side = 'long' if order.side == 'buy' else 'short'
    # Check for existing position
    if order.symbol in ACCOUNT['positions']:
        pos = ACCOUNT['positions'][order.symbol]
        # Only allow explicit close (or reduce_only) orders in the exact opposite direction and matching quantity
        is_opposite = (pos['side'] == 'long' and order.side == 'sell') or (pos['side'] == 'short' and order.side == 'buy')
        is_close_type = order.type in ['close', 'reduce_only']
        is_exact_qty = abs(order.quantity - pos['quantity']) < 1e-8
        if is_opposite and is_close_type and is_exact_qty:
            close_position(pos, fill_price)
            o = {
                'order_id': order_id,
                'symbol': order.symbol,
                'side': order.side,
                'type': order.type,
                'price': order.price,
                'quantity': order.quantity,
                'leverage': order.leverage,
                'tp': order.tp,
                'sl': order.sl,
                'status': 'filled',
                'created_at': datetime.datetime.utcfromtimestamp(now),
                'filled_at': datetime.datetime.utcfromtimestamp(now),
                'client_order_id': order.client_order_id
            }
            ACCOUNT['orders'][order_id] = o
            return Order(**o)
        else:
            raise HTTPException(400, 'Position already open for symbol. Only explicit close/reduce_only order with exact opposite side and quantity is allowed.')
    # Normal open position logic
    margin = (order.quantity * fill_price) / order.leverage
    pos = {
        'symbol': order.symbol,
        'side': side,
        'entry_price': fill_price,
        'quantity': order.quantity,
        'leverage': order.leverage,
        'unrealized_pnl': 0.0,
        'liquidation_price': calc_liquidation_price(fill_price, order.leverage, side),
        'margin': margin,
        'created_at': now
    }
    ACCOUNT['positions'][order.symbol] = pos
    o = {
        'order_id': order_id,
        'symbol': order.symbol,
        'side': order.side,
        'type': order.type,
        'price': order.price,
        'quantity': order.quantity,
        'leverage': order.leverage,
        'tp': order.tp,
        'sl': order.sl,
        'status': 'filled',
        'created_at': datetime.datetime.utcfromtimestamp(now),
        'filled_at': datetime.datetime.utcfromtimestamp(now),
        'client_order_id': order.client_order_id
    }
    ACCOUNT['orders'][order_id] = o
    # Telegram alert for entry
    send_telegram_message(f"🚀 *Entry*: {order.symbol} {order.side.upper()} @ {fill_price}\nQty: {order.quantity:.4f}x{order.leverage}")
    t = {
        'trade_id': str(uuid4()),
        'order_id': order_id,
        'symbol': order.symbol,
        'side': order.side,
        'entry_price': fill_price,
        'exit_price': '',
        'quantity': order.quantity,
        'leverage': order.leverage,
        'pnl': 0.0,
        'timestamp': datetime.datetime.utcfromtimestamp(now)
    }
    ACCOUNT['trades'].append(t)
    # --- Save trade to CSV ---
    trade_df = pd.DataFrame([t])
    file_exists = os.path.isfile('trades_log.csv')
    trade_df.to_csv('trades_log.csv', mode='a', header=not file_exists, index=False)
    ACCOUNT['balance'] -= margin
    save_account_state()
    return Order(**o)

@app.post('/reset')
def reset_account():
    # Only close all open positions and cancel open orders, but keep balance and trade history
    ACCOUNT['positions'] = {}
    for o in ACCOUNT['orders'].values():
        if o['status'] == 'open':
            o['status'] = 'cancelled'
    save_account_state()
    return {'result': 'reset'}

@app.websocket('/ws/pnl')
async def ws_pnl(websocket: WebSocket):
    await websocket.accept()
    import asyncio
    while True:
        # Get current price from latest bar
        if bars:
            current_price = bars[-1]['close']
        else:
            current_price = None
        balance = ACCOUNT['balance']
        total_value = balance
        for p in ACCOUNT['positions'].values():
            qty = p['quantity']
            entry = p['entry_price']
            side = p['side']
            leverage = p['leverage']
            margin = p['margin']
            if current_price is not None:
                if side == 'long':
                    pos_value = qty * current_price
                else:  # short
                    # For shorts, value = margin + (entry - current) * qty * leverage
                    pos_value = margin + (entry - current_price) * qty * leverage
                total_value += pos_value
            else:
                # fallback: just add margin
                total_value += margin
        await websocket.send_json({"total_pnl": total_value})
        await asyncio.sleep(1) 