import requests
import time
import json
import pandas as pd
import asyncio
import websockets
from collections import deque
import numpy as np
import datetime
from mexc_python.mexcpy.api import MexcFuturesAPI as API
from mexc_python.mexcpy.mexcTypes import CreateOrderRequest, OrderSide, OrderType, OpenType
from dataclasses import asdict

# --- Config ---
USER_TOKEN = "WEB029506898cc35436d28428665685e59061ae4b900a6f95e06ab25a0341f438f1"
TELEGRAM_BOT_TOKEN = '8156711122:AAFYoW3ESDlxAjSfHO_DkjabgKZ3aUc3oRI'
TELEGRAM_CHAT_ID = '7168811895'
TELEGRAM_API_URL = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}'

mexc_api = API(token=USER_TOKEN)

PARAMS = {
    'efficiency': 0.6296,
    'spike': 1.6204,
    'volz': 0.1848,
    'volz_fade': -0.1848,  # Changed to negative to make SHORT condition symmetric
    'tp': 0.00179,
    'trailing': 0.0005,
    'timeout': 279,
    'risk_per_trade': 0.05,  # Changed to match backtester (5% risk)
    'leverage': 100,
}

bars = deque(maxlen=400)
position = None
TRADE_LOG = 'live_trades_log.csv'
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@kline_1s"

# --- Logging ---
def log(msg):
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def log_with_time(msg):
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def log_trade(trade):
    df = pd.DataFrame([trade])
    df.to_csv(TRADE_LOG, mode='a', header=not pd.io.common.file_exists(TRADE_LOG), index=False)

# --- Telegram ---
def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    requests.post(url, data=payload, timeout=5)

# --- MEXC Order Helper ---
def clean_order_payload(order):
    d = asdict(order)
    for k in ['side', 'type', 'openType']:
        if d.get(k) is not None and hasattr(d[k], 'value'):
            d[k] = d[k].value
    return {k: v for k, v in d.items() if v is not None}

async def send_order(order):
    payload = clean_order_payload(order)
    print(f"[ORDER PAYLOAD] {payload}")
    try:
        print(f"[ORDER] Attempting to send order...")
        response = await mexc_api.create_order(order)
        print(f"[ORDER] Response received: {response}")
        return response
    except Exception as e:
        print(f"[ORDER] Error sending order: {e}")
        print(f"[ORDER] Error type: {type(e)}")
        import traceback
        print(f"[ORDER] Full traceback: {traceback.format_exc()}")
        raise e

async def send_balance_update():
    # Fetch real USDT balance from MEXC
    asset_resp = await mexc_api.get_user_asset('USDT')
    usdt_bal = 0.0
    if asset_resp.success and hasattr(asset_resp.data, 'availableBalance'):
        usdt_bal = asset_resp.data.availableBalance
    msg = f"💰 USDT (USD): {usdt_bal:.2f}"
    print(f"[BALANCE] {msg}")
    send_telegram(msg)

# --- Strategy Logic ---
async def handle_candle(candle):
    global position
    bar = {
        'open': float(candle['o']),
        'high': float(candle['h']),
        'low': float(candle['l']),
        'close': float(candle['c']),
        'volume': float(candle['v']),
        'trade_count': int(candle['n']),
        'timestamp': candle['T'] / 1000.0
    }
    bars.append(bar)
    if len(bars) < 61:
        return

    df = list(bars)
    last = df[-1]
    efficiency = abs(last['close'] - last['open']) / (last['high'] - last['low'] + 1e-8)  # Exact match
    # Calculate TradeSpike exactly like backtester (rolling 60s mean)
    trade_count_60s_mean = np.mean([b['trade_count'] for b in df[-60:]])
    trade_spike = last['trade_count'] / (trade_count_60s_mean + 1e-8)  # Exact match
    # Calculate Returns exactly like backtester (pct_change)
    closes = np.array([b['close'] for b in df])
    returns = np.diff(closes) / closes[:-1]  # Same as pct_change().fillna(0)
    
    # Calculate VolZ exactly like backtester
    if len(returns) >= 300:
        # Vol60s = Returns.rolling(60, min_periods=1).std()
        vol_60s = np.std(returns[-60:])
        
        # Calculate rolling 60s volatility for 300s window
        vol_60s_values = []
        for i in range(60, len(returns)+1):
            vol_60s_values.append(np.std(returns[i-60:i]))
        
        # VolZ = (Vol60s - Vol60s.rolling(300, min_periods=1).mean()) / (Vol60s.rolling(300, min_periods=1).std() + 1e-8)
        vol_300_mean = np.mean(vol_60s_values[-300:])
        vol_300_std = np.std(vol_60s_values[-300:])
        
        volz = (vol_60s - vol_300_mean) / (vol_300_std + 1e-8)
    else:
        volz = 0
    price = last['close']

    # Debug strategy values
    if position is None and len(bars) >= 300:
        log(f"[STRATEGY] Efficiency: {efficiency:.4f} (>{PARAMS['efficiency']}) | Spike: {trade_spike:.4f} (>{PARAMS['spike']}) | Volz: {volz:.4f} (>{PARAMS['volz']} or <{PARAMS['volz_fade']})")

    # Only fetch balance when we need it (when placing orders)
    # Removed frequent balance fetching that was causing API errors

    if position is None:
        if efficiency > PARAMS['efficiency'] and trade_spike > PARAMS['spike'] and volz > PARAMS['volz']:
            log(f"[SIGNAL] LONG TRIGGERED! Efficiency: {efficiency:.4f}, Spike: {trade_spike:.4f}, Volz: {volz:.4f}")
            now = time.time()
            # --- LIVE ORDER EXECUTION (LONG) ---
            asset_resp = await mexc_api.get_user_asset('USDT')
            usdt_bal = 0.0
            if asset_resp.success and hasattr(asset_resp.data, 'availableBalance'):
                usdt_bal = asset_resp.data.availableBalance
            price = last['close']
            # Fixed position size: 10 contracts
            vol = 10
            order = CreateOrderRequest(
                symbol="BTC_USDT",
                vol=vol,
                side=OrderSide.OpenLong,
                type=OrderType.MarketOrder,
                openType=OpenType.Cross,
                leverage=PARAMS['leverage']
            )
            print(f"[ORDER REQUEST] LONG: {order}")
            resp = await send_order(order)
            log_with_time(f"[LIVE TRADE] LONG ENTRY | Contracts: {vol} | Resp: {resp}")
            position = {'side': 'long', 'entry_price': price, 'created_at': now, 'qty': vol, 'peak': price}
            send_trade_update('long', price, 0.0, vol, 'Entry', is_entry=True)
            await send_balance_update()
        elif efficiency > PARAMS['efficiency'] and trade_spike > PARAMS['spike'] and volz < PARAMS['volz_fade']:
            log(f"[SIGNAL] SHORT TRIGGERED! Efficiency: {efficiency:.4f}, Spike: {trade_spike:.4f}, Volz: {volz:.4f}")
            now = time.time()
            # --- LIVE ORDER EXECUTION (SHORT) ---
            asset_resp = await mexc_api.get_user_asset('USDT')
            usdt_bal = 0.0
            if asset_resp.success and hasattr(asset_resp.data, 'availableBalance'):
                usdt_bal = asset_resp.data.availableBalance
            price = last['close']
            # Fixed position size: 10 contracts
            vol = 10
            order = CreateOrderRequest(
                symbol="BTC_USDT",
                vol=vol,
                side=OrderSide.OpenShort,
                type=OrderType.MarketOrder,
                openType=OpenType.Cross,
                leverage=PARAMS['leverage']
            )
            print(f"[ORDER REQUEST] SHORT: {order}")
            resp = await send_order(order)
            log_with_time(f"[LIVE TRADE] SHORT ENTRY | Contracts: {vol} | Resp: {resp}")
            position = {'side': 'short', 'entry_price': price, 'created_at': now, 'qty': vol, 'peak': price}
            send_trade_update('short', price, 0.0, vol, 'Entry', is_entry=True)
            await send_balance_update()
    else:
        dur = time.time() - position['created_at']
        ret = ((price - position['entry_price']) / position['entry_price']) if position['side'] == 'long' else ((position['entry_price'] - price) / position['entry_price'])
        # Calculate peak/trough returns exactly like backtester
        if position['side'] == 'long':
            position['peak'] = max(position['peak'], price)
            peak_ret = (position['peak'] - position['entry_price']) / position['entry_price']
        else:  # short
            position['peak'] = min(position['peak'], price)  # 'peak' is actually trough for shorts
            peak_ret = (position['entry_price'] - position['peak']) / position['entry_price']

        # Exit conditions exactly like backtester
        if ret >= PARAMS['tp'] or (peak_ret - ret >= PARAMS['trailing']) or dur >= PARAMS['timeout']:
            close_side = OrderSide.CloseLong if position['side'] == 'long' else OrderSide.CloseShort
            pos_resp = await mexc_api.get_open_positions(symbol="BTC_USDT")
            for pos in pos_resp.data:
                # Handle both object and dict formats
                position_type = pos.positionType if hasattr(pos, 'positionType') else pos.get('positionType')
                hold_vol = pos.holdVol if hasattr(pos, 'holdVol') else pos.get('holdVol')
                position_id = pos.positionId if hasattr(pos, 'positionId') else pos.get('positionId')
                
                if position_type in [1,2] and hold_vol > 0:
                    close_order = CreateOrderRequest(
                        symbol="BTC_USDT", vol=hold_vol, side=close_side,
                        type=OrderType.MarketOrder, openType=OpenType.Cross,
                        positionId=position_id
                    )
                    close_resp = await send_order(close_order)
                    log(f"[EXIT] {close_side.name} | Qty: {hold_vol} | Resp: {close_resp}")
                    send_telegram(f"*EXIT* {close_side.name}\nQty: {hold_vol}\nPrice: {price}\nPnL: {ret*100:.2f}%")
            log_trade({
                'side': position['side'] if isinstance(position['side'], str) else position['side'].name,
                'entry_price': position['entry_price'],
                'exit_price': price,
                'qty': position['qty'],
                'timestamp': datetime.datetime.utcnow(),
                'pnl': ret * position['qty']
            })
            position = None

# --- Simple WebSocket Loop ---
async def run_bot():
    while True:
        try:
            log("[BOT] Connecting to Binance WebSocket...")
            async with websockets.connect(BINANCE_WS_URL) as ws:
                log("[BOT] Connected! Receiving 1s candles...")
                async for msg in ws:
                    try:
                        data = json.loads(msg)
                        if 'k' in data:
                            candle = data['k']
                            log(f"[CANDLE] O:{candle['o']} H:{candle['h']} L:{candle['l']} C:{candle['c']} V:{candle['v']} TC:{candle['n']} TS:{candle['T']}")
                            try:
                                await handle_candle(candle)
                            except Exception as handle_error:
                                log(f"[BOT] Handle candle error: {handle_error}")
                                import traceback
                                log(f"[BOT] Handle traceback: {traceback.format_exc()}")
                    except Exception as e:
                        log(f"[BOT] Message error: {e}")
                        import traceback
                        log(f"[BOT] Message traceback: {traceback.format_exc()}")
                        continue
        except Exception as e:
            log(f"[BOT] Connection error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

def send_trade_update(side, entry_price, exit_price, qty, reason, pnl=None, is_entry=True):
    if is_entry:
        msg = (
            f"*New Entry*\n"
            f"Side: {side}\n"
            f"Entry: {entry_price}\n"
            f"Qty: {qty:.6f}\n"
            f"Reason: {reason}"
        )
    else:
        msg = (
            f"*Trade Exited*\n"
            f"Side: {side}\n"
            f"Entry: {entry_price}\n"
            f"Exit: {exit_price}\n"
            f"Qty: {qty:.6f}\n"
            f"Reason: {reason}"
        )
        if pnl is not None:
            msg += f"\nPnL: {pnl:.2f}"
    send_telegram(msg)

if __name__ == "__main__":
    log("LIVE BOT STARTED: MEXC EXECUTION + BINANCE 1s FEED + SIMPLE WS")
    asyncio.run(run_bot())
