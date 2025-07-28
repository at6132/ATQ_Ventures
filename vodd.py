import asyncio
import websockets
import json
from collections import deque
from datetime import datetime
import csv
import os
import numpy as np

# --- Global Buffers ---
rolling_trades = deque(maxlen=2000)
rolling_orderbook = {"bids": [], "asks": []}
CSV_FILE = "direction_score.csv"

def normalize_trade(exchange, timestamp, price, qty, side):
    return {
        "exchange": exchange,
        "timestamp": timestamp,
        "price": price,
        "qty": qty,
        "side": side
    }

# --- WebSocket Streams ---
async def binance_trade_stream():
    uri = "wss://stream.binance.com:9443/ws/dogeusdt@aggTrade"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                async for msg in ws:
                    data = json.loads(msg)
                    trade = normalize_trade("binance", data['T'] / 1000, float(data['p']), float(data['q']), "buy" if not data['m'] else "sell")
                    rolling_trades.append(trade)
        except Exception as e:
            print(f"[binance_trade_stream] Error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

async def bybit_trade_stream():
    uri = "wss://stream.bybit.com/v5/public/linear"
    sub = {"op": "subscribe", "args": ["publicTrade.DOGEUSDT"]}
    while True:
        try:
            async with websockets.connect(uri) as ws:
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    data = json.loads(msg)
                    if 'data' in data:
                        for t in data['data']:
                            trade = normalize_trade("bybit", float(t['T']) / 1000, float(t['p']), float(t['v']), "buy" if t['S'] == "Buy" else "sell")
                            rolling_trades.append(trade)
        except Exception as e:
            print(f"[bybit_trade_stream] Error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

async def okx_trade_stream():
    uri = "wss://ws.okx.com:8443/ws/v5/public"
    sub = {"op": "subscribe", "args": [{"channel": "trades", "instId": "DOGE-USDT"}]}
    while True:
        try:
            async with websockets.connect(uri) as ws:
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    data = json.loads(msg)
                    if 'data' in data:
                        for t in data['data']:
                            trade = normalize_trade("okx", float(t['ts']) / 1000, float(t['px']), float(t['sz']), t['side'])
                            rolling_trades.append(trade)
        except Exception as e:
            print(f"[okx_trade_stream] Error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

async def binance_orderbook_stream():
    uri = "wss://stream.binance.com:9443/ws/dogeusdt@depth5@100ms"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                async for msg in ws:
                    data = json.loads(msg)
                    rolling_orderbook["bids"] = [[float(x[0]), float(x[1])] for x in data["bids"]]
                    rolling_orderbook["asks"] = [[float(x[0]), float(x[1])] for x in data["asks"]]
        except Exception as e:
            print(f"[binance_orderbook_stream] Error: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

# --- Institutional Direction Scoring ---
def analyze_institutional_direction(trades, book):
    if not trades:
        return 0.0

    buy_vol = sum(t['qty'] for t in trades if t['side'] == 'buy')
    sell_vol = sum(t['qty'] for t in trades if t['side'] == 'sell')
    net_delta = (buy_vol - sell_vol) / (buy_vol + sell_vol + 1e-8)

    max_buy = max([t['qty'] for t in trades if t['side'] == 'buy'], default=0)
    max_sell = max([t['qty'] for t in trades if t['side'] == 'sell'], default=0)
    dominance = (max_buy - max_sell) / (max_buy + max_sell + 1e-8)

    price_buckets = {}
    for t in trades:
        key = (round(t['price'], 3), t['side'])
        price_buckets[key] = price_buckets.get(key, 0) + t['qty']
    cluster_strength = max(price_buckets.values(), default=0) / (buy_vol + sell_vol + 1e-8)

    bid_vol = sum(x[1] for x in book['bids'])
    ask_vol = sum(x[1] for x in book['asks'])
    book_imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol + 1e-8)

    iceberg_score = 0
    for side in ['buy', 'sell']:
        price_hits = {}
        for t in trades:
            if t['side'] == side:
                p = round(t['price'], 3)
                price_hits[p] = price_hits.get(p, 0) + 1
        hits = max(price_hits.values(), default=0)
        iceberg_score += (hits / len(trades)) * (1 if side == 'buy' else -1)

    wall_removed = 0
    if 'last_bids' in rolling_orderbook:
        old_bid = sum(x[1] for x in rolling_orderbook['last_bids'])
        if old_bid > 0 and bid_vol < old_bid * 0.7:
            wall_removed = -1
        old_ask = sum(x[1] for x in rolling_orderbook['last_asks'])
        if old_ask > 0 and ask_vol < old_ask * 0.7:
            wall_removed = 1
    rolling_orderbook['last_bids'] = book['bids']
    rolling_orderbook['last_asks'] = book['asks']

    # DOGE-optimized weights
    score = (
        0.3 * net_delta +
        0.2 * dominance +
        0.15 * cluster_strength +
        0.15 * book_imbalance +
        0.1 * iceberg_score +
        0.1 * wall_removed
    )
    return round(max(-1.0, min(1.0, score)), 4)

def save_score(ts, score):
    file_exists = os.path.exists(CSV_FILE)
    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "direction_score"])
        writer.writerow([ts, score])

async def scoring_loop():
    while True:
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        trades = list(rolling_trades)[-300:]
        book = rolling_orderbook.copy()
        score = analyze_institutional_direction(trades, book)
        print(f"[{ts}] Score: {score}")
        save_score(ts, score)
        await asyncio.sleep(1)

# --- Main ---
async def main():
    await asyncio.gather(
        binance_trade_stream(),
        bybit_trade_stream(),
        okx_trade_stream(),
        binance_orderbook_stream(),
        scoring_loop()
    )

asyncio.run(main())
