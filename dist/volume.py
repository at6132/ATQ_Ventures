import asyncio
import websockets
import json
import time
from collections import defaultdict
from datetime import datetime
import csv
import os

# Global state
aggregated_data = defaultdict(lambda: {
    "trade_count": 0,
    "volume": 0,
    "notional": 0,
    "open": None,
    "high": None,
    "low": None,
    "close": None
})
latest_price = {"timestamp": 0, "price": None}
connection_status = {}
per_exchange_agg = defaultdict(lambda: {
    "trade_count": 0,
    "volume": 0,
    "notional": 0,
    "open": None,
    "high": None,
    "low": None,
    "close": None
})

# Define the canonical exchange names for use everywhere
exchanges = [
    "Binance Spot",
    "Binance Futures", 
    "Bybit",
    "OKX",
    "Binance US",
    "Bitmex"
]

# ---------------------------
# 🔌 WebSocket Connections
# ---------------------------

async def binance_kline():
    url = "wss://stream.binance.com:9443/ws/dogeusdt@kline_1s"
    name = "Binance Kline"
    try:
        async with websockets.connect(url) as ws:
            connection_status[name] = True
            try:
                async for msg in ws:
                    data = json.loads(msg)
                    k = data["k"]
                    if k["x"]:  # closed kline
                        ts = int(k["T"] / 1000)
                        price = float(k["c"])
                        latest_price["timestamp"] = ts
                        latest_price["price"] = price
            except Exception as e:
                connection_status[name] = False
                print(f"{name}: connection lost or error during message handling: {e}")
    except Exception as e:
        connection_status[name] = False
        print(f"{name}: could not connect: {e}")

async def handle_trade_stream(name, url, subscribe_message, parser):
    try:
        async with websockets.connect(url) as ws:
            connection_status[name] = True
            try:
                if subscribe_message:
                    await ws.send(json.dumps(subscribe_message))
                async for msg in ws:
                    print(f"[{name} RAW] {msg}")  # Print the raw message received from the exchange
                    try:
                        data = json.loads(msg)
                        trades = parser(data)
                        for trade in trades:
                            ts = int(trade["timestamp"] / 1000)
                            price = float(trade["price"])
                            qty = float(trade["qty"])
                            # Update per-exchange aggregation
                            key = (ts, name)
                            agg = per_exchange_agg[key]
                            agg["trade_count"] += 1
                            agg["volume"] += qty
                            agg["notional"] += qty * price
                            if agg["open"] is None:
                                agg["open"] = price
                            agg["high"] = price if agg["high"] is None else max(agg["high"], price)
                            agg["low"] = price if agg["low"] is None else min(agg["low"], price)
                            agg["close"] = price
                            # Update global aggregation
                            bar = aggregated_data[ts]
                            bar["trade_count"] += 1
                            bar["volume"] += qty
                            bar["notional"] += qty * price
                            if bar["open"] is None:
                                bar["open"] = price
                            bar["high"] = price if bar["high"] is None else max(bar["high"], price)
                            bar["low"] = price if bar["low"] is None else min(bar["low"], price)
                            bar["close"] = price
                    except Exception as e:
                        pass
            except Exception as e:
                connection_status[name] = False
                print(f"{name}: connection lost or error during message handling: {e}")
    except Exception as e:
        connection_status[name] = False
        print(f"{name}: could not connect: {e}")

# ---------------------------
# 🧠 Parsers per exchange
# ---------------------------

def binance_parser(data):
    return [{
        "timestamp": data["T"],
        "price": float(data["p"]),
        "qty": float(data["q"])
    }]

def bybit_parser(data):
    trades = []
    if "topic" in data and "data" in data:
        for d in data["data"]:
            trades.append({
                "timestamp": d["T"],
                "price": float(d["p"]),
                "qty": float(d["v"])
            })
    return trades

def okx_parser(data):
    trades = []
    if "arg" in data and "data" in data:
        for d in data["data"]:
            trades.append({
                "timestamp": int(datetime.strptime(d["ts"], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000) if 'T' not in d["ts"] else int(d["ts"]),
                "price": float(d["px"]),
                "qty": float(d["sz"])
            })
    return trades

def mexc_parser(data):
    print("[MEXC Parser] Called with data:", data)
    trades = []
    if "d" in data:
        for d in data["d"]:
            trades.append({
                "timestamp": d["T"],
                "price": float(d["p"]),
                "qty": float(d["v"])
            })
    return trades

# ---------------------------
# 🧠 Additional Exchange Parsers
# ---------------------------

def coinbase_parser(data):
    print("[Coinbase Parser] Called with data:", data)
    trades = []
    if data.get("type") == "match" and data.get("product_id") == "DOGE-USD":
        trades.append({
            "timestamp": int(datetime.strptime(data["time"], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000),
            "price": float(data["price"]),
            "qty": float(data["size"])
        })
    return trades

def kraken_parser(data):
    # Kraken sends both dicts (system messages) and lists (trade data)
    trades = []
    if isinstance(data, list) and len(data) > 2 and data[2] == 'trade':
        # Example: [channelID, [ [price, volume, time, side, orderType, misc], ... ], 'trade', 'DOGE/USD']
        for t in data[1]:
            trades.append({
                "timestamp": int(float(t[2])),
                "price": float(t[0]),
                "qty": float(t[1])
            })
    return trades

def kucoin_parser(data):
    print("[KuCoin Parser] Called with data:", data)
    trades = []
    if data.get("topic", "").endswith("/market/match:DOGE-USDT") and data.get("data"):
        d = data["data"]
        trades.append({
            "timestamp": int(datetime.strptime(d["time"][:-4], "%Y-%m-%dT%H:%M:%S.%f").timestamp() * 1000),
            "price": float(d["price"]),
            "qty": float(d["size"])
        })
    return trades

def bitfinex_parser(data):
    trades = []
    # Bitfinex sends arrays, need to filter for trade events
    if isinstance(data, list) and len(data) > 1:
        # Trade execution update: [chanId, 'tu', [ID, MTS, AMOUNT, PRICE]]
        if data[1] == 'tu' and isinstance(data[2], list):
            t = data[2]
            trades.append({
                "timestamp": int(t[1] / 1000),
                "price": float(t[3]),
                "qty": float(abs(t[2]))
            })
    return trades

def deribit_parser(data):
    print("[Deribit Parser] Called with data:", data)
    trades = []
    if data.get("method") == "subscription" and "params" in data:
        for t in data["params"]["data"]:
            trades.append({
                "timestamp": int(t["timestamp"] / 1000),
                "price": float(t["price"]),
                "qty": float(t["amount"])
            })
    return trades

def huobi_parser(data):
    trades = []
    if data.get("ch", "").endswith("trade.detail") and "tick" in data and "data" in data["tick"]:
        for t in data["tick"]["data"]:
            trades.append({
                "timestamp": int(t["ts"] / 1000),
                "price": float(t["price"]),
                "qty": float(t["amount"])
            })
    return trades

def gateio_parser(data):
    trades = []
    if data.get("method") == "trades.update" and isinstance(data.get("params"), list) and len(data["params"]) > 1:
        # params: [symbol, [trade1, trade2, ...]]
        for t in data["params"][1]:
            trades.append({
                "timestamp": int(float(t["time"])),
                "price": float(t["price"]),
                "qty": float(t["amount"])
            })
    return trades

def bitstamp_parser(data):
    trades = []
    if data.get("event") == "trade" and data.get("channel") == "live_trades_dogeusd":
        t = data["data"]
        trades.append({
            "timestamp": int(t["timestamp"]),
            "price": float(t["price"]),
            "qty": float(t["amount"])
        })
    return trades

def binanceus_parser(data):
    print("[BinanceUS Parser] Called with data:", data)
    return [{
        "timestamp": data["T"],
        "price": float(data["p"]),
        "qty": float(data["q"])
    }]

def phemex_parser(data):
    print("[Phemex Parser] Called with data:", data)
    trades = []
    if data.get("type") == "trade" and data.get("symbol") == "DOGEUSD":
        trades.append({
            "timestamp": int(data["timestamp"] / 1000),
            "price": float(data["price"]),
            "qty": float(data["size"])
        })
    return trades

def bitmex_parser(data):
    print("[Bitmex Parser] Called with data:", data)
    trades = []
    if data.get("table") == "trade" and "data" in data:
        for t in data["data"]:
            if t["symbol"] == "DOGEUSD":
                trades.append({
                    "timestamp": int(datetime.strptime(t["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()),
                    "price": float(t["price"]),
                    "qty": float(t["size"])
                })
    return trades

# ---------------------------
# 📤 Output Printer
# ---------------------------

async def output_loop():
    csv_file = "data.csv"
    header = ["timestamp", "open", "high", "low", "close", "volume", "trade_count", "notional"]
    header_written = os.path.exists(csv_file) and os.path.getsize(csv_file) > 0
    full_volume_file = "full_volume.csv"
    # List of all exchanges (should match those in main)
    exchanges = [
        "Binance Spot",
        "Binance Futures",
        "Bybit",
        "OKX",
        "Binance US",
        "Bitmex"
    ]
    # Header: timestamp, avg_price, <exchange1>_TC, ...
    full_volume_header = ["timestamp", "avg_price"] + [f"{exch}_TC" for exch in exchanges]
    full_volume_header_written = os.path.exists(full_volume_file) and os.path.getsize(full_volume_file) > 0
    while True:
        ts = int(time.time())
        await asyncio.sleep(1)
        data = aggregated_data.get(ts, {"trade_count": 0, "volume": 0, "notional": 0, "open": None, "high": None, "low": None, "close": None})
        price = latest_price["price"]
        print({
            "timestamp": ts,
            "price": price,
            "trade_count": data["trade_count"],
            "volume": round(data["volume"], 4),
            "notional": round(data["notional"], 2)
        })
        # Write to data.csv
        row = [
            ts,
            data["open"],
            data["high"],
            data["low"],
            data["close"],
            round(data["volume"], 4),
            data["trade_count"],
            round(data["notional"], 2)
        ]
        with open(csv_file, "a", newline="") as f:
            writer = csv.writer(f)
            if not header_written:
                writer.writerow(header)
                header_written = True
            writer.writerow(row)
        # Write one row per second to full_volume.csv: timestamp, avg_price, <exchange1>_TC, ...
        prev_ts = ts - 1
        # Compute global VWAP (avg_price)
        prev_data = aggregated_data.get(prev_ts, {"trade_count": 0, "volume": 0, "notional": 0})
        total_volume = prev_data.get("volume", 0)
        total_notional = prev_data.get("notional", 0)
        avg_price = round(total_notional / total_volume, 2) if total_volume > 0 else None
        # Get trade count for each exchange
        tc_per_exchange = []
        for exch in exchanges:
            ex_bar = per_exchange_agg.get((prev_ts, exch), None)
            tc_per_exchange.append(ex_bar["trade_count"] if ex_bar else 0)
            # Remove after writing
            if ex_bar:
                del per_exchange_agg[(prev_ts, exch)]
        full_row = [prev_ts, avg_price] + tc_per_exchange
        with open(full_volume_file, "a", newline="") as f:
            writer = csv.writer(f)
            if not full_volume_header_written:
                writer.writerow(full_volume_header)
                full_volume_header_written = True
            writer.writerow(full_row)

async def print_connection_status(names):
    await asyncio.sleep(5)
    for name in names:
        status = connection_status.get(name)
        if status is True:
            print(f"{name}: connected")
        elif status is False:
            print(f"{name}: not connected")
        else:
            print(f"{name}: status unknown")

# ---------------------------
# 🚀 Main Launcher
# ---------------------------

async def main():
    exchange_names = [
        "Binance Kline",
        "Binance Spot",
        "Binance Futures",
        "Bybit",
        "OKX",
        "Binance US",
        "Bitmex"
    ]
    tasks = [
        binance_kline(),
        handle_trade_stream("Binance Spot", "wss://stream.binance.com:9443/ws/dogeusdt@trade", None, binance_parser),
        handle_trade_stream("Binance Futures", "wss://fstream.binance.com/ws/dogeusdt@trade", None, binance_parser),
        handle_trade_stream("Bybit", "wss://stream.bybit.com/v5/public/linear", {"op": "subscribe", "args": ["publicTrade.DOGEUSDT"]}, bybit_parser),
        handle_trade_stream("OKX", "wss://ws.okx.com:8443/ws/v5/public", {"op": "subscribe", "args": [{"channel": "trades", "instId": "DOGE-USDT"}]}, okx_parser),
        handle_trade_stream("Binance US", "wss://stream.binance.us:9443/ws/dogeusdt@trade", None, binanceus_parser),
        handle_trade_stream("Bitmex", "wss://www.bitmex.com/realtime", {"op": "subscribe", "args": ["trade:DOGEUSD"]}, bitmex_parser),
        output_loop(),
        print_connection_status(exchange_names)
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
