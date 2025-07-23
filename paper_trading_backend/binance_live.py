import asyncio
import websockets
import json
import time

# --- State ---
bars = []  # Use the same bars deque as kraken_live if needed

async def binance_ws_loop():
    url = "wss://stream.binance.com:9443/ws/btcusdt@kline_1s"
    while True:
        try:
            print("Connecting to Binance WebSocket...")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                print("Connected to Binance WebSocket!")
                last_scan_print = time.time()
                bar_count = 0
                async for msg in ws:
                    now = time.time()
                    data = json.loads(msg)
                    if 'k' in data:
                        k = data['k']
                        if k['x']:  # Only process closed candles
                            bar_count += 1
                            bar = {
                                'open': float(k['o']),
                                'high': float(k['h']),
                                'low': float(k['l']),
                                'close': float(k['c']),
                                'volume': float(k['v']),
                                'trade_count': int(k['n']),
                                'timestamp': k['t'] // 1000  # ms to s
                            }
                            print(f"BINANCE CANDLE {bar_count}: O={bar['open']}, H={bar['high']}, L={bar['low']}, C={bar['close']}, V={bar['volume']}, TC={bar['trade_count']}, TS={bar['timestamp']}")
                            update_ohlcv_bar(bar)
                    if now - last_scan_print > 5:
                        print("[Binance Live Algo] Scanning...")
                        last_scan_print = now
        except Exception as e:
            print(f"Binance WebSocket error: {e}, reconnecting in 5s...")
            await asyncio.sleep(5)

def update_ohlcv_bar(bar):
    # This should match the function in kraken_live.py
    from .kraken_live import bars, run_strategy
    bars.append(bar)
    if len(bars) > 61:
        run_strategy()

def start_binance_live():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(binance_ws_loop()) 