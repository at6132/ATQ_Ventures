import asyncio
import websockets
import json
from collections import deque
from datetime import datetime, timezone
import csv
import os
import time
import math
import signal
import sys

# =========================
# Config
# =========================
SYMBOL_BINANCE = "dogeusdt"     # Binance stream symbol (lowercase)
SYMBOL_OKX     = "DOGE-USDT"    # OKX instId
SAMPLE_INTERVAL = 1.0           # seconds between feature rows (1 Hz is fine)
DEPTH_LEVELS    = 5             # use top-5 for depth pressure
QI_Z_WINDOW     = 120           # samples for ΔQI z-score baseline (~2 minutes @ 1 Hz)
OFI_WINDOW_S    = 2             # aggregate last ~2s of OFI
TRADES_MAXLEN   = 10000         # buffer a few seconds of trades

FEAT_CSV = "micro_features.csv"

# =========================
# State
# =========================
rolling_trades = deque(maxlen=TRADES_MAXLEN)  # {exchange, ts, price, qty, side('buy'/'sell')}
orderbook = {
    "bids": [],  # [[price, size], ...]
    "asks": [],
    "last": {"bid_p": None, "bid_q": None, "ask_p": None, "ask_q": None}
}

dqi_hist   = deque(maxlen=QI_Z_WINDOW)  # ΔQI series for z-score baseline
ofi_hist   = deque(maxlen=10)           # 1s OFI samples; we'll sum last k to approximate ~2s

stop_flag = False

# =========================
# Helpers
# =========================
def now_ts() -> float:
    return time.time()

def ts_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

def safe_div(a, b, eps=1e-12):
    return a / (b + eps)

def write_csv_row(path, header, row):
    exists = os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(header)
        w.writerow(row)
        print(f"Wrote CSV row: {dict(zip(header, row))}")

def zscore_from_series(x: float, series: deque) -> float:
    if len(series) < 20:
        return 0.0
    mu = sum(series) / len(series)
    var = sum((v - mu) ** 2 for v in series) / max(1, len(series) - 1)
    std = math.sqrt(var)
    z = (x - mu) / (std + 1e-12)
    print(f"Z-score calc: x={x:.6f}, mu={mu:.6f}, std={std:.6f}, z={z:.6f}")
    return z

def norm_trade(exchange, t_ms, price, qty, side_flag) -> dict:
    """
    side_flag -> True means aggressor BUY for Binance aggTrade (m == False)
    For others pass explicit 'buy'/'sell'.
    """
    side = "buy" if side_flag == True or side_flag == "buy" else "sell"
    trade = {
        "exchange": exchange,
        "ts": float(t_ms) / (1000.0 if t_ms > 1e10 else 1.0),  # ms or s
        "price": float(price),
        "qty": float(qty),
        "side": side
    }
    print(f"Normalized trade: {trade}")
    return trade

# =========================
# Streams
# =========================
async def binance_trade_stream():
    uri = f"wss://stream.binance.com:9443/ws/{SYMBOL_BINANCE}@aggTrade"
    while not stop_flag:
        try:
            async with websockets.connect(uri, ping_interval=20) as ws:
                async for msg in ws:
                    d = json.loads(msg)
                    print(f"Raw Binance trade: {d}")
                    trade = norm_trade(
                        "binance",
                        d["T"],
                        d["p"],
                        d["q"],
                        True if not d.get("m", False) else False
                    )
                    rolling_trades.append(trade)
        except Exception as e:
            print(f"[binance_trade_stream] {e} — reconnect in 3s")
            await asyncio.sleep(3)

async def bybit_trade_stream():
    uri = "wss://stream.bybit.com/v5/public/linear"
    sub = {"op": "subscribe", "args": ["publicTrade.DOGEUSDT"]}
    while not stop_flag:
        try:
            async with websockets.connect(uri, ping_interval=20) as ws:
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    d = json.loads(msg)
                    print(f"Raw Bybit message: {d}")
                    if "data" in d:
                        for t in d["data"]:
                            side = "buy" if t.get("S") == "Buy" else "sell"
                            trade = norm_trade("bybit", float(t["T"]), t["p"], t["v"], side)
                            rolling_trades.append(trade)
        except Exception as e:
            print(f"[bybit_trade_stream] {e} — reconnect in 3s")
            await asyncio.sleep(3)

async def okx_trade_stream():
    uri = "wss://ws.okx.com:8443/ws/v5/public"
    sub = {"op": "subscribe", "args": [{"channel": "trades", "instId": SYMBOL_OKX}]}
    while not stop_flag:
        try:
            async with websockets.connect(uri, ping_interval=20) as ws:
                await ws.send(json.dumps(sub))
                async for msg in ws:
                    d = json.loads(msg)
                    print(f"Raw OKX message: {d}")
                    if "data" in d:
                        for t in d["data"]:
                            side = t.get("side", "buy")
                            trade = norm_trade("okx", float(t["ts"]), t["px"], t["sz"], side)
                            rolling_trades.append(trade)
        except Exception as e:
            print(f"[okx_trade_stream] {e} — reconnect in 3s")
            await asyncio.sleep(3)

async def binance_orderbook_stream():
    uri = f"wss://stream.binance.com:9443/ws/{SYMBOL_BINANCE}@depth5@100ms"
    while not stop_flag:
        try:
            async with websockets.connect(uri, ping_interval=20) as ws:
                async for msg in ws:
                    d = json.loads(msg)
                    print(f"Raw Binance orderbook: {d}")
                    bids = [[float(p), float(q)] for p, q in d.get("bids", [])][:DEPTH_LEVELS]
                    asks = [[float(p), float(q)] for p, q in d.get("asks", [])][:DEPTH_LEVELS]
                    print(f"Processed orderbook - Bids: {bids}, Asks: {asks}")
                    orderbook["bids"] = bids
                    orderbook["asks"] = asks
        except Exception as e:
            print(f"[binance_orderbook_stream] {e} — reconnect in 3s")
            await asyncio.sleep(3)

# =========================
# Feature Computation
# =========================
def compute_features(now_s: float):
    if not orderbook["bids"] or not orderbook["asks"]:
        return None

    bid_p, bid_q = orderbook["bids"][0]
    ask_p, ask_q = orderbook["asks"][0]
    print(f"Top of book - Bid: {bid_p}@{bid_q}, Ask: {ask_p}@{ask_q}")

    mid    = 0.5 * (bid_p + ask_p)
    spread = ask_p - bid_p
    print(f"Mid: {mid:.8f}, Spread: {spread:.8f}")

    # Queue imbalance at top
    qi = safe_div(bid_q - ask_q, bid_q + ask_q)
    print(f"Queue Imbalance: {qi:.6f}")

    # ΔQI and z-score baseline
    prev_bid_q = orderbook["last"]["bid_q"]
    prev_ask_q = orderbook["last"]["ask_q"]
    if prev_bid_q is None or prev_ask_q is None:
        dqi = 0.0
        print("First orderbook snapshot - no ΔQI")
    else:
        prev_qi_inst = safe_div(prev_bid_q - prev_ask_q, prev_bid_q + prev_ask_q)
        dqi = qi - prev_qi_inst
        print(f"ΔQI calculation: current={qi:.6f} - prev={prev_qi_inst:.6f} = {dqi:.6f}")
    
    dqi_hist.append(dqi)
    dqi_z = zscore_from_series(dqi, dqi_hist)

    # Microprice & tilt (Stoikov)
    I = safe_div(bid_q, bid_q + ask_q)
    microprice = I * ask_p + (1 - I) * bid_p
    tilt = microprice - mid
    print(f"Microprice: {microprice:.8f}, Tilt: {tilt:.8f}")

    # Signed tape over last 1s
    buy_1s = 0.0
    sell_1s = 0.0
    cutoff = now_s - 1.0
    for t in reversed(rolling_trades):
        if t["ts"] < cutoff:
            break
        if t["side"] == "buy":
            buy_1s += t["qty"]
        else:
            sell_1s += t["qty"]
    print(f"1s Volume - Buy: {buy_1s:.3f}, Sell: {sell_1s:.3f}")

    # OFI using best-quote changes (1-s sample, then aggregate to ~2s)
    last = orderbook["last"]
    ofi_1s = 0.0
    if last["bid_p"] is not None and last["ask_p"] is not None:
        # Bid side contribution
        if bid_p > last["bid_p"]:
            ofi_1s += bid_q
        elif bid_p == last["bid_p"]:
            ofi_1s += (bid_q - (last["bid_q"] or 0.0))

        # Ask side contribution
        if ask_p < last["ask_p"]:
            ofi_1s -= ask_q
        elif ask_p == last["ask_p"]:
            ofi_1s -= (ask_q - (last["ask_q"] or 0.0))

    print(f"1s OFI: {ofi_1s:.3f}")
    ofi_hist.append(ofi_1s)
    
    # aggregate last ~OFI_WINDOW_S seconds (approximate by count at 1 Hz)
    k = min(len(ofi_hist), max(1, int(OFI_WINDOW_S / SAMPLE_INTERVAL)))
    ofi_agg = sum(list(ofi_hist)[-k:])
    print(f"Aggregated {k}s OFI: {ofi_agg:.3f}")

    # Depth pressure top-k
    depth_bid_k = sum(q for _, q in orderbook["bids"][:DEPTH_LEVELS])
    depth_ask_k = sum(q for _, q in orderbook["asks"][:DEPTH_LEVELS])
    print(f"Depth {DEPTH_LEVELS} levels - Bid: {depth_bid_k:.3f}, Ask: {depth_ask_k:.3f}")

    # update last snapshot for next delta computation
    orderbook["last"]["bid_p"] = bid_p
    orderbook["last"]["bid_q"] = bid_q
    orderbook["last"]["ask_p"] = ask_p
    orderbook["last"]["ask_q"] = ask_q

    features = {
        "timestamp": ts_iso(now_s),
        "bid": bid_p,
        "ask": ask_p,
        "size_bid": bid_q,
        "size_ask": ask_q,
        "spread": spread,
        "mid": mid,
        "microprice": microprice,
        "tilt": tilt,
        "QI": qi,
        "dQI": dqi,
        "QI_z": dqi_z,
        "OFI_2s": ofi_agg,
        "buy_vol_1s": buy_1s,
        "sell_vol_1s": sell_1s,
        "depth_bid_k": depth_bid_k,
        "depth_ask_k": depth_ask_k
    }
    print(f"Computed features: {features}")
    return features

# =========================
# Sampler
# =========================
async def sampler_loop():
    header = ["timestamp","bid","ask","size_bid","size_ask","spread","mid",
              "microprice","tilt","QI","dQI","QI_z","OFI_2s",
              "buy_vol_1s","sell_vol_1s","depth_bid_k","depth_ask_k"]
    next_t = now_ts()
    while not stop_flag:
        try:
            now = now_ts()
            if now >= next_t:
                print(f"\nSampling features at {ts_iso(now)}")
                feats = compute_features(now)
                if feats is not None:
                    write_csv_row(FEAT_CSV, header, [feats[h] for h in header])
                next_t += SAMPLE_INTERVAL
            else:
                await asyncio.sleep(min(0.01, next_t - now))
        except Exception as e:
            print(f"[sampler_loop] {e}")
            await asyncio.sleep(0.1)

# =========================
# Entrypoint
# =========================
def handle_sigterm(*_):
    global stop_flag
    stop_flag = True
    print("Shutting down...")

async def main():
    tasks = [
        asyncio.create_task(binance_orderbook_stream()),
        asyncio.create_task(binance_trade_stream()),
        asyncio.create_task(bybit_trade_stream()),
        asyncio.create_task(okx_trade_stream()),
        asyncio.create_task(sampler_loop())
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_sigterm)
    signal.signal(signal.SIGTERM, handle_sigterm)
    try:
        asyncio.run(main())
    except RuntimeError:
        # In case of nested loops in some environments
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
