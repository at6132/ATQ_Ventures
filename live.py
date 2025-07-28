import asyncio, time, os, math, datetime, collections
from collections import deque
import pandas as pd
from typing import Optional

# ===== MEXC SDK (from your uploaded files) =====
import sys
sys.path.append('mexc_python')
from mexcpy.api import MexcFuturesAPI as API
from mexcpy.mexcTypes import CreateOrderRequest, OrderSide, OrderType, OpenType

# =========================
# CONFIG
# =========================
SYMBOL = "DOGE_USDT"

# MEXC AUTH — place your MEXC u-id/token here or set env MEXC_TOKEN
USER_TOKEN = os.getenv("MEXC_TOKEN", "WEB60fa5faf1de49582034b5826b049133c01bac291fbbfd8be6b977f3957575ea6")

# TELEGRAM (set both to enable alerts)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8156711122:AAFYoW3ESDlxAjSfHO_DkjabgKZ3aUc3oRI")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "7168811895")

LEVERAGE = 25
FEE_RT = 0.0005                   # 0.05% round‑trip (for logs/breakeven math only)

DATA_CSV = "data.csv"             # ts (s), open, high, low, close, (volume, trade_count optional)
VODD_CSV = "direction_score.csv"  # timestamp (ISO), direction_score
FEAT_CSV = "micro_features.csv"   # timestamp (ISO), ... QI_z, tilt, spread, mid, ...

# Strategy
VODD_THR = 0.70                   # abs(score) crossing threshold
SPREAD_MAX = 0.0002               # 0.02% spread guard (spread/mid)
PERSIST_ROWS = 2                  # require N consecutive feature rows agree

# Brackets & timing
TP_PCT = 0.0025                   # +0.25%
SL_PCT = 0.00125                  # -0.125%
BE_PCT = 0.0012                   # +0.12% -> move SL to entry (breakeven)
TRADE_TIMEOUT_S = 180             # hard timeout

# Post‑only chaser tuning
TICK = 1e-5                       # DOGE tick; adjust if your market differs
ENTRY_MAX_REPRICES = 10
REPRICE_BACKOFF_S  = [0.2,0.3,0.4,0.6,0.8,1.0,1.2,1.5,1.5,2.0]  # reused cyclically

# Entry abort rule (stop chasing entry if price moves too far from the signal)
ENTRY_ABORT_MOVE_PCT = 0.001      # 0.10% from signal → abort entry

# Misc
ORDER_VOL = 25                    # position size (contracts/coins depending on market)
PX_DEC   = 5                      # price decimals for rounding
MIN_SECONDS_BETWEEN_ENTRIES = 5
CSV_TAIL_N = 300

# =========================
# STATE
# =========================
mexc = API(token=USER_TOKEN)

bars = deque(maxlen=400)          # recent OHLC for reference
best_bid: Optional[float] = None
best_ask: Optional[float] = None

class Status:
    FLAT="FLAT"; ARMED="ARMED"; ENTERING="ENTERING"; OPEN="OPEN"; EXITING="EXITING"

state = {
    "status": Status.FLAT,
    "side": None,                  # 'long'/'short'
    "entry_price": None,
    "created_at": None,
    "sl_mode": "percent",          # 'percent' or 'breakeven'
    "tp_order_id": None,
    "last_entry_at": 0.0,
    "armed_at": None,
    "entry_order_id": None,
}

trade_lock = asyncio.Lock()

# =========================
# LOG/TELEGRAM
# =========================
def now_utc(): return datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
def log(msg): print(f"[{now_utc()}] {msg}")

def print_csv_status():
    """Print CSV loading status every second"""
    try:
        data_row = data_tail.last()
        vodd_row = vodd_tail.last()
        feat_row = feat_tail.last()
        
        data_ts = f"{data_row['_ts']:.0f}" if data_row is not None else "None"
        vodd_ts = f"{vodd_row['_ts']:.0f}" if vodd_row is not None else "None"
        feat_ts = f"{feat_row['_ts']:.0f}" if feat_row is not None else "None"
        
        vodd_score = f"{vodd_row['direction_score']:.3f}" if vodd_row is not None else "None"
        
        print(f"[{now_utc()}] 📊 CSV STATUS | Data: {data_ts} | VODD: {vodd_ts} (score: {vodd_score}) | Features: {feat_ts}")
    except Exception as e:
        print(f"[{now_utc()}] 📊 CSV STATUS ERROR: {e}")

def print_trade_update():
    """Print detailed trade status every second when in trade"""
    if state["status"] not in (Status.OPEN, Status.EXITING):
        return
        
    try:
        side = state["side"]
        entry_price = state["entry_price"]
        created_at = state["created_at"]
        
        if not bars:
            return
            
        current_price = bars[-1]["close"]
        current_time = time.time()
        trade_duration = current_time - created_at
        time_left = max(0, TRADE_TIMEOUT_S - trade_duration)
        
        # Calculate P&L
        if side == "long":
            pnl_pct = (current_price - entry_price) / entry_price
            tp_price = entry_price * (1 + TP_PCT)
            sl_price = entry_price * (1 - SL_PCT) if state["sl_mode"] == "percent" else entry_price
            tp_distance = (tp_price - current_price) / entry_price * 100
            sl_distance = (current_price - sl_price) / entry_price * 100
        else:  # short
            pnl_pct = (entry_price - current_price) / entry_price
            tp_price = entry_price * (1 - TP_PCT)
            sl_price = entry_price * (1 + SL_PCT) if state["sl_mode"] == "percent" else entry_price
            tp_distance = (current_price - tp_price) / entry_price * 100
            sl_distance = (sl_price - current_price) / entry_price * 100
        
        # Apply fees
        pnl_pct -= FEE_RT
        
        # Calculate USDT P&L
        position_size = ORDER_VOL / 100  # Convert to account percentage
        notional_value = position_size * LEVERAGE
        pnl_usdt = pnl_pct * notional_value
        
        print(f"[{now_utc()}] 💰 TRADE_UPDATE | {side.upper()} | Entry: {entry_price:.5f} | Current: {current_price:.5f} | P&L: {pnl_pct*100:.3f}% ({pnl_usdt:.4f} USDT)")
        print(f"[{now_utc()}] 🎯 TARGETS | TP: {tp_price:.5f} ({tp_distance:.3f}% away) | SL: {sl_price:.5f} ({sl_distance:.3f}% away) | Time Left: {time_left:.0f}s")
        
    except Exception as e:
        print(f"[{now_utc()}] 💰 TRADE_UPDATE ERROR: {e}")

def send_telegram(text: str):
    try:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or "REPLACE_WITH_TELEGRAM_BOT_TOKEN" in str(TELEGRAM_BOT_TOKEN):  # not configured
            return
        import requests
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
        requests.post(url, data=payload, timeout=5)
    except Exception as e:
        print(f"[{now_utc()}] [TELEGRAM_ERR] {e}")

def send_trade_update(side, entry_price, exit_price, qty, reason, pnl=None, is_entry=True):
    if is_entry:
        msg = (
            f"*🚀 New Entry*\n"
            f"Side: {side.upper()}\n"
            f"Entry: `{entry_price:.5f}`\n"
            f"Qty: `{qty}`\n"
            f"Reason: {reason}\n"
            f"Time: {now_utc()}"
        )
    else:
        pnl_str = f"`{pnl:.5f}`" if pnl is not None else "n/a"
        msg = (
            f"*✅ Exit*\n"
            f"Side: {side.upper()}\n"
            f"Exit: `{exit_price:.5f}`\n"
            f"PNL: {pnl_str}\n"
            f"Reason: {reason}\n"
            f"Time: {now_utc()}"
        )
    send_telegram(msg)

def send_error_alert(kind, details, level="WARNING"):
    emoji = {"WARNING":"⚠️","ERROR":"🛑","CRITICAL":"🔥"}.get(level,"⚠️")
    msg = (
        f"*{emoji} {kind}*\n"
        f"{details}\n"
        f"Time: {now_utc()}"
    )
    send_telegram(msg)

def send_missed_entry_alert(side, reason, signal_price, current_price):
    price_change_pct = abs(current_price - signal_price) / max(signal_price, 1e-12) * 100
    msg = (
        f"*❌ Missed Entry*\n"
        f"Side: {side.upper()}\n"
        f"Reason: {reason}\n"
        f"Signal: `{signal_price:.5f}`\n"
        f"Now: `{current_price:.5f}`  (*{price_change_pct:.2f}% drift*)\n"
        f"Time: {now_utc()}"
    )
    send_telegram(msg)

def send_general_update(update_type, details):
    emoji_map = {
        "BOT_START": "🤖", "BOT_STOP": "🛑", "RECONNECT": "🔌",
        "VODD_ARMED": "🎯", "ENTRY_ABORT": "❌", "ENTRY_FILLED": "✅",
        "TP_PLACED": "🎯", "TP_CANCELLED": "❌",
        "TP_HIT": "🎯", "SL_HIT": "🛡️", "TIMEOUT": "⏳",
        "EXITED": "✅", "EXIT_RETRY": "🔁",
        "ORDER_PLACED":"📝", "ORDER_FILLED":"✅", "ORDER_CANCELLED":"❌"
    }
    emoji = emoji_map.get(update_type, "ℹ️")
    msg = (
        f"*{emoji} {update_type.replace('_',' ').title()}*\n"
        f"{details}\n"
        f"Time: {now_utc()}"
    )
    send_telegram(msg)

# =========================
# UTILS
# =========================
def round_px(x: float) -> float: return round(x, PX_DEC)
def pct(cur: float, ref: float) -> float:
    if ref == 0: return 0.0
    return (cur - ref) / ref

# =========================
# TICKER (best bid/ask from MEXC) — async & non‑blocking
# =========================
async def update_best_bid_ask():
    import aiohttp
    url = f"https://contract.mexc.com/api/v1/contract/ticker?symbol={SYMBOL}"
    global best_bid, best_ask
    print(f"[{now_utc()}] 🔄 TICKER | Starting MEXC ticker updates for {SYMBOL}")
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url, timeout=2.5) as resp:
                    j = await resp.json()
                    data = j.get("data") or {}
                    bb = data.get("bid1"); ba = data.get("ask1")
                    if bb is not None and ba is not None:
                        old_bid, old_ask = best_bid, best_ask
                        best_bid = float(bb); best_ask = float(ba)
                        
                        # Print ticker updates occasionally
                        if old_bid != best_bid or old_ask != best_ask:
                            spread = (best_ask - best_bid) / best_bid * 100
                            print(f"[{now_utc()}] 💱 TICKER | Bid: {best_bid:.5f} | Ask: {best_ask:.5f} | Spread: {spread:.4f}%")
            except Exception as e:
                log(f"[TICKER] {e}")
            await asyncio.sleep(0.25)

# =========================
# CSV TAIL READERS
# =========================
class CsvTail:
    def __init__(self, path, ts_col, iso=False):
        self.path = path; self.ts_col = ts_col; self.iso = iso
    def read_latest(self, n=CSV_TAIL_N):
        try:
            df = pd.read_csv(self.path)
            if self.iso: df["_ts"] = pd.to_datetime(df[self.ts_col], utc=True).astype("int64")//10**9
            else:        df["_ts"] = df[self.ts_col].astype(float)
            return df.tail(n).copy()
        except Exception as e:
            log(f"[CSV ERR] {self.path}: {e}")
            return None
    def last(self):
        df = self.read_latest(1)
        if df is None or len(df)==0: return None
        return df.iloc[-1]

DATA_CSV = DATA_CSV
VODD_CSV = VODD_CSV
FEAT_CSV = FEAT_CSV
data_tail = CsvTail(DATA_CSV, "timestamp", iso=False)
vodd_tail = CsvTail(VODD_CSV, "timestamp", iso=True)
feat_tail = CsvTail(FEAT_CSV, "timestamp", iso=True)

def prefill_bars():
    print(f"[{now_utc()}] 📊 LOADING DATA | Starting data preload...")
    
    df = data_tail.read_latest(120)
    if df is None:
        print(f"[{now_utc()}] ❌ DATA ERROR | Failed to load data.csv")
        return
    
    loaded_count = 0
    for _,r in df.iterrows():
        bars.append({
            "timestamp": float(r["_ts"]),
            "open": float(r["open"]), "high": float(r["high"]),
            "low": float(r["low"]),   "close": float(r["close"]),
            "volume": float(r.get("volume",0)), "trade_count": int(r.get("trade_count",0))
        })
        loaded_count += 1
    
    print(f"[{now_utc()}] ✅ DATA LOADED | Loaded {loaded_count} candles from data.csv")
    if loaded_count > 0:
        first_ts = bars[0]["timestamp"]
        last_ts = bars[-1]["timestamp"]
        time_span = last_ts - first_ts
        print(f"[{now_utc()}] 📈 DATA RANGE | From {first_ts:.0f} to {last_ts:.0f} ({time_span:.0f}s span)")
        print(f"[{now_utc()}] 💰 PRICE RANGE | Low: {min(b['low'] for b in bars):.5f} | High: {max(b['high'] for b in bars):.5f}")

# =========================
# VODD gate (fresh |score| crossing)
# =========================
class VoddGate:
    def __init__(self, thr=VODD_THR): 
        self.thr=thr; 
        self.prev=None
        print(f"[{now_utc()}] 🎯 VODD_GATE | Initialized with threshold: {thr}")
    
    def check(self):
        row = vodd_tail.last()
        if row is None: return None
        score = float(row["direction_score"])
        crossed = (abs(self.prev) < self.thr and abs(score) >= self.thr) if (self.prev is not None) else (abs(score) >= self.thr)
        
        # Print VODD score occasionally
        if abs(score) > 0.5:  # Only print significant scores
            print(f"[{now_utc()}] 🧠 VODD_SCORE | Score: {score:.3f} | Threshold: {self.thr} | Crossed: {crossed}")
        
        self.prev = score
        return {"ts": float(row["_ts"]), "score": score, "cross": crossed}

# =========================
# Direction picker (QI_z + tilt) with spread & persistence guards
# =========================
class DirectionPicker:
    def __init__(self, persist=PERSIST_ROWS): 
        self.persist=persist
        print(f"[{now_utc()}] 🧭 DIRECTION_PICKER | Initialized with persistence: {persist} rows")
    
    def pick(self):
        df = feat_tail.read_latest(self.persist)
        if df is None or len(df) < self.persist: 
            print(f"[{now_utc()}] ⚠️ DIRECTION_PICKER | Insufficient feature data: {len(df) if df is not None else 0}/{self.persist}")
            return None
        # Spread guard
        spread_pct = (df["spread"].astype(float) / (df["mid"].astype(float).abs()+1e-12))
        if (spread_pct > SPREAD_MAX).any():
            print(f"[{now_utc()}] ⚠️ SPREAD_GUARD | Spread too high: {spread_pct.max():.4f}% > {SPREAD_MAX*100:.2f}%")
            return None
        # Row-wise decision & persistence
        votes = []
        for _,r in df.iterrows():
            qi_z = float(r["QI_z"]); tilt = float(r["tilt"])
            long_ok  = (qi_z >= +1.0) and (tilt > 0.0)
            short_ok = (qi_z <= -1.0) and (tilt <  0.0)
            votes.append("long" if (long_ok and not short_ok) else ("short" if (short_ok and not long_ok) else None))
        if all(v=="long" for v in votes):  
            print(f"[{now_utc()}] 🧭 DIRECTION | LONG signal detected (QI_z & tilt)")
            return "long"
        if all(v=="short" for v in votes): 
            print(f"[{now_utc()}] 🧭 DIRECTION | SHORT signal detected (QI_z & tilt)")
            return "short"
        return None

# =========================
# MEXC helpers
# =========================
async def get_open_position_exists() -> bool:
    """Check if we have an open position with retry logic"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = await mexc.get_open_positions(symbol=SYMBOL)
            data = getattr(resp, "data", None) or []
            
            print(f"[{now_utc()}] 🔍 POSITION_CHECK | Found {len(data)} position(s)")
            
            for p in data:
                hold_vol = float(getattr(p, "holdVol", 0))
                position_type = getattr(p, "positionType", "Unknown")
                symbol = getattr(p, "symbol", "Unknown")
                print(f"[{now_utc()}] 📊 POSITION_DETAIL | Symbol: {symbol}, Type: {position_type}, Volume: {hold_vol}")
                if hold_vol > 0 and symbol == SYMBOL:
                    print(f"[{now_utc()}] ✅ POSITION_FOUND | Open position: {hold_vol} contracts for {symbol}")
                    return True
            
            print(f"[{now_utc()}] ❌ POSITION_NOT_FOUND | No open positions with volume > 0")
            return False
            
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"[{now_utc()}] ⚠️ POS_CHECK_RETRY | Attempt {attempt + 1}/{max_retries}: {e}")
                await asyncio.sleep(0.1)
            else:
                print(f"[{now_utc()}] ❌ POS_CHECK_ERR | Final attempt failed: {e}")
                return False
    return False

async def get_order_state(order_id: str):
    """Get order state with retry logic and detailed logging"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            resp = await mexc.get_order_by_order_id(order_id)
            o = getattr(resp, "data", None)
            if not o: 
                print(f"[{now_utc()}] ⚠️ ORDER_STATE | Order {order_id} not found")
                return None
            
            state = int(getattr(o, "state", 0))
            deal_avg = float(getattr(o, "dealAvgPrice", 0) or 0)
            price = float(getattr(o, "price", 0) or 0)
            
            # Log order state changes
            if state in (2, 3):  # Filled states
                print(f"[{now_utc()}] ✅ ORDER_FILLED | Order {order_id} filled @ {deal_avg:.5f}")
            elif state == 4:  # Cancelled
                print(f"[{now_utc()}] ❌ ORDER_CANCELLED | Order {order_id} cancelled")
            
            return {
                "state": state,            # 2/3=filled; 4=cancelled
                "deal_avg": deal_avg,
                "price": price
            }
            
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"[{now_utc()}] ⚠️ ORDER_STATE_RETRY | Attempt {attempt + 1}/{max_retries}: {e}")
                await asyncio.sleep(0.1)
            else:
                print(f"[{now_utc()}] ❌ ORDER_STATE_ERR | Final attempt failed: {e}")
                return None
    return None

async def cancel_order(order_id: Optional[str]) -> bool:
    if not order_id: return True
    try:
        resp = await mexc.cancel_orders([str(order_id)])
        send_general_update("ORDER_CANCELLED", f"Cancelled order `{order_id}`")
        return bool(resp)
    except Exception as e:
        send_error_alert("CANCEL_ERR", f"Cancel order `{order_id}` error: {e}", "ERROR")
        return False

async def create_post_only(open_close: OrderSide, price: float) -> Optional[str]:
    req = CreateOrderRequest(
        symbol=SYMBOL, vol=ORDER_VOL,
        side=open_close,
        type=OrderType.PostOnlyMaker,
        openType=OpenType.Isolated,
        leverage=LEVERAGE,
        price=round_px(price)
    )
    try:
        print(f"[{now_utc()}] 📝 CREATE_ORDER | {open_close.name} @ {round_px(price):.5f}")
        resp = await mexc.create_order(req)
        
        # Check if order creation was successful
        if not resp or not resp.success:
            print(f"[{now_utc()}] ❌ ORDER_FAILED | API response failed: {resp}")
            return None
            
        # Extract order ID from response
        oid = None
        if resp.data and hasattr(resp.data, 'orderId'):
            oid = str(resp.data.orderId)
            print(f"[{now_utc()}] ✅ ORDER_CREATED | {open_close.name} @ {round_px(price):.5f} (oid: {oid})")
            send_general_update("ORDER_PLACED", f"Post‑Only `{open_close.name}` @ `{round_px(price):.5f}` (oid `{oid}`)")
        else:
            print(f"[{now_utc()}] ❌ ORDER_FAILED | No order ID in response: {resp.data}")
            
        return oid
        
    except Exception as e:
        print(f"[{now_utc()}] ❌ CREATE_ERROR | {open_close.name} @ {round_px(price):.5f}: {e}")
        send_error_alert("CREATE_ERR", f"Create Post‑Only `{open_close.name}` @ `{price}` failed: {e}", "ERROR")
        return None

# =========================
# Post‑only chasers (entries/exits)
# =========================
async def chase_post_only_entry(side: str, signal_px: float):
    """
    Maker‑only entry. Reprices up to ENTRY_MAX_REPRICES within a short window.
    Aborts if price drift from signal exceeds ENTRY_ABORT_MOVE_PCT.
    """
    attempts = 0
    last_oid: Optional[str] = None

    while attempts < ENTRY_MAX_REPRICES:
        if best_bid is None or best_ask is None:
            await asyncio.sleep(0.05); continue

        # maker-safe price at the book edge
        if side == "long":
            px = min(best_bid, signal_px*(1+0.001)) - TICK  # <= bid
            side_enum = OrderSide.OpenLong
        else:
            px = max(best_ask, signal_px*(1-0.001)) + TICK  # >= ask
            side_enum = OrderSide.OpenShort

        px = round_px(px)

        # abort if drifted too far from signal
        if abs(pct(px, signal_px)) > ENTRY_ABORT_MOVE_PCT:
            send_missed_entry_alert(side, "Entry drifted too far from signal", signal_px, px)
            send_general_update("ENTRY_ABORT", f"{side.upper()} drift {pct(px,signal_px)*100:.2f}% > {ENTRY_ABORT_MOVE_PCT*100:.2f}%")
            break

        # cancel previous
        if last_oid:
            await cancel_order(last_oid); last_oid = None

        oid = await create_post_only(side_enum, px)
        if not oid:
            attempts += 1; await asyncio.sleep(0.1); continue

        # short wait for fill before reprice
        wait_s = REPRICE_BACKOFF_S[min(attempts, len(REPRICE_BACKOFF_S)-1)]
        tfill = time.time()
        filled = False
        fill_attempts = 0
        max_fill_checks = int(wait_s * 10)  # Check every 0.1s
        
        while time.time()-tfill <= wait_s and fill_attempts < max_fill_checks:
            os = await get_order_state(oid)
            if os:
                state = os["state"]
                print(f"[{now_utc()}] 🔍 ORDER_STATE | Order {oid} state: {state} (attempt {fill_attempts + 1})")
                
                if state in (2, 3):  # Uncompleted or Completed
                    # Check if it actually filled by looking at deal volume
                    if os["deal_vol"] > 0:
                        filled = True
                        print(f"[{now_utc()}] ✅ ENTRY_FILLED | Order {oid} filled in {time.time()-tfill:.2f}s")
                        break
                    else:
                        print(f"[{now_utc()}] ⏳ ORDER_PENDING | Order {oid} state {state} but not filled yet")
                elif state == 4:  # Cancelled
                    print(f"[{now_utc()}] ❌ ENTRY_CANCELLED | Order {oid} was cancelled")
                    break
                elif state == 5:  # Invalid
                    print(f"[{now_utc()}] ❌ ENTRY_INVALID | Order {oid} is invalid")
                    break
            else:
                print(f"[{now_utc()}] ⚠️ ORDER_STATE_NULL | Could not get state for order {oid}")
                
            fill_attempts += 1
            await asyncio.sleep(0.1)

        if filled:
            os = await get_order_state(oid)
            print(f"[{now_utc()}] 🔍 ORDER_STATE_DEBUG | State: {os['state'] if os else 'None'}, Deal_avg: {os['deal_avg'] if os else 'None'}, Price: {os['price'] if os else 'None'}")
            
            if os and os["deal_avg"] > 0:
                entry_px = os["deal_avg"]
                print(f"[{now_utc()}] ✅ ENTRY_PRICE | Using deal_avg: {entry_px:.5f}")
            else:
                entry_px = px
                print(f"[{now_utc()}] ⚠️ ENTRY_PRICE | Using order price (deal_avg=0): {entry_px:.5f}")
                if os:
                    print(f"[{now_utc()}] ⚠️ ORDER_WARNING | Order filled but deal_avg=0, this might indicate an issue")
            
            send_general_update("ORDER_FILLED", f"ENTRY `{side.upper()}` filled @ `{entry_px:.5f}` (oid `{oid}`)")
            return True, entry_px, oid

        last_oid = oid
        attempts += 1

    if last_oid:
        await cancel_order(last_oid)
    return False, None, None

async def chase_post_only_exit(side: str, entry_px: float, reason: str):
    """
    Maker‑only exit that NEVER gives up: cancel TP once, then loop:
      - place post‑only exit pegged to current best book edge
      - poll briefly; if not filled, cancel & reprice immediately
    Runs until position is flat.
    """
    # cancel TP if exists (once)
    if state.get("tp_order_id"):
        await cancel_order(state["tp_order_id"])
        send_general_update("TP_CANCELLED", f"Cancelled TP oid `{state['tp_order_id']}` before exit")
        state["tp_order_id"] = None

    backoff_idx = 0
    last_oid: Optional[str] = None

    while True:
        # stop if already flat
        if not await get_open_position_exists():
            send_general_update("EXITED", f"{side.upper()} closed — {reason}")
            return True

        if best_bid is None or best_ask is None:
            await asyncio.sleep(0.05); continue

        # maker-safe exit price (never crosses)
        if side == "long":
            px = round_px(best_ask + TICK)    # SELL maker ≥ ask
            side_enum = OrderSide.CloseLong
        else:
            px = round_px(best_bid - TICK)    # BUY maker ≤ bid
            side_enum = OrderSide.CloseShort

        # cancel previous
        if last_oid:
            await cancel_order(last_oid); last_oid = None

        oid = await create_post_only(side_enum, px)
        if not oid:
            await asyncio.sleep(0.1); continue

        # quick poll; if not filled, retry with reprice
        wait_s = REPRICE_BACKOFF_S[min(backoff_idx, len(REPRICE_BACKOFF_S)-1)]
        t0 = time.time()
        filled = False
        fill_attempts = 0
        max_fill_checks = int(wait_s * 10)  # Check every 0.1s
        
        while time.time()-t0 <= wait_s and fill_attempts < max_fill_checks:
            os = await get_order_state(oid)
            if os:
                state = os["state"]
                print(f"[{now_utc()}] 🔍 EXIT_STATE | Order {oid} state: {state} (attempt {fill_attempts + 1})")
                
                if state in (2, 3):  # Uncompleted or Completed
                    # Check if it actually filled by looking at deal volume
                    if os["deal_vol"] > 0:
                        filled = True
                        print(f"[{now_utc()}] ✅ EXIT_FILLED | Order {oid} filled in {time.time()-t0:.2f}s")
                        break
                    else:
                        print(f"[{now_utc()}] ⏳ EXIT_PENDING | Order {oid} state {state} but not filled yet")
                elif state == 4:  # Cancelled
                    print(f"[{now_utc()}] ❌ EXIT_CANCELLED | Order {oid} was cancelled")
                    break
                elif state == 5:  # Invalid
                    print(f"[{now_utc()}] ❌ EXIT_INVALID | Order {oid} is invalid")
                    break
            else:
                print(f"[{now_utc()}] ⚠️ EXIT_STATE_NULL | Could not get state for order {oid}")
                
            # also consider flat during polling
            if not await get_open_position_exists():
                filled = True
                print(f"[{now_utc()}] ✅ POSITION_CLOSED | Position closed while polling order {oid}")
                break
            fill_attempts += 1
            await asyncio.sleep(0.1)

        if filled:
            send_general_update("ORDER_FILLED", f"EXIT `{side.upper()}` filled (oid `{oid}`) — {reason}")
            last_oid = None
            backoff_idx = 0  # reset and verify loop again (in case partials remain)
            
            # Double-check position is actually closed
            await asyncio.sleep(0.1)  # Brief pause
            if not await get_open_position_exists():
                print(f"[{now_utc()}] ✅ EXIT_CONFIRMED | Position fully closed")
                return True
            else:
                print(f"[{now_utc()}] ⚠️ EXIT_PARTIAL | Position still open, continuing...")
            continue

        last_oid = oid
        backoff_idx = min(backoff_idx + 1, len(REPRICE_BACKOFF_S)-1)
        send_general_update("EXIT_RETRY", f"Repricing exit `{side.upper()}` to book edge; attempt {backoff_idx+1}")

# =========================
# Entry/TP/Exit orchestration
# =========================
async def place_tp_post_only(side: str, entry_px: float):
    """Place take profit order with retry logic"""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            # Calculate TP price
            if side == "long":
                px = round_px(entry_px * (1 + TP_PCT))
                side_enum = OrderSide.CloseLong
            else:  # short
                px = round_px(entry_px * (1 - TP_PCT))
                side_enum = OrderSide.CloseShort
            
            print(f"[{now_utc()}] 🎯 TP_CALC | {side.upper()} TP: {entry_px:.5f} → {px:.5f} ({TP_PCT*100:.2f}%)")
            
            # Check if position still exists before placing TP
            print(f"[{now_utc()}] 🔍 TP_POSITION_CHECK | Verifying position exists before TP attempt {attempt + 1}")
            if not await get_open_position_exists():
                print(f"[{now_utc()}] ⚠️ TP_SKIP | Position no longer exists, skipping TP placement")
                return False
            else:
                print(f"[{now_utc()}] ✅ TP_POSITION_OK | Position exists, proceeding with TP attempt {attempt + 1}")
            
            # Place the order
            oid = await create_post_only(side_enum, px)
            if oid:
                state["tp_order_id"] = oid
                send_general_update("TP_PLACED", f"Placed TP `{side.upper()}` @ `{px:.5f}` (oid `{oid}`)")
                print(f"[{now_utc()}] ✅ TP_SUCCESS | Take profit order {oid} placed @ {px:.5f}")
                return True
            else:
                print(f"[{now_utc()}] ⚠️ TP_RETRY | Attempt {attempt + 1}/{max_retries} failed")
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.2)  # Longer delay between retries
                    
        except Exception as e:
            print(f"[{now_utc()}] ❌ TP_ERROR | Attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(0.2)  # Longer delay between retries
    
    print(f"[{now_utc()}] ❌ TP_FAILED | All {max_retries} attempts failed")
    return False

async def try_enter(side: str, signal_px: float):
    async with trade_lock:
        if state["status"] not in (Status.FLAT, Status.ARMED): return False
        if await get_open_position_exists():
            state["status"] = Status.OPEN; return False
        if time.time() - state["last_entry_at"] < MIN_SECONDS_BETWEEN_ENTRIES:
            return False

        ok, entry_px, order_id = await chase_post_only_entry(side, signal_px)
        if not ok:
            send_error_alert("ENTRY_FAIL", f"{side.upper()} entry not filled (post‑only)")
            state["status"] = Status.FLAT
            state["armed_at"] = None
            state["last_entry_at"] = time.time()
            return False

        state.update({
            "status": Status.OPEN,
            "side": side,
            "entry_price": entry_px,
            "created_at": time.time(),
            "sl_mode": "percent",
            "entry_order_id": order_id,
            "last_entry_at": time.time(),
        })
        send_trade_update(side, entry_px, entry_px, ORDER_VOL, "VODD gate + QI_z&tilt", is_entry=True)

        # immediate TP placement with delay to avoid race condition
        print(f"[{now_utc()}] 🎯 PLACING_TP | Immediately placing TP for {side.upper()} @ {entry_px:.5f}")
        await asyncio.sleep(1.0)  # Longer delay to ensure position is settled
        
        # Double-check position still exists before placing TP
        print(f"[{now_utc()}] 🔍 CHECKING_POSITION | Verifying position exists before TP placement")
        position_exists = await get_open_position_exists()
        if not position_exists:
            print(f"[{now_utc()}] ⚠️ POSITION_GONE | Position closed before TP placement")
            # Try one more time after a longer delay
            await asyncio.sleep(1.0)
            position_exists = await get_open_position_exists()
            if not position_exists:
                print(f"[{now_utc()}] ❌ POSITION_STILL_GONE | Position still not found after 2s delay")
                return True
            else:
                print(f"[{now_utc()}] ✅ POSITION_FOUND_LATE | Position found after longer delay")
        else:
            print(f"[{now_utc()}] ✅ POSITION_CONFIRMED | Position exists, proceeding with TP placement")
            
        tp_success = await place_tp_post_only(side, entry_px)
        if tp_success:
            print(f"[{now_utc()}] ✅ TP_PLACED | Take profit order placed successfully")
        else:
            print(f"[{now_utc()}] ❌ TP_FAILED | Failed to place take profit order")
            send_error_alert("TP_FAIL", f"Failed to place TP for {side.upper()} @ {entry_px:.5f}", "ERROR")
        return True

async def trigger_exit(reason: str):
    if state["status"] not in (Status.OPEN, Status.EXITING): return
    side = state["side"]; entry = state["entry_price"]

    print(f"[{now_utc()}] 🚨 TRIGGER_EXIT | {side.upper()} exit triggered - {reason}")
    
    # Cancel TP order if it exists
    if state.get("tp_order_id"):
        print(f"[{now_utc()}] 🚨 CANCEL_TP | Cancelling TP order {state['tp_order_id']} before exit")
        await cancel_order(state["tp_order_id"])
        state["tp_order_id"] = None

    state["status"] = Status.EXITING
    await chase_post_only_exit(side, entry, reason)

    # finalize if flat
    if not await get_open_position_exists():
        print(f"[{now_utc()}] ✅ TRADE_EXIT | {side.upper()} closed - {reason}")
        send_trade_update(side, state.get("entry_price", 0.0), 0.0, ORDER_VOL, reason, is_entry=False)
        state.update({
            "status": Status.FLAT,
            "side": None,
            "entry_price": None,
            "tp_order_id": None,
            "armed_at": None
        })
    else:
        # exit will continue to chase in the background
        pass

# =========================
# Monitor trade: BE/SL/Timeout + detect TP fill
# =========================
async def monitor_trade():
    while True:
        await asyncio.sleep(0.2)
        if state["status"] not in (Status.OPEN, Status.EXITING): continue
        if not bars: continue

        # TP order monitoring
        if state.get("tp_order_id"):
            tp_os = await get_order_state(state["tp_order_id"])
            if tp_os:
                tp_state = tp_os["state"]
                print(f"[{now_utc()}] 🔍 TP_STATE | Order {state['tp_order_id']} state: {tp_state}")
                
                if tp_state in (2, 3):  # Uncompleted or Completed
                    # Check if it actually filled by looking at deal volume
                    if tp_os["deal_vol"] > 0:
                        print(f"[{now_utc()}] ✅ TP_FILLED | Take profit order {state['tp_order_id']} filled @ {tp_os['deal_avg']:.5f}")
                        send_general_update("TP_FILLED", f"Take profit filled @ {tp_os['deal_avg']:.5f}")
                        state.update({"status": Status.FLAT, "side": None, "entry_price": None, "tp_order_id": None, "armed_at": None})
                        continue
                    else:
                        print(f"[{now_utc()}] ⏳ TP_PENDING | Take profit order {state['tp_order_id']} state {tp_state} but not filled yet")
                elif tp_state == 4:  # TP cancelled
                    print(f"[{now_utc()}] ⚠️ TP_CANCELLED | Take profit order {state['tp_order_id']} was cancelled")
                    state["tp_order_id"] = None
                elif tp_state == 5:  # Invalid
                    print(f"[{now_utc()}] ❌ TP_INVALID | Take profit order {state['tp_order_id']} is invalid")
                    state["tp_order_id"] = None
            else:
                print(f"[{now_utc()}] ⚠️ TP_STATE_NULL | Could not get state for TP order {state['tp_order_id']}")

        # TP detection: if no open position, assume closed (TP or exit)
        # Add small delay to avoid race condition with TP placement
        await asyncio.sleep(0.05)
        if not await get_open_position_exists():
            print(f"[{now_utc()}] ✅ TP_DETECTED | Position closed — TP or exit filled")
            send_general_update("TP_HIT", "Position closed — TP or exit filled")
            state.update({"status": Status.FLAT, "side": None, "entry_price": None, "tp_order_id": None, "armed_at": None})
            continue

        if state["status"] != Status.OPEN:
            continue  # EXITING — let the chaser run

        side = state["side"]; entry = state["entry_price"]
        px = bars[-1]["close"]

        # Breakeven ratchet
        if state["sl_mode"] != "breakeven":
            if (side=="long" and px >= entry*(1+BE_PCT)) or (side=="short" and px <= entry*(1-BE_PCT)):
                state["sl_mode"] = "breakeven"
                send_general_update("TP_ADJUST", "SL ratcheted to entry (breakeven)")

        # Timeout exit
        if time.time() - state["created_at"] >= TRADE_TIMEOUT_S:
            send_general_update("TIMEOUT", f"{side.upper()} timed out {TRADE_TIMEOUT_S}s — exiting")
            await trigger_exit("Timeout")
            continue

        # SL against book
        if best_bid is None or best_ask is None: 
            continue
        if side == "long":
            thresh = entry if state["sl_mode"]=="breakeven" else entry*(1 - SL_PCT)
            if best_bid <= thresh:
                send_general_update("SL_HIT", f"{side.upper()} SL reached — exiting")
                await trigger_exit("Stop Loss")
                continue
        else:
            thresh = entry if state["sl_mode"]=="breakeven" else entry*(1 + SL_PCT)
            if best_ask >= thresh:
                send_general_update("SL_HIT", f"{side.upper()} SL reached — exiting")
                await trigger_exit("Stop Loss")
                continue

# =========================
# Strategy: VODD gate + QI_z/tilt picker
# =========================
class CsvTailLocal(CsvTail): pass  # alias just to keep names clear

async def strategy_loop():
    print(f"[{now_utc()}] 🚀 STARTING BOT | Initializing strategy components...")
    
    # Load historical data
    prefill_bars()
    
    # Initialize strategy components
    gate = VoddGate(VODD_THR)
    picker = DirectionPicker(PERSIST_ROWS)
    
    print(f"[{now_utc()}] ⚙️ CONFIG | VODD threshold: {VODD_THR} | TP: {TP_PCT*100:.2f}% | SL: {SL_PCT*100:.2f}% | Timeout: {TRADE_TIMEOUT_S}s")
    print(f"[{now_utc()}] 💼 POSITION | Size: {ORDER_VOL} | Leverage: {LEVERAGE}x | Fee: {FEE_RT*100:.3f}%")
    print(f"[{now_utc()}] 🎯 STRATEGY | VODD gate + QI_z & tilt | Persistence: {PERSIST_ROWS} rows")
    print(f"[{now_utc()}] 🤖 BOT_START | Strategy loop started | VODD threshold: {VODD_THR}")

    while True:
        await asyncio.sleep(0.25)

        # keep bars fresh
        r = data_tail.last()
        if r is not None:
            new_candle = {
                "timestamp": float(r["_ts"]),
                "open": float(r["open"]), "high": float(r["high"]),
                "low": float(r["low"]),   "close": float(r["close"]),
                "volume": float(r.get("volume",0)), "trade_count": int(r.get("trade_count",0))
            }
            bars.append(new_candle)
            
            # Print new candle info occasionally
            if len(bars) % 60 == 0:  # Every 60 candles (1 minute)
                print(f"[{now_utc()}] 📈 NEW CANDLE | Price: {new_candle['close']:.5f} | Volume: {new_candle['volume']:.1f} | Trades: {new_candle['trade_count']}")

        # only act when flat/armed & cooldown satisfied
        if state["status"] not in (Status.FLAT, Status.ARMED):
            continue
        if time.time() - state["last_entry_at"] < MIN_SECONDS_BETWEEN_ENTRIES:
            continue

        g = gate.check()
        if g and g["cross"]:
            state["status"] = Status.ARMED
            state["armed_at"] = g["ts"]
            print(f"[{now_utc()}] 🎯 TRADE_TRIGGER | VODD armed! Score: {g['score']:.3f} (threshold: {VODD_THR})")
            send_general_update("VODD_ARMED", f"|score| crossed `{VODD_THR}` (score `{g['score']:.3f}`)")

        # armed window (≤ 10s) to pick direction & enter
        if state["status"] == Status.ARMED:
            if (time.time() - (state["armed_at"] or 0)) > 10:
                print(f"[{now_utc()}] ⏰ ARMED_EXPIRED | No direction within 10s - disarming")
                send_general_update("ENTRY_ABORT", "no direction within 10s → disarm")
                state["status"] = Status.FLAT
                state["armed_at"] = None
                continue

            side = picker.pick()
            if side is None:
                continue

            signal_px = bars[-1]["close"] if bars else ((best_bid or best_ask) or 0.0)
            print(f"[{now_utc()}] 🚀 TRADE_ENTRY | Attempting {side.upper()} entry at {signal_px:.5f}")
            ok = await try_enter(side, signal_px)
            if not ok:
                # disarm on failed entry
                state["status"] = Status.FLAT
                state["armed_at"] = None

# =========================
# MAIN
# =========================
async def status_monitor():
    """Print status updates every second"""
    while True:
        await asyncio.sleep(1.0)
        print_csv_status()
        print_trade_update()

async def main():
    print(f"[{now_utc()}] 🌟 BOT INITIALIZATION | Starting live trading bot...")
    print(f"[{now_utc()}] 📁 FILES | Data: {DATA_CSV} | VODD: {VODD_CSV} | Features: {FEAT_CSV}")
    print(f"[{now_utc()}] 🔗 MEXC | Symbol: {SYMBOL} | Token: {USER_TOKEN[:10]}...")
    print(f"[{now_utc()}] 📱 TELEGRAM | Bot: {TELEGRAM_BOT_TOKEN[:10]}... | Chat: {TELEGRAM_CHAT_ID}")
    
    send_general_update("BOT_START", "Live bot started (Post‑Only; VODD gate + QI_z & tilt)")
    
    print(f"[{now_utc()}] 🔄 STARTING TASKS | Launching async tasks...")
    tasks = [
        asyncio.create_task(update_best_bid_ask()),
        asyncio.create_task(strategy_loop()),
        asyncio.create_task(monitor_trade()),
        asyncio.create_task(status_monitor()),
    ]
    print(f"[{now_utc()}] ✅ TASKS LAUNCHED | {len(tasks)} tasks running")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        send_general_update("BOT_STOP", "Bot stopped by user")
        log("Bot stopped by user")
    except RuntimeError:
        # Some environments disallow asyncio.run twice; fallback
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except Exception as e:
        send_error_alert("BOT_CRASH", f"Unhandled error: {e}", "CRITICAL")
        raise
