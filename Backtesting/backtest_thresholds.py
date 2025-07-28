import pandas as pd
import numpy as np

# Load 1s data
FILENAME = 'BTCUSDT_1s_June2025.parquet'
df = pd.read_parquet(FILENAME)

# Ensure datetime index
if not isinstance(df.index, pd.DatetimeIndex):
    df.index = pd.to_datetime(df.index)

# Calculate rolling indicators
# Spike: trade_count / mean(trade_count last 60s)
df['trade_count_60s_mean'] = df['trade_count'].rolling(60).mean()
df['spike'] = df['trade_count'] / (df['trade_count_60s_mean'] + 1e-8)

# Volz: z-score of 60s rolling volatility vs last 300
closes = df['close'].values
returns = np.diff(closes) / closes[:-1]
vol_60s = pd.Series(returns).rolling(60).std()
vol_60s_values = [np.std(returns[i-60:i]) if i >= 60 else np.nan for i in range(60, len(returns)+1)]
vol_300_mean = pd.Series(vol_60s_values).rolling(300).mean()
vol_300_std = pd.Series(vol_60s_values).rolling(300).std()
volz = (vol_60s - vol_300_mean) / (vol_300_std + 1e-8)
volz_full = np.concatenate([np.full(61, np.nan), volz.values])
df['volz'] = volz_full[:len(df)]

# 5s rolling averages for trade_count and volume
trade_count_5s_avg = df['trade_count'].rolling(5).mean()
volume_5s_avg = df['volume'].rolling(5).mean()
df['trade_count_5s_avg'] = trade_count_5s_avg
df['volume_5s_avg'] = volume_5s_avg

move_threshold = 0.0015  # 0.15%
window_size = 180  # 3 minutes (180 seconds)

# Count total actual moves for recall calculation (in 3 min window)
actual_moves = 0
for i in range(len(df) - window_size):
    start_price = df.iloc[i]['close']
    window = df.iloc[i+1:i+1+window_size]
    max_move = (window['close'].max() - start_price) / start_price
    min_move = (window['close'].min() - start_price) / start_price
    if max_move >= move_threshold or min_move <= -move_threshold:
        actual_moves += 1

# Enhanced noise filtering trigger logic
trigger_indices = []
for i in range(4, len(df) - window_size):
    # 1. VolZ > 0.75 for at least 3 out of last 5 seconds
    volz_last5 = df['volz'].iloc[i-4:i+1]
    if (volz_last5 > 0.75).sum() < 3:
        continue
    # 2. Spike strictly increasing for 3 consecutive seconds
    s2, s1, s0 = df['spike'].iloc[i-2], df['spike'].iloc[i-1], df['spike'].iloc[i]
    if not (s2 < s1 < s0):
        continue
    # 3. Volume/trade_count shift
    tc = df['trade_count'].iloc[i]
    tc5 = df['trade_count_5s_avg'].iloc[i]
    vol = df['volume'].iloc[i]
    vol5 = df['volume_5s_avg'].iloc[i]
    if not (tc > tc5 * 1.5 or vol > vol5 * 2):
        continue
    trigger_indices.append(df.index[i])

true_positives = 0
false_positives = 0
true_positive_times = []
false_positive_times = []
for i in trigger_indices:
    idx = df.index.get_loc(i)
    if idx + window_size >= len(df):
        continue
    start_price = df.iloc[idx]['close']
    window = df.iloc[idx+1:idx+1+window_size]
    max_move = (window['close'].max() - start_price) / start_price
    min_move = (window['close'].min() - start_price) / start_price
    if max_move >= move_threshold or min_move <= -move_threshold:
        true_positives += 1
        true_positive_times.append(i)
    else:
        false_positives += 1
        false_positive_times.append(i)
total_triggers = true_positives + false_positives
recall = true_positives / actual_moves if actual_moves > 0 else 0
precision = true_positives / total_triggers if total_triggers > 0 else 0
f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

print(f"Total triggers: {total_triggers}")
print(f"True positives (move happened): {true_positives} ({100*true_positives/total_triggers:.2f}%)")
print(f"False positives (no move): {false_positives} ({100*false_positives/total_triggers:.2f}%)")
print(f"Recall (TP/actual moves): {true_positives}/{actual_moves} = {100*true_positives/actual_moves:.2f}%")
print(f"F1 score: {f1*100:.2f}%")
print("\nExample true positive times:", true_positive_times[:5])
print("Example false positive times:", false_positive_times[:5])