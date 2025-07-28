import pandas as pd
import pandas as pd
import customtkinter as ctk
import plotly.graph_objs as go
import plotly.io as pio
import numpy as np
import datetime
import threading
import tempfile
import webbrowser
import matplotlib.pyplot as plt
import io
import random
import optuna
import optuna.visualization as ov

# --- Strategy Logic ---
def compute_features(df, efficiency_eps=1e-8):
    df['Efficiency'] = np.abs(df['close'] - df['open']) / (df['high'] - df['low'] + efficiency_eps)
    df['TradeCount60s'] = df['trade_count'].rolling(60, min_periods=1).mean()
    df['TradeSpike'] = df['trade_count'] / (df['TradeCount60s'] + 1e-8)
    df['Returns'] = df['close'].pct_change().fillna(0)
    df['Vol60s'] = df['Returns'].rolling(60, min_periods=1).std()
    df['VolZ'] = (df['Vol60s'] - df['Vol60s'].rolling(300, min_periods=1).mean()) / (df['Vol60s'].rolling(300, min_periods=1).std() + 1e-8)
    return df

def run_backtest(df, eff_thresh, spike_thresh, volz_breakout, volz_fade, tp, trailing, timeout, initial_balance=100000, risk_per_trade=0.05, fee=0.0, leverage=100):
    trades = []
    in_position = False
    entry_idx = None
    entry_price = None
    peak_price = None
    trough_price = None
    entry_time = None
    position_type = None
    balance = initial_balance
    equity_curve = []
    max_balance = initial_balance

    for i, row in df.iterrows():
        ts = row['timestamp']
        if not in_position:
            # Long entry
            if (row['Efficiency'] > eff_thresh and
                row['TradeSpike'] > spike_thresh and
                row['VolZ'] > volz_breakout):
                in_position = True
                position_type = 'long'
                entry_idx = i
                entry_price = row['close']
                peak_price = entry_price
                entry_time = ts
                # Calculate position size with leverage
                size = balance * risk_per_trade * leverage / entry_price

            # Short entry
            elif (row['Efficiency'] > eff_thresh and
                  row['TradeSpike'] > spike_thresh and
                  row['VolZ'] < volz_fade):
                in_position = True
                position_type = 'short'
                entry_idx = i
                entry_price = row['close']
                trough_price = entry_price
                entry_time = ts
                # Calculate position size with leverage
                size = balance * risk_per_trade * leverage / entry_price

        else:
            price = row['close']
            duration = (ts - entry_time).total_seconds()
            exit_reason = None

            if position_type == 'long':
                peak_price = max(peak_price, price)
                ret = (price - entry_price) / entry_price
                peak_ret = (peak_price - entry_price) / entry_price
                
                if ret >= tp:
                    exit_reason = 'TP'
                elif peak_ret - ret >= trailing:
                    exit_reason = 'Trailing'
                elif duration >= timeout:
                    exit_reason = 'Timeout'

            else:  # short position
                trough_price = min(trough_price, price)
                ret = (entry_price - price) / entry_price
                trough_ret = (entry_price - trough_price) / entry_price
                
                if ret >= tp:
                    exit_reason = 'TP'
                elif trough_ret - ret >= trailing:
                    exit_reason = 'Trailing'
                elif duration >= timeout:
                    exit_reason = 'Timeout'
            
            if exit_reason:
                gross_pnl = ret * size * entry_price
                fee_paid = 0.0  # No fee
                net_pnl = gross_pnl
                balance += net_pnl
                trades.append({
                    'entry': entry_time,
                    'exit': ts,
                    'entry_price': entry_price,
                    'exit_price': price,
                    'pnl_pct': ret,
                    'gross_pnl': gross_pnl,
                    'net_pnl': net_pnl,
                    'reason': exit_reason,
                    'size': size,
                    'balance': balance,
                    'duration': duration,
                    'fee_paid': fee_paid,
                    'type': position_type
                })
                equity_curve.append({'time': ts, 'balance': balance})
                max_balance = max(max_balance, balance)
                in_position = False
                position_type = None

    trades_df = pd.DataFrame(trades)
    equity_df = pd.DataFrame(equity_curve)
    return trades_df, equity_df

def compute_stats(trades_df, equity_df, initial_balance=100000):
    if trades_df.empty:
        return {}
    total_pnl = trades_df['net_pnl'].sum()
    win_rate = (trades_df['net_pnl'] > 0).mean()
    num_trades = len(trades_df)
    avg_duration = trades_df['duration'].mean()
    returns = equity_df['balance'].pct_change().dropna()
    sharpe = returns.mean() / (returns.std() + 1e-8) * np.sqrt(365*24*60*60)  # annualized for 1s bars
    max_balance = equity_df['balance'].cummax()
    drawdown = (equity_df['balance'] - max_balance) / max_balance
    max_drawdown = drawdown.min()
    final_balance = equity_df['balance'].iloc[-1] if not equity_df.empty else np.nan
    total_pnl_pct = (final_balance - initial_balance) / initial_balance * 100 if not np.isnan(final_balance) else np.nan
    # Calculate average daily %
    if not equity_df.empty:
        days = (equity_df['time'].iloc[-1] - equity_df['time'].iloc[0]).total_seconds() / (24*3600)
        avg_daily_pct = (final_balance / initial_balance) ** (1 / max(days, 1e-8)) - 1
        avg_daily_pct *= 100
    else:
        avg_daily_pct = np.nan
    stats = {
        'Total PnL': total_pnl,
        'Total PnL %': total_pnl_pct,
        'Win Rate': win_rate,
        'Number of Trades': num_trades,
        'Average Duration (s)': avg_duration,
        'Sharpe Ratio': sharpe,
        'Max Drawdown': max_drawdown,
        'Final Balance': final_balance,
        'Avg Daily %': avg_daily_pct
    }
    return stats

# --- UI ---
class BacktesterApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title('PulseFlow Scalper Backtester')
        self.geometry('600x700')
        self.df = None
        self.trades = None
        self.equity = None # Added for equity curve
        self.stats = None # Added for stats
        self.create_widgets()

    def create_widgets(self):
        self.load_btn = ctk.CTkButton(self, text='Load Data', command=self.load_data)
        self.load_btn.pack(pady=10)
        self.param_frame = ctk.CTkFrame(self)
        self.param_frame.pack(pady=10, fill='x')
        self.eff_label = ctk.CTkLabel(self.param_frame, text='Efficiency Threshold:')
        self.eff_label.grid(row=0, column=0)
        self.eff_entry = ctk.CTkEntry(self.param_frame)
        self.eff_entry.insert(0, '0.6296')
        self.eff_entry.grid(row=0, column=1)
        self.spike_label = ctk.CTkLabel(self.param_frame, text='Trade Spike Threshold:')
        self.spike_label.grid(row=1, column=0)
        self.spike_entry = ctk.CTkEntry(self.param_frame)
        self.spike_entry.insert(0, '1.6204')
        self.spike_entry.grid(row=1, column=1)
        self.volz_label = ctk.CTkLabel(self.param_frame, text='VolZ Breakout:')
        self.volz_label.grid(row=2, column=0)
        self.volz_entry = ctk.CTkEntry(self.param_frame)
        self.volz_entry.insert(0, '0.1848')
        self.volz_entry.grid(row=2, column=1)
        self.volz_fade_label = ctk.CTkLabel(self.param_frame, text='VolZ Fade (Short) Threshold:')
        self.volz_fade_label.grid(row=3, column=0)
        self.volz_fade_entry = ctk.CTkEntry(self.param_frame)
        self.volz_fade_entry.insert(0, '-0.1848')
        self.volz_fade_entry.grid(row=3, column=1)
        self.tp_label = ctk.CTkLabel(self.param_frame, text='Take Profit %:')
        self.tp_label.grid(row=4, column=0)
        self.tp_entry = ctk.CTkEntry(self.param_frame)
        self.tp_entry.insert(0, '0.00179')
        self.tp_entry.grid(row=4, column=1)
        self.trailing_label = ctk.CTkLabel(self.param_frame, text='Trailing Stop %:')
        self.trailing_label.grid(row=5, column=0)
        self.trailing_entry = ctk.CTkEntry(self.param_frame)
        self.trailing_entry.insert(0, '0.0005')
        self.trailing_entry.grid(row=5, column=1)
        self.timeout_label = ctk.CTkLabel(self.param_frame, text='Timeout (sec):')
        self.timeout_label.grid(row=6, column=0)
        self.timeout_entry = ctk.CTkEntry(self.param_frame)
        self.timeout_entry.insert(0, '279')
        self.timeout_entry.grid(row=6, column=1)
        self.leverage_label = ctk.CTkLabel(self.param_frame, text='Leverage:')
        self.leverage_label.grid(row=7, column=0)
        self.leverage_entry = ctk.CTkEntry(self.param_frame)
        self.leverage_entry.insert(0, '100')
        self.leverage_entry.grid(row=7, column=1)
        self.risk_label = ctk.CTkLabel(self.param_frame, text='Risk Per Trade (%):')
        self.risk_label.grid(row=8, column=0)
        self.risk_entry = ctk.CTkEntry(self.param_frame)
        self.risk_entry.insert(0, '5')
        self.risk_entry.grid(row=8, column=1)
        self.bt_btn = ctk.CTkButton(self, text='Run Backtest', command=self.run_backtest_thread)
        self.bt_btn.pack(pady=10)
        self.hyperopt_btn = ctk.CTkButton(self, text='Hyperopt', command=self.run_hyperopt_thread)
        self.hyperopt_btn.pack(pady=10)
        self.status = ctk.CTkLabel(self, text='')
        self.status.pack()
        self.chart_btn = ctk.CTkButton(self, text='Show Charts', command=self.show_charts, state='disabled')
        self.chart_btn.pack(pady=10)

    def load_data(self):
        self.status.configure(text='Loading data...')
        try:
            df = pd.read_parquet('BTCUSDT_1s_June2025.parquet')
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            else:
                # Use index as timestamp if not present
                df = df.reset_index()
                if 'datetime' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['datetime'])
                else:
                    df['timestamp'] = pd.to_datetime(df.index)
            self.df = compute_features(df)
            self.status.configure(text='Data loaded! Rows: %d' % len(self.df))
        except Exception as e:
            self.status.configure(text=f'Error loading data: {e}')

    def run_backtest_thread(self):
        threading.Thread(target=self.run_backtest, daemon=True).start()

    def run_backtest(self):
        if self.df is None:
            self.status.configure(text='Load data first!')
            return
        self.status.configure(text='Running backtest...')
        try:
            eff = float(self.eff_entry.get())
            spike = float(self.spike_entry.get())
            volz = float(self.volz_entry.get())
            volz_fade = float(self.volz_fade_entry.get())
            tp = float(self.tp_entry.get())
            trailing = float(self.trailing_entry.get())
            timeout = float(self.timeout_entry.get())
            initial_balance = 100000 # Default initial balance
            leverage = float(self.leverage_entry.get())
            risk_per_trade = float(self.risk_entry.get()) / 100.0
            trades, equity = run_backtest(self.df, eff, spike, volz, volz_fade, tp, trailing, timeout, initial_balance=initial_balance, risk_per_trade=risk_per_trade, fee=0.0, leverage=leverage)
            self.trades = trades
            self.equity = equity
            if not trades.empty:
                trades.to_csv('trades_export.csv', index=False)
            self.stats = compute_stats(trades, equity, initial_balance)
            stats_str = '\n'.join([f"{k}: {v:.4f}" if isinstance(v, float) else f"{k}: {v}" for k, v in self.stats.items()])
            self.status.configure(text=f'Backtest done! Trades: {len(trades)}\n{stats_str}')
            self.chart_btn.configure(state='normal')
        except Exception as e:
            self.status.configure(text=f'Error: {e}')

    def run_hyperopt_thread(self):
        threading.Thread(target=self.run_hyperopt, daemon=True).start()

    def run_hyperopt(self, n_trials=25):
        if self.df is None:
            self.status.configure(text='Load data first!')
            return
        self.status.configure(text='Running Optuna hyperopt...')
        self.update()
        df = self.df
        results = []
        initial_balance = 100000
        epsilon = 1e-6
        def objective(trial):
            eff = trial.suggest_float('efficiency', 0.4, 0.8)
            spike = trial.suggest_float('spike', 1.0, 2.0)
            volz = trial.suggest_float('volz', 0.0, 1.5)
            volz_fade = trial.suggest_float('volz_fade', -1.5, 4.0)
            tp = trial.suggest_float('tp', 0.001, 0.0025)
            trailing = trial.suggest_float('trailing', 0.0005, 0.00075)
            timeout = trial.suggest_float('timeout', 30, 300)
            leverage = trial.suggest_float('leverage', 10, 200)
            risk_per_trade = trial.suggest_float('risk_per_trade', 0.01, 0.10)
            trades, equity = run_backtest(df, eff, spike, volz, volz_fade, tp, trailing, timeout, initial_balance=initial_balance, risk_per_trade=risk_per_trade, fee=0.0, leverage=leverage)
            stats = compute_stats(trades, equity, initial_balance)
            net_pnl = stats.get('Total PnL', 0)
            max_dd = abs(stats.get('Max Drawdown', 0))
            score = net_pnl / (max_dd + epsilon)
            trial.set_user_attr('Total PnL', stats.get('Total PnL', 0))
            trial.set_user_attr('Total PnL %', stats.get('Total PnL %', 0))
            trial.set_user_attr('Avg Daily %', stats.get('Avg Daily %', 0))
            trial.set_user_attr('Win Rate', stats.get('Win Rate', 0))
            trial.set_user_attr('Sharpe Ratio', stats.get('Sharpe Ratio', 0))
            trial.set_user_attr('Max Drawdown', stats.get('Max Drawdown', 0))
            trial.set_user_attr('Final Balance', stats.get('Final Balance', 0))
            trial.set_user_attr('Leverage', leverage)
            trial.set_user_attr('Risk Per Trade', risk_per_trade)
            trial.set_user_attr('Risk-Adj Score', score)
            return score
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
        # Collect all results
        for t in study.trials:
            results.append({
                'efficiency': t.params.get('efficiency'),
                'spike': t.params.get('spike'),
                'volz': t.params.get('volz'),
                'volz_fade': t.params.get('volz_fade'),
                'tp': t.params.get('tp'),
                'trailing': t.params.get('trailing'),
                'timeout': t.params.get('timeout'),
                'Leverage': t.user_attrs.get('Leverage'),
                'Risk Per Trade': t.user_attrs.get('Risk Per Trade'),
                'Total PnL': t.user_attrs.get('Total PnL'),
                'Total PnL %': t.user_attrs.get('Total PnL %'),
                'Avg Daily %': t.user_attrs.get('Avg Daily %'),
                'Win Rate': t.user_attrs.get('Win Rate'),
                'Sharpe Ratio': t.user_attrs.get('Sharpe Ratio'),
                'Max Drawdown': t.user_attrs.get('Max Drawdown'),
                'Final Balance': t.user_attrs.get('Final Balance'),
                'Risk-Adj Score': t.user_attrs.get('Risk-Adj Score'),
                'score': t.value
            })
        pd.DataFrame(results).to_csv('optuna_results.csv', index=False)
        # Show best result
        best = study.best_trial
        best_str = f"Best Params (Optuna):\nEfficiency: {best.params['efficiency']:.4f}\nSpike: {best.params['spike']:.4f}\nVolZ: {best.params['volz']:.4f}\nVolZ Fade: {best.params['volz_fade']:.4f}\nTP: {best.params['tp']:.4f}\nTrailing: {best.params['trailing']:.4f}\nTimeout: {best.params['timeout']:.2f}\nLeverage: {best.params['leverage']:.2f}\nRisk Per Trade: {best.params['risk_per_trade']:.2f}\n\nStats:\n" + '\n'.join([f"{k}: {v:.4f}" if isinstance(v, float) else f"{k}: {v}" for k, v in best.user_attrs.items()])
        import tkinter.messagebox as mb
        mb.showinfo("Optuna Hyperopt Best Result", best_str)
        self.status.configure(text='Optuna hyperopt done! See optuna_results.csv for all results.')
        self.show_optuna_visuals(study, results)

    def show_optuna_visuals(self, study, results):
        import tempfile, webbrowser
        import plotly.express as px
        df = pd.DataFrame(results)
        # Optimization history (risk-adjusted score)
        fig1 = ov.plot_optimization_history(study)
        with tempfile.NamedTemporaryFile('w', delete=False, suffix='.html') as f1:
            fig1.write_html(f1.name)
            webbrowser.open('file://' + f1.name)
        # Parameter importance (risk-adjusted score)
        fig2 = ov.plot_param_importances(study)
        with tempfile.NamedTemporaryFile('w', delete=False, suffix='.html') as f2:
            fig2.write_html(f2.name)
            webbrowser.open('file://' + f2.name)
        # Parallel coordinates
        fig3 = ov.plot_parallel_coordinate(study)
        with tempfile.NamedTemporaryFile('w', delete=False, suffix='.html') as f3:
            fig3.write_html(f3.name)
            webbrowser.open('file://' + f3.name)
        # Slice plot
        fig4 = ov.plot_slice(study)
        with tempfile.NamedTemporaryFile('w', delete=False, suffix='.html') as f4:
            fig4.write_html(f4.name)
            webbrowser.open('file://' + f4.name)
        # Scatter plot: Net PnL vs Max Drawdown, colored by risk-adjusted score
        if 'Total PnL' in df.columns and 'Max Drawdown' in df.columns and 'Risk-Adj Score' in df.columns:
            fig5 = px.scatter(df, x='Max Drawdown', y='Total PnL', color='Risk-Adj Score', title='Net PnL vs Max Drawdown (colored by Risk-Adj Score)', color_continuous_scale='Viridis')
            with tempfile.NamedTemporaryFile('w', delete=False, suffix='.html') as f5:
                fig5.write_html(f5.name)
                webbrowser.open('file://' + f5.name)
        # Scatter plot: Net PnL vs Win Rate, colored by risk-adjusted score
        if 'Total PnL' in df.columns and 'Win Rate' in df.columns and 'Risk-Adj Score' in df.columns:
            fig6 = px.scatter(df, x='Win Rate', y='Total PnL', color='Risk-Adj Score', title='Net PnL vs Win Rate (colored by Risk-Adj Score)', color_continuous_scale='Viridis')
            with tempfile.NamedTemporaryFile('w', delete=False, suffix='.html') as f6:
                fig6.write_html(f6.name)
                webbrowser.open('file://' + f6.name)

    def show_charts(self):
        if self.df is None or self.trades is None or self.trades.empty:
            self.status.configure(text='No trades to plot!')
            return
        import plotly.subplots as sp
        df = self.df
        trades = self.trades
        equity = self.equity
        fig = sp.make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.03,
                               subplot_titles=("Price & Trades", "Equity Curve", "Drawdown"))
        fig.add_trace(go.Candlestick(x=df['timestamp'], open=df['open'], high=df['high'], low=df['low'], close=df['close'], name='Price'), row=1, col=1)
        longs = trades[trades['type'] == 'long']
        shorts = trades[trades['type'] == 'short']
        fig.add_trace(go.Scatter(x=longs['entry'], y=longs['entry_price'], mode='markers', marker=dict(color='green', size=8), name='Long Entry'), row=1, col=1)
        fig.add_trace(go.Scatter(x=longs['exit'], y=longs['exit_price'], mode='markers', marker=dict(color='lime', size=8), name='Long Exit'), row=1, col=1)
        fig.add_trace(go.Scatter(x=shorts['entry'], y=shorts['entry_price'], mode='markers', marker=dict(color='red', size=8), name='Short Entry'), row=1, col=1)
        fig.add_trace(go.Scatter(x=shorts['exit'], y=shorts['exit_price'], mode='markers', marker=dict(color='pink', size=8), name='Short Exit'), row=1, col=1)
        fig.add_trace(go.Scatter(x=equity['time'], y=equity['balance'], mode='lines+markers', name='Equity'), row=2, col=1)
        if not equity.empty:
            max_balance = equity['balance'].cummax()
            drawdown = (equity['balance'] - max_balance) / max_balance
            fig.add_trace(go.Scatter(x=equity['time'], y=drawdown, mode='lines', name='Drawdown'), row=3, col=1)
        fig2 = go.Figure()
        fig2.add_trace(go.Histogram(x=trades['net_pnl'], nbinsx=50, name='Net PnL'))
        fig2.update_layout(title='Trade Net PnL Histogram', xaxis_title='Net PnL', yaxis_title='Count')
        fig3 = go.Figure()
        fig3.add_trace(go.Histogram(x=trades['duration'], nbinsx=50, name='Duration'))
        fig3.update_layout(title='Trade Duration Histogram', xaxis_title='Duration (s)', yaxis_title='Count')
        import tempfile, webbrowser, plotly.io as pio
        with tempfile.NamedTemporaryFile('w', delete=False, suffix='.html') as f:
            pio.write_html(fig, file=f.name, auto_open=False)
            webbrowser.open('file://' + f.name)
        with tempfile.NamedTemporaryFile('w', delete=False, suffix='.html') as f2:
            pio.write_html(fig2, file=f2.name, auto_open=False)
            webbrowser.open('file://' + f2.name)
        with tempfile.NamedTemporaryFile('w', delete=False, suffix='.html') as f3:
            pio.write_html(fig3, file=f3.name, auto_open=False)
            webbrowser.open('file://' + f3.name)
        if hasattr(self, 'stats') and self.stats:
            import tkinter.messagebox as mb
            stats_str = '\n'.join([f"{k}: {v:.4f}" if isinstance(v, float) else f"{k}: {v}" for k, v in self.stats.items()])
            mb.showinfo("Backtest Stats", stats_str)

if __name__ == '__main__':
    ctk.set_appearance_mode('dark')
    app = BacktesterApp()
    app.mainloop() 