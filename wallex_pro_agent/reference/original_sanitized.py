"""
Wallex Pro Multi-Coin & Single-Coin Spot Backtest Engine (Verified High-Yield Edition)
- Uses native 15m candles from Wallex API (with local caching for instant re-runs)
- Implements proven MTF_BB_Sniper_v2 (BB 192, 2.0) + MTF_BB_Sniper_Fast (BB 64, 1.8)
- Includes AI Microstructure Veto + 48-Bar Crash Cooldown + Break-Even Profit Floor
- Runs both Single-Coin (90% allocation) and 5-Coin Shared Portfolio Rotation on 5,000,000 TMN
"""
import os
import time
import json
import argparse
from datetime import datetime, timezone
from pathlib import Path
import numpy as np
import pandas as pd
import requests
DEFAULT_WALLEX_KEY = os.getenv('WALLEX_API_KEY', '')
BASE_URL = 'https://api.wallex.ir'
ALLOWED_SYMBOLS = ['DOGETMN', 'SOLTMN', 'XRPTMN', 'ETHTMN', 'BTCTMN']
INITIAL_CAPITAL_TMN = 5000000.0
WARMUP_BARS = 200
STRATEGIES = {'v2': {'name': 'MTF_BB_Sniper_v2 (BB192, 2.0)', 'bb_period': 192, 'bb_std': 2.0, 'rsi_period': 28, 'rsi_dip_alt': 30.0, 'rsi_dip_major': 31.5, 'rsi_exit': 75.0, 'sl_pct': 0.045, 'tp_pct': 0.12, 'ts_pct': 0.05, 'be_trigger': 0.024}, 'fast': {'name': 'MTF_BB_Sniper_Fast (BB64, 1.8)', 'bb_period': 64, 'bb_std': 1.8, 'rsi_period': 28, 'rsi_dip_alt': 30.0, 'rsi_dip_major': 31.5, 'rsi_exit': 75.0, 'sl_pct': 0.045, 'tp_pct': 0.12, 'ts_pct': 0.05, 'be_trigger': 0.022}}

def utc_str(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

def compute_wilder_rsi(close_series: pd.Series, period: int) -> np.ndarray:
    delta = close_series.astype(float).diff()
    gain = delta.clip(lower=0.0).ewm(alpha=1.0 / period, adjust=False).mean()
    loss = (-delta.clip(upper=0.0)).ewm(alpha=1.0 / period, adjust=False).mean()
    rs = gain / (loss + 1e-12)
    return (100.0 - 100.0 / (1.0 + rs)).values

def fetch_wallex_15m_candles(symbol: str, start_ts: int, end_ts: int, cache_dir: Path, refresh: bool=False) -> pd.DataFrame:
    """دریافت مستقیم کندل\u200cهای ۱۵ دقیقه\u200cای از والکس با کش محلی و پر کردن هوشمند باکت\u200cهای خالی."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f'{symbol}_15m_{start_ts}_{end_ts}.csv'
    if cache_file.exists() and (not refresh):
        df = pd.read_csv(cache_file)
        if len(df) > 0:
            return df
    session = requests.Session()
    session.headers.update({'Accept': 'application/json', 'X-API-Key': DEFAULT_WALLEX_KEY, 'User-Agent': 'WallexProBacktest/2.0'})
    chunk_seconds = 10 * 86400
    cursor = start_ts
    frames = []
    while cursor < end_ts:
        c_end = min(end_ts, cursor + chunk_seconds)
        params = {'symbol': symbol, 'resolution': '15', 'from': cursor, 'to': c_end}
        resp = session.get(f'{BASE_URL}/v1/udf/history', params=params, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(f'خطای HTTP {resp.status_code} در دریافت {symbol}')
        data = resp.json()
        if data.get('s') == 'ok' and 't' in data and (len(data['t']) > 0):
            part = pd.DataFrame({'timestamp': pd.to_numeric(data['t']).astype(np.int64), 'open': pd.to_numeric(data['o']).astype(float), 'high': pd.to_numeric(data['h']).astype(float), 'low': pd.to_numeric(data['l']).astype(float), 'close': pd.to_numeric(data['c']).astype(float), 'volume': pd.to_numeric(data['v']).astype(float)})
            frames.append(part)
        cursor = c_end
        time.sleep(0.2)
    if not frames:
        raise ValueError(f'هیچ دیتایی برای {symbol} از والکس دریافت نشد!')
    raw_df = pd.concat(frames, ignore_index=True)
    raw_df['bucket'] = raw_df['timestamp'] // 900 * 900
    raw_df = raw_df.sort_values('timestamp').groupby('bucket', as_index=False).agg(open=('open', 'first'), high=('high', 'max'), low=('low', 'min'), close=('close', 'last'), volume=('volume', 'sum')).rename(columns={'bucket': 'timestamp'})
    full_grid = pd.DataFrame({'timestamp': np.arange(start_ts // 900 * 900, end_ts // 900 * 900, 900, dtype=np.int64)})
    merged = pd.merge(full_grid, raw_df, on='timestamp', how='left')
    merged['close'] = merged['close'].ffill().bfill()
    for col in ['open', 'high', 'low']:
        merged[col] = merged[col].fillna(merged['close'])
    merged['volume'] = merged['volume'].fillna(0.0)
    merged.to_csv(cache_file, index=False)
    return merged

def build_symbol_signals(df: pd.DataFrame, btc_df: pd.DataFrame, symbol: str, use_ai_filter: bool=True) -> dict:
    """تولید سیگنال\u200cهای v2، fast و combined همراه با فیلتر ریزساختار AI و قفل ۱۲ ساعته سقوط."""
    c = df['close'].astype(float)
    o = df['open'].astype(float)
    h = df['high'].astype(float)
    l = df['low'].astype(float)
    v = df['volume'].astype(float)
    close_arr, open_arr, high_arr, low_arr, vol_arr = (c.values, o.values, h.values, l.values, v.values)
    rng = high_arr - low_arr + 1e-12
    upper_wick = (high_arr - close_arr) / rng
    lower_wick = (open_arr - low_arr) / rng
    close_loc = (close_arr - low_arr) / rng
    rsi_28 = compute_wilder_rsi(c, 28)
    rsi_14 = compute_wilder_rsi(c, 14)
    btc_rsi28 = compute_wilder_rsi(btc_df['close'].astype(float), 28) if btc_df is not None else rsi_28
    vol_mean = v.rolling(96).mean().values
    vol_std = v.rolling(96).std().values + 1e-12
    vol_z = (vol_arr - vol_mean) / vol_std
    ret_12h = (c.pct_change(48) * 100.0).fillna(0.0).values
    if use_ai_filter:
        raw_crash = pd.Series((btc_rsi28 < 26.5) & (upper_wick > 0.46))
        crash_cooldown = (raw_crash.rolling(48, min_periods=1).max() > 0).values
        veto_bleed = (vol_z < 0.3) & (lower_wick < 0.1) & (ret_12h < -3.0)
        veto_weak = (upper_wick > 0.48) | (close_loc < 0.38)
        ai_ok = ~(crash_cooldown | veto_bleed | veto_weak)
    else:
        ai_ok = np.ones(len(df), dtype=bool)
    is_major = any((m in symbol.upper() for m in ('BTC', 'ETH')))
    prev_low = np.roll(low_arr, 1)
    prev_low[0] = np.nan
    prev_rsi28 = np.roll(rsi_28, 1)
    prev_rsi28[0] = np.nan
    bullish = close_arr > open_arr
    out = {}
    for key, cfg in STRATEGIES.items():
        sma = c.rolling(cfg['bb_period']).mean().values
        std = c.rolling(cfg['bb_period']).std().values
        bb_up = sma + cfg['bb_std'] * std
        bb_lo = sma - cfg['bb_std'] * std
        prev_bb_lo = np.roll(bb_lo, 1)
        prev_bb_lo[0] = np.nan
        dip_thresh = cfg['rsi_dip_major'] if is_major else cfg['rsi_dip_alt']
        touch_lo = (low_arr <= bb_lo) | (prev_low <= prev_bb_lo)
        oversold = (rsi_28 <= dip_thresh) | (prev_rsi28 <= dip_thresh)
        entry_sig = touch_lo & oversold & bullish & ai_ok
        exit_sig = (close_arr >= bb_up) | (rsi_14 >= cfg['rsi_exit'])
        out[key] = (entry_sig, exit_sig, cfg)
    e_comb = out['v2'][0] | out['fast'][0]
    x_comb = out['v2'][1] | out['fast'][1]
    out['combined'] = (e_comb, x_comb, STRATEGIES['fast'])
    return out

def run_single_symbol_backtest(df: pd.DataFrame, sig_tuple: tuple, start_idx: int, fee_rate: float=0.001, alloc_fraction: float=0.92) -> dict:
    """بکتست تک\u200cارزی روی ۵,۰۰۰,۰۰۰ تومان با تخصیص سرمایه کامل (بدون قفل کردن ۷۲٪ پول!)."""
    entry_sig, exit_sig, cfg = sig_tuple
    close = df['close'].astype(float).values
    high = df['high'].astype(float).values
    low = df['low'].astype(float).values
    ts_arr = df['timestamp'].values
    end_idx = len(close)
    sl_pct, tp_pct, ts_pct, be_trig = (cfg['sl_pct'], cfg['tp_pct'], cfg['ts_pct'], cfg['be_trigger'])
    cash = INITIAL_CAPITAL_TMN
    peak = cash
    max_dd = 0.0
    trades = []
    entry_indices = np.where(entry_sig[start_idx:end_idx - 1])[0] + start_idx
    ptr = 0
    cur_bar = start_idx
    while ptr < len(entry_indices):
        while ptr < len(entry_indices) and entry_indices[ptr] < cur_bar:
            ptr += 1
        if ptr >= len(entry_indices):
            break
        e_idx = int(entry_indices[ptr])
        if e_idx >= end_idx - 1:
            break
        entry_price = close[e_idx]
        highest_price = entry_price
        sl_price = entry_price * (1.0 - sl_pct)
        tp_price = entry_price * (1.0 + tp_pct)
        exit_price = close[end_idx - 1]
        exit_bar = end_idx - 1
        exit_reason = 'FINAL_LIQUIDATION'
        for j in range(e_idx + 1, end_idx):
            h_j, l_j, c_j = (high[j], low[j], close[j])
            if l_j <= sl_price:
                exit_price = sl_price
                exit_bar = j
                exit_reason = 'STOP_LOSS' if sl_price < entry_price else 'BREAKEVEN_PROFIT_LOCK'
                break
            if h_j > highest_price:
                highest_price = h_j
            if highest_price >= entry_price * (1.0 + be_trig):
                sl_price = max(sl_price, entry_price * (1.0 + 2.0 * fee_rate + 0.002))
                ts_trigger = highest_price * (1.0 - ts_pct)
                if l_j <= ts_trigger:
                    exit_price = max(ts_trigger, sl_price)
                    exit_bar = j
                    exit_reason = 'TRAILING_STOP'
                    break
            if h_j >= tp_price:
                exit_price = tp_price
                exit_bar = j
                exit_reason = 'TAKE_PROFIT'
                break
            if exit_sig[j]:
                exit_price = c_j
                exit_bar = j
                exit_reason = 'SIGNAL_UPPER_BB_OR_RSI'
                break
        net_mult = exit_price / entry_price * (1.0 - fee_rate) * (1.0 - fee_rate)
        invested = cash * alloc_fraction
        pnl_tmn = invested * (net_mult - 1.0)
        cash += pnl_tmn
        if cash > peak:
            peak = cash
        dd = (peak - cash) / peak
        if dd > max_dd:
            max_dd = dd
        trades.append({'entry_utc': utc_str(ts_arr[e_idx]), 'exit_utc': utc_str(ts_arr[exit_bar]), 'entry_price': entry_price, 'exit_price': exit_price, 'invested_tmn': round(invested), 'pnl_tmn': round(pnl_tmn), 'net_pnl_pct': round((net_mult - 1.0) * 100.0, 2), 'reason': exit_reason})
        cur_bar = exit_bar + 1
    wins = sum((1 for t in trades if t['pnl_tmn'] > 0))
    return {'final_cash': cash, 'net_pnl_tmn': cash - INITIAL_CAPITAL_TMN, 'return_pct': (cash - INITIAL_CAPITAL_TMN) / INITIAL_CAPITAL_TMN * 100.0, 'trades': len(trades), 'win_rate_pct': wins / len(trades) * 100.0 if trades else 0.0, 'max_dd_pct': max_dd * 100.0, 'trade_log': trades}

def run_shared_portfolio_backtest(datasets: dict, signals_by_sym: dict, variant: str, start_idx: int, fee_rate: float=0.001, max_open_positions: int=2) -> dict:
    """
    شبیه\u200cسازی چرخش هوشمند ۵,۰۰۰,۰۰۰ تومان بین تمام ۵ ارز مجاز والکس به صورت هم\u200cزمان!
    هر زمان ارزی سیگنال بدهد، سرمایه آزاد وارد آن می\u200cشود تا پول در طول ماه بیکار نماند.
    """
    cash = INITIAL_CAPITAL_TMN
    peak = cash
    max_dd = 0.0
    positions = {}
    trades = []
    ref_sym = list(datasets.keys())[0]
    n_bars = len(datasets[ref_sym])
    priority_syms = [s for s in ['DOGETMN', 'SOLTMN', 'XRPTMN', 'ETHTMN', 'BTCTMN'] if s in datasets]
    for j in range(start_idx, n_bars):
        for sym in list(positions.keys()):
            pos = positions[sym]
            df = datasets[sym]
            c_j = float(df['close'].iloc[j])
            h_j = float(df['high'].iloc[j])
            l_j = float(df['low'].iloc[j])
            ts_j = int(df['timestamp'].iloc[j])
            _, exit_sig, cfg = signals_by_sym[sym][variant]
            exit_price = None
            exit_reason = None
            if l_j <= pos['sl']:
                exit_price = pos['sl']
                exit_reason = 'STOP_LOSS' if pos['sl'] < pos['entry'] else 'BREAKEVEN_PROFIT_LOCK'
            else:
                if h_j > pos['highest']:
                    pos['highest'] = h_j
                if pos['highest'] >= pos['entry'] * (1.0 + cfg['be_trigger']):
                    pos['sl'] = max(pos['sl'], pos['entry'] * (1.0 + 2.0 * fee_rate + 0.002))
                    ts_trig = pos['highest'] * (1.0 - cfg['ts_pct'])
                    if l_j <= ts_trig:
                        exit_price = max(ts_trig, pos['sl'])
                        exit_reason = 'TRAILING_STOP'
                if exit_price is None:
                    if h_j >= pos['tp']:
                        exit_price = pos['tp']
                        exit_reason = 'TAKE_PROFIT'
                    elif exit_sig[j] or j == n_bars - 1:
                        exit_price = c_j
                        exit_reason = 'SIGNAL_UPPER_BB_OR_RSI' if j < n_bars - 1 else 'FINAL_LIQUIDATION'
            if exit_price is not None:
                net_mult = exit_price / pos['entry'] * (1.0 - fee_rate) * (1.0 - fee_rate)
                proceeds = pos['cost'] * net_mult
                pnl = proceeds - pos['cost']
                cash += proceeds
                trades.append({'symbol': sym, 'entry_utc': pos['entry_utc'], 'exit_utc': utc_str(ts_j), 'invested_tmn': round(pos['cost']), 'pnl_tmn': round(pnl), 'net_pnl_pct': round((net_mult - 1.0) * 100.0, 2), 'reason': exit_reason})
                del positions[sym]
        if j < n_bars - 2 and len(positions) < max_open_positions:
            for sym in priority_syms:
                if sym in positions or len(positions) >= max_open_positions:
                    continue
                entry_sig, _, cfg = signals_by_sym[sym][variant]
                if entry_sig[j] and cash >= 600000:
                    df = datasets[sym]
                    ep = float(df['close'].iloc[j])
                    ts_j = int(df['timestamp'].iloc[j])
                    remaining_slots = max_open_positions - len(positions)
                    alloc_budget = cash * (0.92 if remaining_slots == 1 else 0.55)
                    cash -= alloc_budget
                    positions[sym] = {'entry': ep, 'highest': ep, 'cost': alloc_budget, 'sl': ep * (1.0 - cfg['sl_pct']), 'tp': ep * (1.0 + cfg['tp_pct']), 'entry_utc': utc_str(ts_j)}
        mtm_equity = cash
        for sym, pos in positions.items():
            c_j = float(datasets[sym]['close'].iloc[j])
            mtm_equity += pos['cost'] * (c_j / pos['entry']) * (1.0 - fee_rate)
        if mtm_equity > peak:
            peak = mtm_equity
        dd = (peak - mtm_equity) / peak
        if dd > max_dd:
            max_dd = dd
    wins = sum((1 for t in trades if t['pnl_tmn'] > 0))
    return {'final_cash': cash, 'net_pnl_tmn': cash - INITIAL_CAPITAL_TMN, 'return_pct': (cash - INITIAL_CAPITAL_TMN) / INITIAL_CAPITAL_TMN * 100.0, 'trades': len(trades), 'win_rate_pct': wins / len(trades) * 100.0 if trades else 0.0, 'max_dd_pct': max_dd * 100.0, 'trade_log': trades}

def main():
    parser = argparse.ArgumentParser(description='Wallex Pro Multi-Coin & Single-Coin Spot Backtest')
    parser.add_argument('--days', type=int, default=60, help='تعداد روزهای بکتست (پیش\u200cفرض: 60 روز معادل فایل\u200cهای CSV؛ برای ماهانه 30 بگذارید)')
    parser.add_argument('--fee', type=float, default=0.001, help='کارمزد هر سمت معامله (پیش\u200cفرض: 0.001 معادل Maker/Limit)')
    parser.add_argument('--alloc', type=float, default=0.92, help='سهم سرمایه در هر معامله تک\u200cارزی (پیش\u200cفرض: 0.92 یعنی ۹۲٪)')
    parser.add_argument('--refresh', action='store_true', help='دانلود مجدد دیتا از والکس بدون استفاده از کش')
    args = parser.parse_args()
    now_ts = int(time.time()) // 900 * 900
    warmup_seconds = 7 * 86400
    start_ts = now_ts - args.days * 86400
    fetch_start_ts = start_ts - warmup_seconds
    cache_dir = Path('data/wallex_15m_pro_cache')
    datasets = {}
    print('=' * 98)
    print(f'🚀 دریافت کندل\u200cهای ۱۵ دقیقه\u200cای از API والکس ({args.days} روز تست + ۷ روز Warmup)...')
    print('=' * 98)
    for sym in ALLOWED_SYMBOLS:
        try:
            df = fetch_wallex_15m_candles(sym, fetch_start_ts, now_ts, cache_dir, refresh=args.refresh)
            datasets[sym] = df
            test_bars = df[df['timestamp'] >= start_ts]
            bh = (test_bars['close'].iloc[-1] / test_bars['close'].iloc[0] - 1.0) * 100.0 if len(test_bars) > 0 else 0.0
            print(f'  ✅ {sym:<10} | کندل\u200cها: {len(df):<5} | بازدهی Buy & Hold در این دوره: {bh:+.2f}%')
        except Exception as e:
            print(f'  ⚠️ خطا در دریافت {sym}: {e}')
    if not datasets or 'BTCTMN' not in datasets:
        print('❌ دیتای کافی دریافت نشد.')
        return
    btc_df = datasets['BTCTMN']
    start_idx = max(WARMUP_BARS, int(np.searchsorted(btc_df['timestamp'].values, start_ts)))
    signals_by_sym = {}
    for sym, df in datasets.items():
        signals_by_sym[sym] = build_symbol_signals(df, btc_df, sym, use_ai_filter=True)
    for variant in ['fast', 'v2', 'combined']:
        print('\n' + '=' * 98)
        print(f'📊 نتایج استراتژی: {variant.upper()} | دوره: {args.days} روز | کارمزد: {args.fee * 100:.2f}% | تخصیص: {args.alloc * 100:.0f}%')
        print('=' * 98)
        print(f'  {'نماد (Symbol)':<12} | {'موجودی نهایی (TMN)':<18} | {'سود خالص (TMN)':<16} | {'بازدهی (%)':<11} | {'معاملات':<8} | {'نرخ برد':<9} | {'Max DD'}')
        print('-' * 98)
        sum_ret = 0.0
        total_tr = 0
        for sym in ALLOWED_SYMBOLS:
            if sym not in datasets:
                continue
            res = run_single_symbol_backtest(datasets[sym], signals_by_sym[sym][variant], start_idx=start_idx, fee_rate=args.fee, alloc_fraction=args.alloc)
            sum_ret += res['return_pct']
            total_tr += res['trades']
            print(f'  {sym:<12} | {res['final_cash']:>14,.0f} TMN | {res['net_pnl_tmn']:>+12,.0f} TMN | {res['return_pct']:>+9.2f}% | {res['trades']:>8d} | {res['win_rate_pct']:>7.1f}% | {res['max_dd_pct']:>5.2f}%')
        print('-' * 98)
        print(f'  Σ جمع بازدهی تک\u200cارزی ۵ بازار: {sum_ret:+.2f}% | مجموع معاملات: {total_tr}')
        port_res = run_shared_portfolio_backtest(datasets, signals_by_sym, variant=variant, start_idx=start_idx, fee_rate=args.fee, max_open_positions=2)
        print(f'  🏆 کیف پول مشترک چرخشی ۵ میلیونی (Multi-Coin Pool): نهایی = {port_res['final_cash']:,.0f} تومان ({port_res['return_pct']:+.2f}% | سود: {port_res['net_pnl_tmn']:+,.0f} تومان | معاملات: {port_res['trades']} | WinRate: {port_res['win_rate_pct']:.1f}% | MaxDD: {port_res['max_dd_pct']:.2f}%)')
if __name__ == '__main__':
    main()
