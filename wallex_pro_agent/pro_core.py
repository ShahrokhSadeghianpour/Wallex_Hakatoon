import numpy as np
import pandas as pd
from decimal import Decimal
from config import SYMBOLS
STRATEGIES={'v2': {'name': 'MTF_BB_Sniper_v2 (BB192, 2.0)', 'bb_period': 192, 'bb_std': 2.0, 'rsi_period': 28, 'rsi_dip_alt': 30.0, 'rsi_dip_major': 31.5, 'rsi_exit': 75.0, 'sl_pct': 0.045, 'tp_pct': 0.12, 'ts_pct': 0.05, 'be_trigger': 0.024}, 'fast': {'name': 'MTF_BB_Sniper_Fast (BB64, 1.8)', 'bb_period': 64, 'bb_std': 1.8, 'rsi_period': 28, 'rsi_dip_alt': 30.0, 'rsi_dip_major': 31.5, 'rsi_exit': 75.0, 'sl_pct': 0.045, 'tp_pct': 0.12, 'ts_pct': 0.05, 'be_trigger': 0.022}}
SOURCE_SHA256='35b64ea811346dabaf2a70e2528597aaead9bc7c228276c194973c769abb3843'

def compute_wilder_rsi(close_series: pd.Series, period: int) -> np.ndarray:
    delta = close_series.astype(float).diff()
    gain = delta.clip(lower=0.0).ewm(alpha=1.0 / period, adjust=False).mean()
    loss = (-delta.clip(upper=0.0)).ewm(alpha=1.0 / period, adjust=False).mean()
    rs = gain / (loss + 1e-12)
    return (100.0 - 100.0 / (1.0 + rs)).values

def _source_signals(df: pd.DataFrame, btc_df: pd.DataFrame, symbol: str, use_ai_filter: bool=True) -> dict:
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


def validate(df,now=None):
    names=['timestamp','open','high','low','close','volume']
    out=df[names].copy().reset_index(drop=True)
    for c in names:out[c]=pd.to_numeric(out[c],errors='raise')
    if len(out)<200 or not np.isfinite(out.to_numpy(dtype=float)).all():raise ValueError('Need 200 finite bars')
    t=out.timestamp.to_numpy(dtype=float)
    if np.any(t%900) or np.any(np.diff(t)!=900):raise ValueError('Not contiguous 15m UTC data')
    if (out[['open','high','low','close']]<=0).any().any() or (out.volume<0).any():raise ValueError('Invalid price/volume')
    if ((out.low>out[['open','close']].min(axis=1))|(out.high<out[['open','close']].max(axis=1))).any():raise ValueError('Bad OHLC')
    if now is not None and np.any(t+900>now):raise ValueError('Unclosed bar')
    out['timestamp']=out.timestamp.astype('int64');return out

def signals(df,btc,symbol,meta=True):
    if symbol not in SYMBOLS:raise ValueError('Outside universe')
    df,btc=validate(df),validate(btc)
    if not np.array_equal(df.timestamp,btc.timestamp):raise ValueError('BTC timestamps differ')
    ref=_source_signals(df,btc,symbol,use_ai_filter=meta)
    out=pd.DataFrame({'timestamp':df.timestamp})
    for origin in ('v2','fast'):
        out['entry_'+origin]=ref[origin][0];out['exit_'+origin]=ref[origin][1]
    out.loc[:199,['entry_v2','entry_fast']]=False
    return out

def choose(row,variant='combined'):
    if variant not in ('v2','fast','combined'):raise ValueError('Bad variant')
    for origin in (('v2','fast') if variant=='combined' else (variant,)):
        if bool(row['entry_'+origin]):return origin
    return None

def owned_exit(row,origin):
    if origin not in STRATEGIES:raise ValueError('Position origin must be v2 or fast')
    return bool(row['exit_'+origin])

def describe(df,btc,symbol):
    c,o,h,l,v=(df[k].astype(float) for k in ('close','open','high','low','volume'))
    span=h-l+1e-12;uw=(h-c)/span;lw=(o-l)/span;loc=(c-l)/span
    z=(v-v.rolling(96).mean())/(v.rolling(96).std()+1e-12)
    rsi=compute_wilder_rsi(c,28);br=compute_wilder_rsi(btc.close,28)
    ret=c.pct_change(48,fill_method=None)*100
    def f(x):return float(x) if np.isfinite(x) else None
    detail={'rsi28':f(rsi[-1]),'rsi14':f(compute_wilder_rsi(c,14)[-1]),'btc_rsi28':f(br[-1]),
        'upper_wick':f(uw.iloc[-1]),'lower_wick':f(lw.iloc[-1]),'close_location':f(loc.iloc[-1]),
        'vol_z':f(z.iloc[-1]),'ret_12h_pct':f(ret.iloc[-1]),
        'cooldown_48':bool(pd.Series((br<26.5)&(uw.to_numpy()>.46)).rolling(48,min_periods=1).max().iloc[-1]),
        'veto_bleed':bool(z.iloc[-1]<.3 and lw.iloc[-1]<.1 and ret.iloc[-1]<-3),
        'veto_weak':bool(uw.iloc[-1]>.48 or loc.iloc[-1]<.38),'bullish':bool(c.iloc[-1]>o.iloc[-1])}
    for origin,cfg in STRATEGIES.items():
        m=c.rolling(cfg['bb_period']).mean();s=c.rolling(cfg['bb_period']).std()
        detail[origin]={'upper_bb':f((m+cfg['bb_std']*s).iloc[-1]),'lower_bb':f((m-cfg['bb_std']*s).iloc[-1]),
            'rsi_dip':cfg['rsi_dip_major'] if symbol in ('BTCTMN','ETHTMN') else cfg['rsi_dip_alt']}
    return detail

def protection(p,bid,fee,technical=False):
    D=Decimal;out=dict(p);cfg=STRATEGIES[p['origin']]
    price,rate=D(str(bid)),D(str(fee));entry=D(p['entry_price'])
    if not price.is_finite() or price<=0 or not rate.is_finite() or not 0<=rate<D('.1'):raise ValueError('Bad bid/fee')
    stop=max(D(p.get('stop_price',entry*(1-D(str(cfg['sl_pct']))))),entry*(1-D(str(cfg['sl_pct']))))
    high=D(p['highest_price']);reason=None
    if price<=stop:reason=p.get('stop_reason','STOP_LOSS')
    else:
        high=max(high,price)
        if high>=entry*(1+D(str(cfg['be_trigger']))):
            out['break_even_armed']=True
            floor=entry*(1+2*rate+D('.002'));trail=high*(1-D(str(cfg['ts_pct'])))
            if floor>stop:stop=floor;out['stop_reason']='BREAKEVEN_PROFIT_LOCK'
            if trail>stop:stop=trail;out['stop_reason']='TRAILING_STOP'
        out['highest_price']=str(high);out['stop_price']=str(stop)
        if price<=stop:reason=out.get('stop_reason','STOP_LOSS')
        elif price>=entry*(1+D(str(cfg['tp_pct']))):reason='TAKE_PROFIT'
        elif technical:reason='SIGNAL_UPPER_BB_OR_RSI'
    return out,reason
