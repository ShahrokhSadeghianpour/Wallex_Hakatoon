import json,os,re,time
from pathlib import Path
import numpy as np
import pandas as pd
from config import ROOT,DATA,SYMBOLS
from pro_core import validate
COLS=['timestamp','open','high','low','close','volume']

def atomic_csv(path,frame):
    tmp=path.with_name(path.name+'.tmp');frame.to_csv(tmp,index=False);os.replace(tmp,path)

def aggregate(old,raw,start,end,replace_from):
    parts=[]
    if old is not None:parts.append(old[old.timestamp<replace_from][COLS])
    if raw is not None and not raw.empty:
        raw=raw.copy();raw['bucket']=raw.timestamp.astype('int64')//900*900
        grouped=raw.groupby('bucket',as_index=False).agg(open=('open','first'),high=('high','max'),low=('low','min'),close=('close','last'),volume=('volume','sum')).rename(columns={'bucket':'timestamp'})
        parts.append(grouped[(grouped.timestamp>=replace_from)&(grouped.timestamp<end)])
    if not parts:raise ValueError('No observed history')
    joined=pd.concat(parts,ignore_index=True)
    if joined.timestamp.duplicated().any():raise ValueError('Duplicate buckets')
    out=joined.set_index('timestamp').reindex(np.arange(start,end,900,dtype=np.int64));out.index.name='timestamp'
    missing=int(out.close.isna().sum());out['close']=out.close.ffill()
    if out.close.isna().any():raise ValueError('Leading history missing; future bfill is forbidden')
    for k in ('open','high','low'):out[k]=out[k].fillna(out.close)
    out['volume']=out.volume.fillna(0)
    return validate(out.reset_index(),now=end),missing

class Feed:
    def __init__(self,client,root=None):
        self.client=client;self.root=Path(root) if root else DATA/'pro_feed';self.root.mkdir(parents=True,exist_ok=True)
    def fetch(self,symbol,start,end,resolution='15'):
        parts=[];cursor=start
        while cursor<end:
            stop=min(end,(cursor//86400+1)*86400)
            r=self.client.get('/v1/udf/history',{'symbol':symbol,'resolution':resolution,'from':cursor,'to':stop-1})
            if r.get('s')=='no_data':cursor=stop;continue
            if r.get('s')!='ok':raise ValueError('History API error')
            arrays=[r.get(k) for k in ('t','o','h','l','c','v')]
            if any(not isinstance(a,list) for a in arrays) or len({len(a) for a in arrays})!=1:raise ValueError('History arrays differ')
            f=pd.DataFrame(dict(zip(COLS,arrays)))
            for k in COLS:f[k]=pd.to_numeric(f[k],errors='raise')
            if not f.empty:
                ts=f.timestamp.to_numpy(dtype=float)
                if not np.isfinite(f.to_numpy(dtype=float)).all() or np.any(ts%60) or np.any(np.diff(ts)<=0):raise ValueError('Bad raw timestamps/numbers')
                if np.any(ts<cursor) or np.any(ts>=stop):raise ValueError('Server ignored time bounds')
                if (f[['open','high','low','close']]<=0).any().any() or (f.volume<0).any():raise ValueError('Bad prices/volume')
                if ((f.low>f[['open','close']].min(axis=1))|(f.high<f[['open','close']].max(axis=1))).any():raise ValueError('Bad raw OHLC')
                f['timestamp']=f.timestamp.astype('int64');parts.append(f)
                print(f'[DATA] {symbol} rows={len(f)} from={cursor}',flush=True)
            cursor=stop;time.sleep(.15)
        return pd.concat(parts,ignore_index=True) if parts else pd.DataFrame(columns=COLS)
    def initialize(self,end):
        anchor=self.root/'anchor.json'
        if anchor.exists():return int(json.loads(anchor.read_text())['start'])
        cache=Path(os.getenv('REFERENCE_CACHE_DIR',str(ROOT/'data'/'wallex_15m_pro_cache')) or str(ROOT/'data'/'wallex_15m_pro_cache'))
        records={s:{} for s in SYMBOLS};pattern=re.compile(r'^([A-Z0-9]+)_15m_(\d+)_(\d+)\.csv$')
        if cache.exists():
            for p in cache.glob('*_15m_*.csv'):
                m=pattern.match(p.name)
                if m and m[1] in records:
                    start,stop=int(m[2]),int(m[3])
                    if start%900==0 and stop%900==0 and stop<=end and stop-start>=200*900:records[m[1]][(start,stop)]=p
        common=set(records[SYMBOLS[0]])
        for s in SYMBOLS[1:]:common &= set(records[s])
        if common:
            start,stop=max(common,key=lambda x:(x[1],-x[0]))
            for s in SYMBOLS:
                f=validate(pd.read_csv(records[s][(start,stop)]),now=stop)
                if not np.array_equal(f.timestamp,np.arange(start,stop,900)):raise ValueError('Legacy grid mismatch')
                atomic_csv(self.root/(s+'.csv'),f)
            source='LEGACY_COMMON_CACHE_IMPUTATION_UNKNOWN'
        else:start=end-(int(os.getenv('HISTORY_DAYS','30'))+7)*86400;source='NEW_DOWNLOAD'
        tmp=anchor.with_suffix('.tmp');tmp.write_text(json.dumps({'start':start,'source':source}));os.replace(tmp,anchor)
        return start
    def snapshot(self):
        end=int(time.time())//900*900;start=self.initialize(end);frames={};quality={}
        for s in SYMBOLS:
            path=self.root/(s+'.csv');old=validate(pd.read_csv(path),now=end) if path.exists() else None
            if old is not None and int(old.timestamp.iloc[0])!=start:raise ValueError('Anchor mismatch')
            begin=max(start,int(old.timestamp.iloc[-1])+900-1800) if old is not None else start
            raw=self.fetch(s,begin,end);frames[s],missing=aggregate(old,raw,start,end,begin);atomic_csv(path,frames[s])
            quality[s]={'carried_in_refresh':missing,'last_bucket_observed':bool(not raw.empty and (raw.timestamp//900*900==end-900).any())}
        if end!=int(time.time())//900*900:raise ValueError('Crossed boundary; retry snapshot')
        return frames,quality
    def usdt_trend(self):
        end=int(time.time())//3600*3600;raw=self.fetch('USDTTMN',end-6*3600,end,'60')
        if raw.empty:return {'available':False}
        c=raw.set_index('timestamp').close;points=(end-3600,end-7200,end-5*3600)
        if any(t not in c.index for t in points):return {'available':False}
        return {'available':True,'ret_1h':float(c.loc[points[0]]/c.loc[points[1]]-1),'ret_4h':float(c.loc[points[0]]/c.loc[points[2]]-1)}
