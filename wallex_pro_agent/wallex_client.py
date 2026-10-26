import time
from decimal import Decimal,ROUND_DOWN,ROUND_UP
from urllib.parse import quote
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config import KEY
D=Decimal

def num(x):
    v=D(str(x))
    if not v.is_finite() or v<0:raise ValueError('Invalid API number')
    return v

def step(x):
    v=num(x)
    if v!=int(v) or v>18:raise ValueError('Unexpected precision')
    return D('1').scaleb(-int(v))

def rounded(x,s,up=False):return (D(str(x))/s).to_integral_value(rounding=ROUND_UP if up else ROUND_DOWN)*s

class Wallex:
    def __init__(self,key=None):
        self.key=KEY if key is None else key;self.session=requests.Session()
        self.session.headers.update({'Accept':'application/json','User-Agent':'WallexProAgent/1'})
        retry=Retry(total=2,backoff_factor=1,allowed_methods=frozenset({'GET'}),status_forcelist=(429,500,502,503,504),respect_retry_after_header=True)
        self.session.mount('https://',HTTPAdapter(max_retries=retry))
    def __enter__(self):return self
    def __exit__(self,*args):self.session.close()
    def get(self,path,params=None,private=False):
        if private and not self.key:raise RuntimeError('WALLEX_API_KEY missing')
        r=self.session.get('https://api.wallex.ir'+path,params=params,
            headers={'X-API-Key':self.key} if private else {},timeout=(4,12),allow_redirects=False)
        if r.status_code!=200:raise RuntimeError(f'{path}: HTTP {r.status_code}')
        data=r.json()
        if not isinstance(data,dict):raise ValueError('Non-object API response')
        if path.endswith('/history'):return data
        if data.get('success') is not True:raise RuntimeError('API success not confirmed')
        return data['result']
    def write_once(self,method,path,payload=None):
        if method not in ('POST','DELETE') or not self.key:raise ValueError('Invalid authenticated write')
        with requests.Session() as s:
            s.mount('https://',HTTPAdapter(max_retries=0))
            r=s.request(method,'https://api.wallex.ir'+path,json=payload if method=='POST' else None,
                headers={'X-API-Key':self.key,'Accept':'application/json'},timeout=(4,12),allow_redirects=False)
        try:data=r.json()
        except ValueError:data=None
        return r.status_code,data
    def markets(self):
        records=self.get('/v1/markets')['symbols'];out={}
        for s,r in records.items():
            if r.get('quoteAsset')=='TMN':out[s]={'qstep':step(r['stepSize']),'pstep':step(r['tickSize']),
                'minq':num(r['minQty']),'minn':num(r['minNotional']),'maxq':num(r['maxQty']) if r.get('maxQty') is not None else None}
        return out
    def fees(self,symbols):
        result=self.get('/v1/account/fee',private=True);out={s:num(result[s]['takerFeeRate']) for s in symbols}
        if any(v>=D('.1') for v in out.values()):raise ValueError('Unexpected fee units')
        return out
    def balances(self):
        r=self.get('/v1/account/balances-detail',private=True)
        out={s:{k:num(v[k]) for k in ('total','freeze','available')} for s,v in r.items()}
        if 'TMN' not in out:raise ValueError('No TMN balance')
        return out
    def orders(self):
        out=self.get('/v1/account/openOrders',private=True)['orders']
        if not isinstance(out,list):raise ValueError('Bad open-order schema')
        return out
    def order(self,cid):return self.get('/v1/account/orders/'+quote(cid,safe=''),private=True)
    def book(self,symbol):
        raw=self.get('/v1/depth',{'symbol':symbol});sides={}
        for name,reverse in (('bid',True),('ask',False)):
            levels={}
            for r in raw[name]:
                p,q=num(r['price']),num(r['quantity'])
                if p<=0:raise ValueError('Bad book price')
                if q>0:levels[p]=levels.get(p,D('0'))+q
            sides[name]=sorted(levels.items(),reverse=reverse)[:5]
            if not sides[name]:raise ValueError('Empty book')
        bid,ask=sides['bid'][0][0],sides['ask'][0][0]
        if bid>=ask:raise ValueError('Crossed book')
        bv=sum((q for p,q in sides['bid']),D('0'));av=sum((q for p,q in sides['ask']),D('0'))
        bn=sum((p*q for p,q in sides['bid']),D('0'));an=sum((p*q for p,q in sides['ask']),D('0'))
        return {'symbol':symbol,'fetched_at':time.time(),'best_bid':bid,'best_ask':ask,
            'spread_pct':(ask-bid)/((ask+bid)/2)*100,'buy_sell_volume_ratio':bv/av,
            'notional_imbalance':(bn-an)/(bn+an),'bid_notional':bn,'ask_notional':an,**sides}
