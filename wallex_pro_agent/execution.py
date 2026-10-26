import json,time
from decimal import Decimal
from urllib.parse import quote
from config import SYMBOLS,live_guard,schedule,TTL,safe_error
from wallex_client import rounded
D=Decimal

def payload(symbol,side,qty,price,rules):
    q,p=D(str(qty)),D(str(price))
    if symbol not in SYMBOLS or side not in ('BUY','SELL') or not q.is_finite() or not p.is_finite() or min(q,p)<=0:raise ValueError('Bad order')
    if q%rules['qstep'] or p%rules['pstep']:raise ValueError('Precision violation')
    if q<rules['minq'] or q*p<rules['minn'] or (rules['maxq'] is not None and q>rules['maxq']):raise ValueError('Market size violation')
    return {'symbol':symbol,'side':side,'type':'LIMIT','quantity':format(q,'f'),'price':format(p,'f')}

class Broker:
    def __init__(self,state,client,live=False,confirmed=False):
        self.state,self.client,self.live=state,client,live
        if live:live_guard(confirmed)
    def pending(self,symbol=None):
        return [r for r in self.state.intents() if r['status'] in ('OPEN','UNKNOWN','ACCOUNTING_ERROR') and (symbol is None or json.loads(r['payload'])['symbol']==symbol)]
    def accept(self,row,order):
        try:self.state.apply(row['k'],order);return True
        except Exception as e:self.state.error(row['k'],safe_error(e),'ACCOUNTING_ERROR');return False
    def refresh(self,k):
        row=self.state.intent(k)
        if row is None:raise ValueError('Unknown intent')
        if self.live and row['status']!='REJECTED':
            try:self.accept(row,self.client.order(row['client']))
            except Exception as e:self.state.error(k,safe_error(e))
        return self.state.intent(k)
    def submit(self,k,symbol,side,qty,price,rules,context,book=None,fee=None):
        body=payload(symbol,side,qty,price,rules);old=self.state.intent(k)
        if old:
            stored=json.loads(old['payload'])
            if any(stored[x]!=v for x,v in body.items()):raise ValueError('Conflicting intent')
            return self.refresh(k)
        if self.pending(symbol):raise RuntimeError('Pending order for this symbol')
        if self.live and side=='BUY' and time.time()>=schedule()[1]:raise RuntimeError('Buy cutoff reached')
        if not self.live and (book is None or fee is None or time.time()-book['fetched_at']>15):raise ValueError('Shadow requires fresh book')
        row,fresh=self.state.reserve(k,body,context)
        if not fresh:return self.refresh(k)
        if not self.live:
            self.accept(row,self.shadow_fill(row,book,D(str(fee))));return self.state.intent(k)
        try:
            code,data=self.client.write_once('POST','/v1/account/orders',json.loads(row['payload']))
            if code in (200,201) and isinstance(data,dict) and data.get('success') is True:
                self.accept(row,data['result']);return self.state.intent(k)
            if code in (400,401,403,422) and isinstance(data,dict) and data.get('success') is False:
                self.state.error(k,f'HTTP {code}: order rejected','REJECTED');return self.state.intent(k)
        except Exception as e:self.state.error(k,safe_error(e))
        return self.refresh(k)
    def cancel(self,k):
        r=self.refresh(k)
        if r['status']=='OPEN' and self.live:
            try:self.client.write_once('DELETE','/v1/account/orders/'+quote(r['client'],safe=''))
            except Exception:pass
            return self.refresh(k)
        return r
    def poll(self):
        alerts=[]
        for r in self.state.intents():
            if r['status'] not in ('OPEN','UNKNOWN','ACCOUNTING_ERROR'):continue
            try:
                fresh=self.refresh(r['k'])
                if fresh['status']=='OPEN' and time.time()-fresh['created']>=TTL:fresh=self.cancel(r['k'])
                if fresh['status'] in ('UNKNOWN','ACCOUNTING_ERROR'):alerts.append(r['k']+':'+fresh['status'])
            except Exception as e:alerts.append(safe_error(e))
        return alerts
    def shadow_fill(self,row,book,fee):
        p=json.loads(row['payload']);remaining=D(p['quantity']);limit=D(p['price']);fills=[]
        for price,available in book['ask' if p['side']=='BUY' else 'bid']:
            price,available=D(str(price)),D(str(available))
            if (p['side']=='BUY' and price>limit) or (p['side']=='SELL' and price<limit):break
            q=min(remaining,available)
            if q<=0:continue
            n=q*price;fills.append({'symbol':p['symbol'],'quantity':str(q),'price':str(price),'sum':str(n),'fee':str(n*fee),'feeAsset':'TMN','isBuyer':p['side']=='BUY'})
            remaining-=q
            if remaining<=0:break
        q=sum((D(f['quantity']) for f in fills),D('0'));n=sum((D(f['sum']) for f in fills),D('0'))
        return {**{k:p[k] for k in ('symbol','side','type','price')},'clientOrderId':row['client'],'origQty':p['quantity'],
            'executedQty':str(q),'executedSum':str(n),'active':False,'fills':fills,'status':'SHADOW_IOC_APPROXIMATION'}
