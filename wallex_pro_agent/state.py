import json,os,csv,io,sqlite3,threading,time,uuid
from pathlib import Path
from contextlib import contextmanager,closing
from datetime import datetime,timezone
from decimal import Decimal
from config import ROOT,DATA,SYMBOLS
from pro_core import STRATEGIES
D=Decimal;ZERO=D('0')

def encode(v):
    def fallback(x):
        if isinstance(x,D):return str(x)
        raise TypeError(type(x).__name__)
    return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':'),default=fallback,allow_nan=False)

def val(v):
    n=D(str(v))
    if not n.is_finite() or n<0:raise ValueError('Invalid accounting number')
    return n

def normalize(order):
    s,side=order['symbol'],order['side']
    if s not in SYMBOLS or side not in ('BUY','SELL') or not isinstance(order.get('active'),bool):raise ValueError('Bad order identity/active')
    oq,eq=val(order['origQty']),val(order['executedQty'])
    if oq<=0 or eq>oq:raise ValueError('Bad executed quantity')
    fills=order.get('fills',[])
    if not isinstance(fills,list) or (eq>0 and not fills):raise ValueError('Missing fills')
    q=n=bf=qf=ZERO
    for f in fills:
        if f.get('symbol')!=s:raise ValueError('Fill symbol mismatch')
        quantity,price,total,fee=(val(f[k]) for k in ('quantity','price','sum','fee'))
        if quantity<=0 or price<=0 or abs(quantity*price-total)>1:raise ValueError('Invalid fill totals')
        if 'isBuyer' in f and (not isinstance(f['isBuyer'],bool) or f['isBuyer']!=(side=='BUY')):raise ValueError('Fill side mismatch')
        asset=f.get('feeAsset');asset=asset.get('symbol') if isinstance(asset,dict) else asset
        if fee>0:
            if asset==s[:-3]:bf+=fee
            elif asset=='TMN':qf+=fee
            else:raise ValueError('Third-asset fee needs manual valuation')
        q+=quantity;n+=total
    if abs(q-eq)>D('1e-18'):raise ValueError('Fill quantities differ from executedQty')
    if 'executedSum' in order and abs(val(order['executedSum'])-n)>1:raise ValueError('executedSum differs')
    if bf>q or qf>n:raise ValueError('Invalid fees')
    return {'client':order['clientOrderId'],'symbol':s,'side':side,'original':str(oq),'q':str(q),'n':str(n),'bf':str(bf),'qf':str(qf),'active':order['active']}

class State:
    def __init__(self,mode,root=None):
        if mode not in ('live','shadow'):raise ValueError('Bad mode')
        self.mode=mode;self.root=Path(root) if root else ROOT
        directory=self.root/'data' if root else DATA;directory.mkdir(parents=True,exist_ok=True)
        self.path=directory/f'pro_{mode}.sqlite3';self.ai_path=directory/'ai_budget.sqlite3'
        self.lock=threading.RLock();self.last_export=0
        with closing(self.connect()) as c:
            c.execute('PRAGMA journal_mode=WAL')
            c.executescript("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY,v TEXT NOT NULL);"
                "CREATE TABLE IF NOT EXISTS positions(symbol TEXT PRIMARY KEY,payload TEXT NOT NULL);"
                "CREATE TABLE IF NOT EXISTS intents(k TEXT PRIMARY KEY,client TEXT UNIQUE,payload TEXT,context TEXT,status TEXT,baseline TEXT,order_json TEXT,error TEXT,created REAL);"
                "CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT,recorded TEXT,payload TEXT);")
            c.execute('INSERT OR IGNORE INTO meta VALUES (?,?)',('paper_cash',encode('5000000')))
    def connect(self):
        c=sqlite3.connect(str(self.path),timeout=20,isolation_level=None);c.row_factory=sqlite3.Row
        c.execute('PRAGMA synchronous=FULL');c.execute('PRAGMA busy_timeout=20000');return c
    @contextmanager
    def tx(self):
        c=self.connect()
        try:c.execute('BEGIN IMMEDIATE');yield c;c.commit()
        except Exception:c.rollback();raise
        finally:c.close()
    def _get(self,c,k,default=None):
        row=c.execute('SELECT v FROM meta WHERE k=?',(k,)).fetchone();return json.loads(row['v']) if row else default
    def _set(self,c,k,v):c.execute('INSERT INTO meta VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v',(k,encode(v)))
    def get(self,k,default=None):
        with closing(self.connect()) as c:return self._get(c,k,default)
    def set(self,k,v):
        with self.tx() as c:self._set(c,k,v)
    def _event(self,c,kind,symbol,reason,details=None):
        p={'mode':self.mode,'kind':kind,'symbol':symbol,'reason_fa':reason,'details':details or {}}
        c.execute('INSERT INTO events(recorded,payload) VALUES (?,?)',(datetime.now(timezone.utc).isoformat(),encode(p)))
    def event(self,kind,symbol,reason,details=None):
        with self.tx() as c:self._event(c,kind,symbol,reason,details)
        self.export()
    def position(self,s):
        with closing(self.connect()) as c:r=c.execute('SELECT payload FROM positions WHERE symbol=?',(s,)).fetchone()
        return json.loads(r['payload']) if r else None
    def positions(self):
        with closing(self.connect()) as c:rows=c.execute('SELECT payload FROM positions ORDER BY symbol').fetchall()
        return [p for p in (json.loads(r['payload']) for r in rows) if p['status']=='OPEN']
    def _save(self,c,p):c.execute('INSERT INTO positions VALUES (?,?) ON CONFLICT(symbol) DO UPDATE SET payload=excluded.payload',(p['symbol'],encode(p)))
    def protection(self,p):
        with self.tx() as c:
            r=c.execute('SELECT payload FROM positions WHERE symbol=?',(p['symbol'],)).fetchone()
            if not r:raise ValueError('Missing position')
            old=json.loads(r['payload'])
            if any(old[k]!=p[k] for k in ('quantity','cost','origin','entry_client')):raise ValueError('Protection changed accounting')
            if D(p['highest_price'])<D(old['highest_price']):raise ValueError('Highest moved backwards')
            if old!=p:self._save(c,p);self._event(c,'PROTECTION',p['symbol'],'حد خروج و سقف مشاهده‌شده به‌روز شد.',p)
        self.export()
    def intents(self):
        with closing(self.connect()) as c:return [dict(r) for r in c.execute('SELECT * FROM intents ORDER BY created,k')]
    def intent(self,k):
        with closing(self.connect()) as c:r=c.execute('SELECT * FROM intents WHERE k=?',(k,)).fetchone()
        return dict(r) if r else None
    def reserve(self,k,payload,context):
        with self.tx() as c:
            row=c.execute('SELECT * FROM intents WHERE k=?',(k,)).fetchone()
            if row:
                old=json.loads(row['payload'])
                if any(old.get(x)!=v for x,v in payload.items()):raise ValueError('Conflicting intent content')
                return dict(row),False
            for r in c.execute("SELECT payload FROM intents WHERE status IN ('UNKNOWN','ACCOUNTING_ERROR')"):
                other=json.loads(r['payload'])
                if payload['side']=='BUY' or other['symbol']==payload['symbol']:raise RuntimeError('Unresolved order blocks submission')
            client='WA_'+uuid.uuid4().hex[:24].upper();body=dict(payload,client_id=client)
            c.execute('INSERT INTO intents VALUES (?,?,?,?,?,?,?,?,?)',(k,client,encode(body),encode(context),'UNKNOWN',None,None,None,time.time()))
            self._event(c,'ORDER_INTENT',payload['symbol'],'درخواست پیش از ارسال ثبت شد.',{'payload':body,'context':context})
            row=dict(c.execute('SELECT * FROM intents WHERE k=?',(k,)).fetchone())
        self.export();return row,True
    def error(self,k,error,status=None):
        with self.tx() as c:
            if status:c.execute('UPDATE intents SET error=?,status=? WHERE k=?',(str(error)[:1000],status,k))
            else:c.execute('UPDATE intents SET error=? WHERE k=?',(str(error)[:1000],k))
    def apply(self,k,order):
        snap=normalize(order)
        with self.tx() as c:
            r=c.execute('SELECT * FROM intents WHERE k=?',(k,)).fetchone()
            if not r:raise ValueError('No intent')
            payload=json.loads(r['payload']);context=json.loads(r['context'])
            if snap['client']!=r['client'] or any(order[x]!=payload[x] for x in ('symbol','side','type')) or D(order['price'])!=D(payload['price']) or D(snap['original'])!=D(payload['quantity']):raise ValueError('Order differs from intent')
            previous=json.loads(r['baseline']) if r['baseline'] else None
            if previous==snap:
                c.execute('UPDATE intents SET status=?,order_json=?,error=NULL WHERE k=?',('OPEN' if snap['active'] else 'TERMINAL',encode(order),k));return False
            if previous:
                if any(previous[x]!=snap[x] for x in ('client','symbol','side','original')):raise ValueError('Snapshot identity changed')
                if not previous['active'] and snap['active']:raise ValueError('Stale active snapshot')
            delta={x:val(snap[x])-(val(previous[x]) if previous else ZERO) for x in ('q','n','bf','qf')}
            if any(v<0 for v in delta.values()):raise ValueError('Cumulative execution moved backwards')
            changed=any(delta.values());realized=ZERO;unknown=False
            if changed:
                base=delta['q']-delta['bf'] if snap['side']=='BUY' else -(delta['q']+delta['bf'])
                tmn=-(delta['n']+delta['qf']) if snap['side']=='BUY' else delta['n']-delta['qf']
                row=c.execute('SELECT payload FROM positions WHERE symbol=?',(snap['symbol'],)).fetchone();p=json.loads(row['payload']) if row else None
                if snap['side']=='BUY':
                    origin=context['origin']
                    if origin not in STRATEGIES:raise ValueError('Unknown origin')
                    if p and p['status']=='OPEN':
                        if p['entry_client']!=snap['client']:raise ValueError('No pyramiding')
                    else:
                        if p and p.get('entry_client')==snap['client']:raise ValueError('Old order cannot reopen closed position')
                        p={'symbol':snap['symbol'],'origin':origin,'status':'OPEN','quantity':'0','cost':'0','grossq':'0','grossn':'0','highest_price':'0','entry_client':snap['client'],'realized':'0'}
                    qty=D(p['quantity'])+base;cost=D(p['cost'])-tmn;gq=D(p['grossq'])+delta['q'];gn=D(p['grossn'])+delta['n']
                    if min(qty,cost,gq)<=0:raise ValueError('Invalid net buy')
                    entry=gn/gq;p.update(quantity=str(qty),cost=str(cost),grossq=str(gq),grossn=str(gn),entry_price=str(entry),highest_price=str(max(entry,D(p['highest_price']))))
                    p.setdefault('stop_price',str(entry*(1-D(str(STRATEGIES[origin]['sl_pct'])))));self._save(c,p)
                elif p and p['status']=='OPEN':
                    qty=D(p['quantity']);cost=D(p['cost']);consumed=-base
                    if consumed<0 or consumed>qty:raise ValueError('Sell exceeds owned quantity')
                    released=cost*consumed/qty;realized=tmn-released
                    p.update(quantity=str(qty-consumed),cost=str(cost-released),realized=str(D(p['realized'])+realized))
                    if qty==consumed:p.update(status='CLOSED',quantity='0',cost='0')
                    self._save(c,p)
                elif context.get('unmanaged_liquidation'):
                    unknown=True;self._set(c,'unknown_cost_basis',True)
                else:raise ValueError('Sell has no matching position')
                if self.mode=='shadow':
                    cash=D(self._get(c,'paper_cash','5000000'))+tmn
                    if cash<0:raise ValueError('Shadow cash overspent')
                    self._set(c,'paper_cash',str(cash))
            c.execute('UPDATE intents SET status=?,baseline=?,order_json=?,error=NULL WHERE k=?',('OPEN' if snap['active'] else 'TERMINAL',encode(snap),encode(order),k))
            self._event(c,'FILL' if changed else 'ORDER_STATUS',snap['symbol'],'گزارش اجرا، پوزیشن و خط مبنا اتمیک ثبت شد.',{'snapshot':snap,'delta':delta,'realized_pnl':None if unknown else str(realized),'context':context})
        self.export();return changed
    def ai_reserve(self):
        with closing(sqlite3.connect(str(self.ai_path),timeout=20,isolation_level=None)) as c:
            c.execute('CREATE TABLE IF NOT EXISTS calls(id TEXT PRIMARY KEY,cost TEXT,estimated INTEGER)');c.execute('BEGIN IMMEDIATE')
            rows=c.execute('SELECT cost FROM calls').fetchall();reserve=D(os.getenv('AI_CALL_RESERVE_USD','.05'))
            if len(rows)>=int(os.getenv('MAX_AI_CALLS','100')) or sum((D(r[0]) for r in rows),ZERO)+reserve>D(os.getenv('AI_BUDGET_USD','2.50')):raise RuntimeError('Shared AI budget exceeded')
            token=uuid.uuid4().hex;c.execute('INSERT INTO calls VALUES (?,?,1)',(token,str(reserve)));c.commit();return token
    def ai_settle(self,token,cost):
        if cost is None:return
        cost=val(cost)
        with closing(sqlite3.connect(str(self.ai_path),timeout=20,isolation_level=None)) as c:c.execute('UPDATE calls SET cost=?,estimated=0 WHERE id=?',(str(cost),token))
    def realized(self):
        total=ZERO
        with closing(self.connect()) as c:
            for r in c.execute('SELECT payload FROM events'):
                v=json.loads(r['payload'])['details'].get('realized_pnl')
                if v is not None:total+=D(v)
        return total
    def export(self,force=False):
        with self.lock:
            if not force and time.monotonic()-self.last_export<5:return
            self.last_export=time.monotonic()
            with closing(self.connect()) as c:rows=c.execute('SELECT * FROM events ORDER BY seq').fetchall()
            records=[{'sequence':r['seq'],'time':r['recorded'],**json.loads(r['payload'])} for r in rows]
            jsonl=''.join(encode(r)+'\n' for r in records);buf=io.StringIO(newline='')
            w=csv.DictWriter(buf,fieldnames=['sequence','time','mode','kind','symbol','reason_fa','details_json']);w.writeheader()
            for r in records:w.writerow({**{k:r[k] for k in ('sequence','time','mode','kind','symbol','reason_fa')},'details_json':encode(r)})
            for directory in (self.root/'logs',self.root/'logs'/self.mode):
                directory.mkdir(parents=True,exist_ok=True)
                for name,body in (('decision_logs.jsonl',jsonl),('decision_logs.csv','\ufeff'+buf.getvalue())):
                    path=directory/name;tmp=directory/(name+'.tmp');tmp.write_text(body,encoding='utf-8');os.replace(tmp,path)
