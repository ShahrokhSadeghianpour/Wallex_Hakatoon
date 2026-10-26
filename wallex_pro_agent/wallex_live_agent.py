import argparse,json,logging,logging.handlers,os,queue,signal,threading,time,uuid
from decimal import Decimal
from pathlib import Path
from config import ROOT,DATA,SYMBOLS,POLL,schedule,live_guard,safe_error
from wallex_client import Wallex,rounded
from state import State,encode
from execution import Broker
from market_data import Feed
from pro_core import signals,choose,describe,protection
from ai_agent import Supervisor
D=Decimal;LOG=logging.getLogger('wallex_pro')

class ProcessLock:
    def __enter__(self):
        DATA.mkdir(parents=True,exist_ok=True);self.h=(DATA/'agent.lock').open('a+b');self.h.seek(0,2)
        if self.h.tell()==0:self.h.write(b'0');self.h.flush()
        self.h.seek(0)
        try:
            if os.name=='nt':
                import msvcrt
                msvcrt.locking(self.h.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(self.h.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError:self.h.close();raise RuntimeError('Another agent is running in this state directory')
        return self
    def __exit__(self,*args):
        self.h.seek(0)
        if os.name=='nt':
            import msvcrt
            msvcrt.locking(self.h.fileno(),msvcrt.LK_UNLCK,1)
        else:
            import fcntl
            fcntl.flock(self.h.fileno(),fcntl.LOCK_UN)
        self.h.close()

def scan_cycle(state):
    supervisor=Supervisor(state)
    with Wallex() as api:
        feed=Feed(api);frames,quality=feed.snapshot();end=int(frames['BTCTMN'].timestamp.iloc[-1])+900
        try:trend=feed.usdt_trend()
        except Exception as e:trend={'available':False,'error':safe_error(e)}
        for s in SYMBOLS:
            row=signals(frames[s],frames['BTCTMN'],s).iloc[-1];origin=choose(row)
            snap={'entry_v2':bool(row.entry_v2),'entry_fast':bool(row.entry_fast),'exit_v2':bool(row.exit_v2),'exit_fast':bool(row.exit_fast),
                  'technical_details':describe(frames[s],frames['BTCTMN'],s)}
            message={'symbol':s,'bar':int(row.timestamp),'origin':origin,'signal':snap,'quality':quality[s],'created':time.time(),
                'ai':{'model_ok':False,'action':'SKIP_FAKEOUT'},'reason_fa':'شرط ورود موتور مرجع برقرار نیست؛ مدل فراخوانی نشد.'}
            if origin:
                occupied={p['symbol'] for p in state.positions()}
                for r in state.intents():
                    if r['status'] in ('OPEN','UNKNOWN','ACCOUNTING_ERROR'):occupied.add(json.loads(r['payload'])['symbol'])
                if s in occupied or len(occupied)>=2:message['reason_fa']='نامزد ورود وجود دارد، ولی پوزیشن/سفارش معلق یا سقف ظرفیت مانع ورود است.'
                else:
                    try:
                        context=supervisor.context()
                        if end!=int(time.time())//900*900:raise ValueError('Signal expired across a candle boundary')
                        samples=[api.book(s)];time.sleep(.75);samples.append(api.book(s));usdt=api.book('USDTTMN')
                        market={'symbol':s,'technical_candidate':True,'technical':snap,'asset_books':samples,'usdt_book':usdt,'usdt_trend':trend,
                                'free_slots':2-len(occupied),'source_fraction':.55 if not occupied else .92,'data_quality':quality[s]}
                        ai=supervisor.evaluate(market,context);message.update(ai=ai,market=market,fundamental=context,reason_fa=ai['reason_fa'])
                    except Exception as e:message['reason_fa']=safe_error(e)
            yield message

def scan_worker(state,out,stop,cutoff,once):
    previous=None
    while not stop.is_set() and (once or time.time()<cutoff):
        bar=int(time.time())//900
        if previous==bar:stop.wait(2);continue
        try:
            for m in scan_cycle(state):
                if stop.is_set():return
                out.put(m)
            previous=bar
        except Exception as e:out.put({'symbol':'BTCTMN','bar':None,'origin':None,'signal':None,'ai':{'model_ok':False,'action':'SKIP_FAKEOUT'},'reason_fa':safe_error(e)})
        if once:return
        stop.wait(20)

def balances(state,api,live):
    if live:return api.balances()
    cash=D(state.get('paper_cash'));out={'TMN':{'total':cash,'available':cash,'freeze':D('0')}}
    for p in state.positions():q=D(p['quantity']);out[p['symbol'][:-3]]={'total':q,'available':q,'freeze':D('0')}
    return out

def account_alerts(state,broker,bals,orders,rules):
    known={r['client'] for r in state.intents()};alerts=[];positions={p['symbol']:p for p in state.positions()}
    if any(o.get('clientOrderId') not in known for o in orders):alerts.append('UNKNOWN_EXTERNAL_ORDER')
    for asset,r in bals.items():
        if asset=='TMN' or r['total']==0:continue
        s=asset+'TMN'
        if s not in SYMBOLS:alerts.append('OUTSIDE_UNIVERSE:'+asset);continue
        qty=D(positions[s]['quantity']) if s in positions else D('0')
        if abs(r['total']-qty)>rules[s]['qstep']:alerts.append('UNTRACKED_BALANCE:'+s)
    for s,p in positions.items():
        actual=bals.get(s[:-3],{}).get('total',D('0'))
        if abs(actual-D(p['quantity']))>rules[s]['qstep']:alerts.append('POSITION_BALANCE_MISMATCH:'+s)
    return alerts

def exit_one(state,broker,api,s,rules,fee,bals,latest,force=False):
    p=state.position(s);p=p if p and p['status']=='OPEN' else None
    if p is None and not force:return
    book=api.book(s);reason='LIQUIDATE_ALL_TO_TMN' if force else None
    if p:
        m=latest.get(s);technical=bool(m and m.get('signal') and m['bar']==int(time.time())//900*900-900 and m['signal']['exit_'+p['origin']])
        new,why=protection(p,book['best_bid'],fee,technical)
        if new!=p:state.protection(new)
        p=new;reason=reason or why
    if reason is None:return
    pending=broker.pending(s)
    if pending:
        for r in pending:
            if r['status']=='OPEN':broker.cancel(r['k'])
        return
    available=bals.get(s[:-3],{}).get('available',D('0'));q=min(available,D(p['quantity'])) if p else available
    q=rounded(q,rules[s]['qstep']);price=rounded(book['best_bid']*(1-D(os.getenv('SELL_LIMIT_ALLOWANCE','.0015'))),rules[s]['pstep'])
    if q<rules[s]['minq'] or q*price<rules[s]['minn']:
        LOG.warning('DUST/BELOW MINIMUM %s qty=%s; not declared liquidated',s,q);return
    row=broker.submit('EXIT_'+s+'_'+uuid.uuid4().hex[:16],s,'SELL',q,price,rules[s],
        {'origin':p['origin'] if p else 'v2','unmanaged_liquidation':p is None,'reason':reason,'position':p,
         'book':book,'ai_not_required_for_exit':True},book,fee)
    LOG.info('SELL %s %s reason=%s',s,row['status'],reason)

def entry_one(state,broker,api,m,rules,fee,bals,cutoff,alerts):
    s=m['symbol'];ai=m.get('ai',{});expected=int(time.time())//900*900-900
    if alerts or state.get('risk_halted',False) or time.time()>=cutoff or m.get('bar')!=expected:return
    if not m.get('origin') or not ai.get('model_ok') or ai.get('action')!='EXECUTE_BUY':return
    occupied={p['symbol'] for p in state.positions()}
    for r in broker.pending():occupied.add(json.loads(r['payload'])['symbol'])
    if s in occupied or len(occupied)>=2 or any(r['status'] in ('UNKNOWN','ACCOUNTING_ERROR') for r in broker.pending()):return
    key=f'ENTRY_{s}_{expected}'
    if state.intent(key):return
    fraction=D(str(ai['allocation_fraction']))
    if not D('.5')<=fraction<=D('.92') or time.time()-m.get('created',0)>180:return
    book=api.book(s)
    if book['spread_pct']>D(os.getenv('MAX_ENTRY_SPREAD_PCT','.75')):
        state.event('SKIP_SPREAD',s,'اسپرد فعلی از سقف ورود بیشتر است.',book);return
    price=rounded(book['best_ask']*(1+D(os.getenv('BUY_LIMIT_ALLOWANCE','.001'))),rules[s]['pstep'],True)
    budget=bals['TMN']['available']*fraction;q=rounded(budget/(price*(1+fee)),rules[s]['qstep'])
    if rules[s]['maxq'] is not None:q=min(q,rounded(rules[s]['maxq'],rules[s]['qstep']))
    if q<rules[s]['minq'] or q*price<rules[s]['minn']:return
    if q*price*(1+fee)>bals['TMN']['available']:raise ValueError('Overspending free TMN')
    if time.time()>=cutoff or int(time.time())//900*900-900!=expected:return
    r=broker.submit(key,s,'BUY',q,price,rules[s],{'origin':m['origin'],'decision':m,'allocation_fraction':str(fraction)},book,fee)
    LOG.info('BUY %s %s origin=%s allocation=%s',s,r['status'],m['origin'],fraction)

def run(mode,confirmed=False,once=False):
    live=mode in ('live','liquidate')
    if live:live_guard(confirmed)
    if live and once:raise ValueError('--once is prohibited for live/liquidate')
    state=State('live' if live else 'shadow');end,cutoff,liquidate=schedule()
    out=queue.Queue();stop=threading.Event();worker=None;latest={};candidates=[];last_status=0;last_fees=0
    LOG.info('mode=%s deadlineUTC=%s cutoff=%s liquidation=%s',mode,end,cutoff,liquidate)
    with ProcessLock(),Wallex() as api:
        rules=api.markets()
        if any(s not in rules for s in SYMBOLS):raise ValueError('Missing configured markets')
        broker=Broker(state,api,live,confirmed);ready=False
        try:fees=api.fees(SYMBOLS);ready=True;state.set('fees',{s:str(v) for s,v in fees.items()})
        except Exception as e:
            old=state.get('fees',{});fees={s:D(old.get(s,'.005')) for s in SYMBOLS};LOG.error('Fee unavailable: exits use estimate, new buys blocked: %s',safe_error(e))
        if mode!='liquidate' and (once or time.time()<cutoff):
            worker=threading.Thread(target=scan_worker,args=(state,out,stop,cutoff,once),daemon=True);worker.start()
        try:
            while True:
                try:
                    for alert in broker.poll():LOG.error('ORDER ALERT %s',alert)
                    while True:
                        try:m=out.get_nowait()
                        except queue.Empty:break
                        latest[m['symbol']]=m;state.event('DECISION',m['symbol'],m['reason_fa'],m)
                        LOG.info('SIGNAL %s origin=%s AI=%s',m['symbol'],m.get('origin'),m.get('ai',{}).get('action'))
                        if m.get('origin'):candidates.append(m)
                    force=mode=='liquidate' or time.time()>=liquidate or state.get('risk_halted',False)
                    bals=balances(state,api,live);orders=api.orders() if live else []
                    alerts=account_alerts(state,broker,bals,orders,rules) if live else []
                    if alerts:LOG.error('ACCOUNT ALERT %s; new buys blocked',alerts)
                    # Exit errors are isolated: one failed symbol must not stop others.
                    for p in state.positions():
                        try:exit_one(state,broker,api,p['symbol'],rules,fees[p['symbol']],bals,latest,force)
                        except Exception as e:LOG.error('EXIT ALERT %s %s',p['symbol'],safe_error(e))
                    if force:
                        bals=balances(state,api,live)
                        for asset,r in bals.items():
                            s=asset+'TMN'
                            if asset=='TMN' or r['available']<=0:continue
                            if s not in SYMBOLS:LOG.error('Outside allowed universe: %s',asset);continue
                            if not any(p['symbol']==s for p in state.positions()):
                                try:exit_one(state,broker,api,s,rules,fees[s],bals,latest,True)
                                except Exception as e:LOG.error('LIQUIDATION ALERT %s',safe_error(e))
                    if time.monotonic()-last_fees>120:
                        try:fees=api.fees(SYMBOLS);ready=True;state.set('fees',{s:str(v) for s,v in fees.items()})
                        except Exception as e:ready=False;LOG.warning('Fee refresh %s',safe_error(e))
                        last_fees=time.monotonic()
                    if not force and ready and candidates:
                        m=candidates.pop(0);bals=balances(state,api,live)
                        entry_one(state,broker,api,m,rules,fees[m['symbol']],bals,cutoff,alerts)
                    if force or time.time()>=cutoff:candidates.clear();stop.set()
                    if time.monotonic()-last_status>30:
                        bals=balances(state,api,live);equity=bals['TMN']['total']
                        for asset,r in bals.items():
                            if asset!='TMN' and r['total']>0 and asset+'TMN' in SYMBOLS:
                                b=api.book(asset+'TMN');equity+=r['total']*b['best_bid']*(1-fees[asset+'TMN'])
                        peak=max(D(state.get('peak','5000000')),equity);dd=float((peak-equity)/peak*100) if peak>0 else 0
                        state.set('peak',str(peak));state.set('last_status',{'time':time.time(),'cash':str(bals['TMN']['available']),'equity_estimate':str(equity),'dd_pct':dd,'realized_since_package':str(state.realized())})
                        limit=float(os.getenv('MAX_DRAWDOWN_PCT','0'))
                        if limit>0 and dd>=limit:state.set('risk_halted',True);state.event('RISK_HALT','ACCOUNT','سقف افت سرمایه فعال شد.',{'dd_pct':dd})
                        LOG.info('HEARTBEAT cash=%s equity~=%s positions=%s realized=%s DD=%.3f%% force=%s',bals['TMN']['available'],equity,len(state.positions()),state.realized(),dd,force)
                        state.export(True);last_status=time.monotonic()
                    if force:
                        bals=balances(state,api,live);coins={a:r['total'] for a,r in bals.items() if a!='TMN' and r['total']>0}
                        if not coins and not state.positions() and not broker.pending() and not (api.orders() if live else []):
                            state.event('LIQUIDATION_CONFIRMED','ACCOUNT','فقط تومان باقی مانده و سفارش فعالی وجود ندارد.',{'cash':str(bals['TMN']['available']),'before_deadline':time.time()<end,'unknown_cost_basis':state.get('unknown_cost_basis',False)})
                            LOG.info('LIQUIDATION CONFIRMED');return 0
                    if once and worker and not worker.is_alive() and out.empty() and not candidates:state.export(True);return 0
                except Exception as e:LOG.error('LOOP ERROR %s',safe_error(e))
                time.sleep(POLL)
        finally:stop.set();state.export(True);LOG.warning('Monitoring stopped; this does NOT liquidate positions')

def probe(test_ai=False):
    state=State('shadow')
    with ProcessLock(),Wallex() as api:
        rules=api.markets();fees=api.fees(SYMBOLS);bals=api.balances();orders=api.orders()
        for s in SYMBOLS:print(s,'qstep=',rules[s]['qstep'],'fee=',fees[s],'spread=',api.book(s)['spread_pct'])
        print('TMN:',bals['TMN']['available'],'open_orders:',len(orders))
        if test_ai:
            ai=Supervisor(state);context=ai.context();result=ai.evaluate({'technical_candidate':False,'purpose':'SELF_TEST'},context)
            state.event('AI_PROBE','ACCOUNT','آزمون اتصال بدون سفارش.',result);print(encode(result))
            if not result.get('model_ok') or result['action']!='SKIP_FAKEOUT':raise ValueError('AI probe failed')
    print('PROBE PASS; real orders: 0')

def main():
    p=argparse.ArgumentParser(description='Wallex Pro Agent; LIVE disabled by default')
    p.add_argument('--mode',choices=['shadow','live','liquidate','probe','status'],required=True)
    p.add_argument('--confirm-live',action='store_true');p.add_argument('--once',action='store_true');p.add_argument('--test-ai',action='store_true')
    p.add_argument('--state-mode',choices=['shadow','live'],default='shadow');a=p.parse_args()
    (ROOT/'logs').mkdir(exist_ok=True);LOG.setLevel(logging.INFO)
    for h in (logging.StreamHandler(),logging.handlers.RotatingFileHandler(ROOT/'logs'/'runtime.log',maxBytes=5000000,backupCount=3,encoding='utf-8')):
        h.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'));LOG.addHandler(h)
    def stop(*unused):raise KeyboardInterrupt
    signal.signal(signal.SIGTERM,stop)
    try:
        if a.mode=='probe':probe(a.test_ai);return 0
        if a.mode=='status':
            s=State(a.state_mode);print(encode({'status':s.get('last_status'),'positions':s.positions(),'realized':str(s.realized()),'intents':[{k:r[k] for k in ('k','client','status','error')} for r in s.intents()]}));return 0
        return run(a.mode,a.confirm_live,a.once)
    except KeyboardInterrupt:LOG.warning('Stopped; no automatic liquidation');return 130
    except Exception as e:LOG.error('%s',safe_error(e));return 1

if __name__=='__main__':raise SystemExit(main())
