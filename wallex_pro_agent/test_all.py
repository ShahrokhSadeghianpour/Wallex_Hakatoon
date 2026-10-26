"""Offline tests: real network is blocked. Test databases are temporary."""
import ast,copy,json,sys,tempfile,time,unittest
from pathlib import Path
from decimal import Decimal
from unittest.mock import patch
import numpy as np
import pandas as pd
import requests
import pro_core as core
from config import SYMBOLS,live_guard
from state import State,normalize
from execution import Broker,payload
from market_data import aggregate
from ai_agent import Supervisor
D=Decimal;ROOT=Path(__file__).resolve().parent
RULE={'qstep':D('.00000001'),'pstep':D('1'),'minq':D('.00000001'),'minn':D('50000'),'maxq':None}

def fixture(n=600):
    rng=np.random.default_rng(20261022);c=100000*np.exp(np.cumsum(rng.normal(0,.012,n)));o=np.r_[c[0],c[:-1]]
    return pd.DataFrame({'timestamp':1700000000//900*900+np.arange(n)*900,'open':o,'high':np.maximum(o,c)*1.004,'low':np.minimum(o,c)*.996,'close':c,'volume':rng.uniform(100,1000,n)})

def order(cid,side='BUY',orig='1',executed='1',price='100000',asset='TMN',active=False):
    q,p=D(executed),D(price);fee=q*D('.003') if asset=='BTC' else q*p*D('.003')
    fills=[] if q==0 else [{'symbol':'BTCTMN','quantity':str(q),'price':str(p),'sum':str(q*p),'fee':str(fee),'feeAsset':asset,'isBuyer':side=='BUY'}]
    return {'clientOrderId':cid,'symbol':'BTCTMN','side':side,'type':'LIMIT','price':price,'origQty':orig,'executedQty':str(q),'executedSum':str(q*p),'active':active,'fills':fills}

class Fake:
    def __init__(self,mode='accepted'):self.mode=mode;self.posts=0;self.deletes=0;self.values={}
    def write_once(self,method,path,body=None):
        if method=='POST':
            self.posts+=1
            if self.mode=='rejected':return 422,{'success':False}
            q=str(D(body['quantity'])*D('.4')) if self.mode=='partial' else '0'
            value=order(body['client_id'],body['side'],body['quantity'],q,body['price'],active=True)
            if self.mode!='timeout_unknown':self.values[body['client_id']]=value
            if self.mode.startswith('timeout'):raise requests.Timeout('synthetic')
            return 201,{'success':True,'result':copy.deepcopy(value)}
        self.deletes+=1;cid=path.rsplit('/',1)[-1]
        if self.mode=='cancel_timeout':raise requests.Timeout('synthetic cancel')
        self.values[cid]['active']=False;return 200,{'success':True,'result':copy.deepcopy(self.values[cid])}
    def order(self,cid):
        if cid not in self.values:raise RuntimeError('synthetic 404')
        return copy.deepcopy(self.values[cid])

class CoreTests(unittest.TestCase):
    def test_reference_parity(self):
        tree=ast.parse((ROOT/'reference'/'original_sanitized.py').read_text(encoding='utf-8'))
        funcs=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('compute_wilder_rsi','build_symbol_signals')]
        ns={'np':np,'pd':pd,'STRATEGIES':copy.deepcopy(core.STRATEGIES)}
        exec(compile(ast.fix_missing_locations(ast.Module(body=funcs,type_ignores=[])),'<pure-reference>','exec'),ns)
        f=fixture()
        for s in ('BTCTMN','ETHTMN','DOGETMN'):
            expected=ns['build_symbol_signals'](f,f,s);actual=core.signals(f,f,s)
            for origin in ('v2','fast'):
                np.testing.assert_array_equal(actual['entry_'+origin].iloc[200:],expected[origin][0][200:])
                np.testing.assert_array_equal(actual['exit_'+origin],expected[origin][1])
    def test_causal_prefix(self):
        f=fixture();pd.testing.assert_frame_equal(core.signals(f,f,'DOGETMN').iloc[:360],core.signals(f.iloc[:360],f.iloc[:360],'DOGETMN'))
    def test_btc_alignment(self):
        f=fixture();b=f.copy();b.timestamp+=900
        with self.assertRaises(ValueError):core.signals(f,b,'DOGETMN')
    def test_owned_combined_exit(self):
        row={'entry_v2':True,'entry_fast':True,'exit_v2':False,'exit_fast':True}
        self.assertEqual(core.choose(row),'v2');self.assertFalse(core.owned_exit(row,'v2'));self.assertTrue(core.owned_exit(row,'fast'))
    def test_source_parameters(self):
        self.assertEqual(core.STRATEGIES['fast']['bb_period'],64);self.assertEqual(core.STRATEGIES['fast']['bb_std'],1.8)
        self.assertEqual(core.STRATEGIES['v2']['rsi_exit'],75)
    def test_arming(self):
        p={'origin':'v2','entry_price':'100000','highest_price':'100000','stop_price':'95500'}
        v,_=core.protection(p,'102300','.003');p['origin']='fast';f,_=core.protection(p,'102300','.003')
        self.assertFalse(v.get('break_even_armed',False));self.assertTrue(f['break_even_armed'])
    def test_protection_no_fake_fill(self):
        p={'origin':'fast','entry_price':'100000','highest_price':'103000','stop_price':'100800'}
        out,reason=core.protection(p,'100000','.003');self.assertIsNotNone(reason);self.assertNotIn('fill_price',out)
    def test_priority(self):self.assertEqual(SYMBOLS,('DOGETMN','XRPTMN','ETHTMN','SOLTMN','BTCTMN'))
    def test_live_off(self):
        with self.assertRaises(RuntimeError):live_guard(False)

class StateTests(unittest.TestCase):
    def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.s=State('shadow',self.tmp.name)
    def tearDown(self):self.tmp.cleanup()
    def reserve(self,k='B',side='BUY',q='1',price='100000',extra=None):return self.s.reserve(k,payload('BTCTMN',side,q,price,RULE),{'origin':'v2',**(extra or {})})[0]
    def buy(self,executed='1',asset='TMN'):
        row=self.reserve();self.s.apply('B',order(row['client'],executed=executed,asset=asset));return row
    def test_quote_fee(self):
        self.buy();p=self.s.position('BTCTMN');self.assertEqual(D(p['cost']),D('100300'));self.assertEqual(D(p['quantity']),D('1'))
    def test_base_fee(self):
        self.buy(asset='BTC');self.assertEqual(D(self.s.position('BTCTMN')['quantity']),D('.997'))
    def test_duplicate(self):
        r=self.buy();cash=self.s.get('paper_cash');self.assertFalse(self.s.apply('B',order(r['client'])));self.assertEqual(cash,self.s.get('paper_cash'))
    def test_restart(self):
        self.buy();self.assertEqual(D(State('shadow',self.tmp.name).position('BTCTMN')['cost']),D('100300'))
    def test_partial(self):
        r=self.buy('.4');self.s.apply('B',order(r['client']));self.assertEqual(D(self.s.position('BTCTMN')['quantity']),D('1'))
    def test_atomic_backward_rejection(self):
        r=self.buy();old=self.s.intent('B')['baseline']
        with self.assertRaises(ValueError):self.s.apply('B',order(r['client'],executed='.4'))
        self.assertEqual(old,self.s.intent('B')['baseline'])
    def test_sell_realized(self):
        self.buy();r=self.reserve('S','SELL',price='110000');self.s.apply('S',order(r['client'],'SELL',price='110000'))
        self.assertFalse(self.s.positions());self.assertEqual(self.s.realized(),D('9370'));self.assertEqual(D(self.s.get('paper_cash')),D('5009370'))
    def test_modes_separate(self):self.buy();self.assertFalse(State('live',self.tmp.name).positions())
    def test_exports(self):
        self.buy();self.s.export(True);self.assertTrue((Path(self.tmp.name)/'logs'/'decision_logs.csv').exists())
        self.assertEqual(len((Path(self.tmp.name)/'logs'/'decision_logs.jsonl').read_text(encoding='utf-8').splitlines()),2)
    def test_third_asset(self):
        with self.assertRaises(ValueError):normalize(order('TEST',asset='USDT'))
    def test_conflicting_key(self):
        self.reserve()
        with self.assertRaises(ValueError):self.reserve(q='2')
    def test_unknown_blocks_new_buy(self):
        self.reserve()
        with self.assertRaises(RuntimeError):self.reserve('B2')
    def test_unmanaged_basis_unknown(self):
        r=self.reserve('U','SELL',extra={'unmanaged_liquidation':True});self.s.apply('U',order(r['client'],'SELL'))
        self.assertTrue(self.s.get('unknown_cost_basis'));self.assertEqual(self.s.realized(),0)

class ExecutionTests(unittest.TestCase):
    def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.s=State('live',self.tmp.name)
    def tearDown(self):self.tmp.cleanup()
    def make(self,mode):
        fake=Fake(mode);b=Broker(self.s,fake,False);b.live=True;return b,fake
    def submit(self,b):return b.submit('B','BTCTMN','BUY','3','100000',RULE,{'origin':'v2'})
    def test_once(self):
        b,f=self.make('accepted');self.submit(b);self.submit(b);self.assertEqual(f.posts,1)
    def test_timeout_found(self):
        b,f=self.make('timeout_found');self.assertEqual(self.submit(b)['status'],'OPEN');self.assertEqual(f.posts,1)
    def test_timeout_404_never_repost(self):
        b,f=self.make('timeout_unknown');self.submit(b);r=self.submit(b);self.assertEqual(r['status'],'UNKNOWN');self.assertEqual(f.posts,1)
    def test_rejected(self):
        b,f=self.make('rejected');self.assertEqual(self.submit(b)['status'],'REJECTED')
    def test_partial_cancel(self):
        b,f=self.make('partial');self.submit(b);self.assertEqual(b.cancel('B')['status'],'TERMINAL');self.assertEqual(D(self.s.position('BTCTMN')['quantity']),D('1.2'))
    def test_cancel_timeout(self):
        b,f=self.make('accepted');self.submit(b);f.mode='cancel_timeout';self.assertEqual(b.cancel('B')['status'],'OPEN')
    def test_precision(self):
        with self.assertRaises(ValueError):payload('BTCTMN','BUY','1.01','100000',dict(RULE,qstep=D('.1')))
    def test_shadow_roundtrip(self):
        state=State('shadow',self.tmp.name);b=Broker(state,Fake(),False)
        book={'fetched_at':time.time(),'ask':[(D('100000'),D('3'))],'bid':[(D('99900'),D('3'))]}
        b.submit('PB','BTCTMN','BUY','3','100000',RULE,{'origin':'v2'},book,D('.003'))
        book={'fetched_at':time.time(),'ask':[(D('110100'),D('3'))],'bid':[(D('110000'),D('3'))]}
        b.submit('PS','BTCTMN','SELL','3','110000',RULE,{'origin':'v2'},book,D('.003'))
        self.assertFalse(state.positions());self.assertEqual(state.realized(),D('28110'))
    def test_stale_shadow_no_intent(self):
        state=State('shadow',self.tmp.name);b=Broker(state,Fake(),False)
        with self.assertRaises(ValueError):b.submit('P','BTCTMN','BUY','3','100000',RULE,{'origin':'v2'},{'fetched_at':time.time()-60},D('.003'))
        self.assertIsNone(state.intent('P'))

class DataAITests(unittest.TestCase):
    def raw(self):return pd.DataFrame({'timestamp':np.arange(3600)*60,'open':100.,'high':102.,'low':99.,'close':101.,'volume':1.})
    def test_partial_bucket(self):
        f,n=aggregate(None,self.raw().drop(index=20),0,216000,0);self.assertEqual(len(f),240);self.assertEqual(f.volume.iloc[1],14)
    def test_no_bfill(self):
        raw=self.raw()
        with self.assertRaises(ValueError):aggregate(None,raw[raw.timestamp>=900],0,216000,0)
    def test_no_overlap_volume(self):
        raw=self.raw();old,_=aggregate(None,raw,0,216000,0);new,_=aggregate(old,raw[raw.timestamp>=214200],0,216000,214200);pd.testing.assert_frame_equal(old,new)
    def test_ai_schema(self):
        with tempfile.TemporaryDirectory() as td:
            ai=Supervisor(State('shadow',td));data={'action':'EXECUTE_BUY','allocation_fraction':.55,'news_tier':'TIER_3_NOISE','reason_fa':'آزمون دلیل فارسی معتبر','risk_flags':[],'evidence_ids':['N1']}
            ai.validate(data,{'news':[{'id':'N1'}]});data['evidence_ids']=['FAKE']
            with self.assertRaises(ValueError):ai.validate(data,{'news':[{'id':'N1'}]})
    def test_ai_budget_shared(self):
        with tempfile.TemporaryDirectory() as td,patch.dict('os.environ',{'AI_BUDGET_USD':'.1','AI_CALL_RESERVE_USD':'.05'}):
            a,b=State('shadow',td),State('live',td);a.ai_reserve();b.ai_reserve()
            with self.assertRaises(RuntimeError):a.ai_reserve()



class IntegrationTests(unittest.TestCase):
    def test_patched_backtest_owned_exit(self):
        import wallex_pro_backtest as bt
        f=fixture(210)
        for c in ('open','high','low','close'):f[c]=100000.
        ev=np.zeros(210,dtype=bool);ev[200]=True;ef=np.zeros(210,dtype=bool)
        xv=np.zeros(210,dtype=bool);xv[204]=True;xf=np.zeros(210,dtype=bool);xf[201]=True
        cfg=dict(core.STRATEGIES['fast']);cfg.update(_owner=np.where(ev,'v2','fast'),_exit_v2=xv,_exit_fast=xf)
        bundles={'BTCTMN':{'v2':(ev,xv,core.STRATEGIES['v2']),'fast':(ef,xf,core.STRATEGIES['fast']),'combined':(ev|ef,xv|xf,cfg)}}
        result=bt.run_shared_portfolio_backtest({'BTCTMN':f},bundles,'combined',200,.003,2)
        self.assertEqual(result['trades'],1)
        self.assertEqual(result['trade_log'][0]['exit_utc'],bt.utc_str(f.timestamp.iloc[204]))
        self.assertEqual(result['trade_log'][0]['origin'],'v2')
        single=bt.run_single_symbol_backtest(f,bundles['BTCTMN']['combined'],200,.003,.92)
        self.assertEqual(single['trade_log'][0]['exit_utc'],bt.utc_str(f.timestamp.iloc[204]))
    def test_shadow_runner_roundtrip(self):
        import wallex_live_agent as agent
        import state as state_module
        class RuntimeAPI:
            def __enter__(self):return self
            def __exit__(self,*args):return None
            def markets(self):return {s:dict(RULE) for s in SYMBOLS}
            def fees(self,symbols):return {s:D('.003') for s in symbols}
            def book(self,s):return {'symbol':s,'fetched_at':time.time(),'best_bid':D('100000'),'best_ask':D('100010'),
                'spread_pct':D('.01'),'ask':[(D('100010'),D('1000'))],'bid':[(D('100000'),D('1000'))]}
        def cycle(state):
            yield {'symbol':'BTCTMN','bar':int(time.time())//900*900-900,'origin':'v2','created':time.time(),
                'signal':{'exit_v2':False,'exit_fast':False},'reason_fa':'آزمون ساختگی مسیر اجرایی؛ نه سیگنال واقعی.',
                'ai':{'model_ok':True,'action':'EXECUTE_BUY','allocation_fraction':.55}}
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);future=(time.time()+86400,time.time()+80000,time.time()+81000)
            with patch.object(agent,'ROOT',root),patch.object(agent,'DATA',root/'data'),patch.object(agent,'POLL',.001),\
                 patch.object(state_module,'ROOT',root),patch.object(state_module,'DATA',root/'data'),\
                 patch.object(agent,'Wallex',RuntimeAPI),patch.object(agent,'scan_cycle',cycle),patch.object(agent,'schedule',lambda:future):
                self.assertEqual(agent.run('shadow',once=True),0)
                self.assertEqual(len(State('shadow',td).positions()),1)
                with patch.object(agent,'schedule',lambda:(time.time()-1,time.time()-3601,time.time()-1801)):
                    self.assertEqual(agent.run('shadow',once=True),0)
                self.assertFalse(State('shadow',td).positions())


def main():
    suite=unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
    with patch('requests.sessions.Session.request',side_effect=AssertionError('REAL NETWORK BLOCKED IN TESTS')):
        result=unittest.TextTestRunner(verbosity=2).run(suite)
    report={'passed':result.wasSuccessful(),'tests_run':result.testsRun,'errors':[(str(t),e) for t,e in result.errors],
            'failures':[(str(t),e) for t,e in result.failures],'real_network_calls':0,'real_orders_sent':0,'ai_calls':0,
            'versions':{'numpy':np.__version__,'pandas':pd.__version__,'requests':requests.__version__}}
    out=ROOT/'reports';out.mkdir(exist_ok=True);(out/'selftest_results.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    print('ALL_TESTS_PASS' if result.wasSuccessful() else 'TESTS_NEED_FIX');return 0 if result.wasSuccessful() else 1
if __name__=='__main__':raise SystemExit(main())
