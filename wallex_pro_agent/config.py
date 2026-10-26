import os
from pathlib import Path
from datetime import datetime,timedelta,timezone
ROOT=Path(__file__).resolve().parent

def load_env():
    p=ROOT/'.env'
    if not p.exists():return
    for line in p.read_text(encoding='utf-8-sig').splitlines():
        line=line.strip()
        if not line or line.startswith('#'):continue
        k,sep,v=line.partition('=')
        if not sep or not k.strip().isidentifier():raise ValueError('Invalid .env line')
        v=v.strip()
        if len(v)>=2 and v[0]==v[-1] and v[0] in ('"',"'"):v=v[1:-1]
        os.environ.setdefault(k.strip(),v)
load_env()
SYMBOLS=('DOGETMN','XRPTMN','ETHTMN','SOLTMN','BTCTMN')
KEY=os.getenv('WALLEX_API_KEY','').strip()
BOX_KEY=os.getenv('BOXAPI_KEY','').strip()
BOX_URL=os.getenv('BOXAPI_BASE_URL','https://ai.boxapi.ir/ai/v1').rstrip('/')
MODEL=os.getenv('BOXAPI_MODEL','gpt-5.6-luna')
DATA=Path(os.getenv('STATE_DIR',str(ROOT/'data')))
END=os.getenv('COMPETITION_END_UTC','2026-10-23T20:25:00Z')
POLL=float(os.getenv('POLL_SECONDS','5'))
TTL=float(os.getenv('ORDER_TTL_SECONDS','15'))
MAX_POSITIONS=2

def schedule():
    end=datetime.fromisoformat(END.replace('Z','+00:00'))
    if end.utcoffset() is None:raise ValueError('Deadline needs timezone')
    return end.timestamp(),(end-timedelta(minutes=60)).timestamp(),(end-timedelta(minutes=30)).timestamp()

def live_guard(confirmed):
    if not confirmed or os.getenv('ENABLE_LIVE_TRADING','false').lower()!='true' or os.getenv('ACCOUNT_STATE_ACKNOWLEDGED','false').lower()!='true':
        raise RuntimeError('LIVE is blocked: --confirm-live and both ENABLE_LIVE_TRADING/ACCOUNT_STATE_ACKNOWLEDGED=true are required')
    if not KEY:raise RuntimeError('WALLEX_API_KEY missing')
    if os.getenv('RENDER') and not os.path.ismount(DATA):raise RuntimeError('LIVE on Render ephemeral storage is blocked')

def safe_error(e):
    text=f'{type(e).__name__}: {e}'
    for secret in (KEY,BOX_KEY):
        if secret:text=text.replace(secret,'[REDACTED]')
    return text[:1000]
