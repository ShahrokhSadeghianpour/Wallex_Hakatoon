import time,zipfile,json
from pathlib import Path
from config import ROOT,KEY,BOX_KEY

def main():
    output=ROOT/'reports';output.mkdir(exist_ok=True);target=output/('support_'+time.strftime('%Y%m%d_%H%M%S')+'.zip')
    included=[];skipped=[]
    with zipfile.ZipFile(target,'w',compression=zipfile.ZIP_DEFLATED) as z:
        for folder in (ROOT/'reports',ROOT/'logs'):
            if not folder.exists():continue
            for p in folder.rglob('*'):
                if not p.is_file() or p.suffix.lower() not in ('.json','.jsonl','.csv','.log'):continue
                if p.stat().st_size>15000000:skipped.append(str(p.relative_to(ROOT)));continue
                body=p.read_text(encoding='utf-8-sig',errors='replace')
                for secret in (KEY,BOX_KEY):
                    if secret:body=body.replace(secret,'[REDACTED]')
                name=str(p.relative_to(ROOT));z.writestr(name,body);included.append(name)
        z.writestr('manifest.json',json.dumps({'included':included,'skipped':skipped,'env_included':False,'databases_included':False},indent=2))
    print('Support archive:',target);print('Contains private trading results; share privately, not publicly.')
if __name__=='__main__':main()
