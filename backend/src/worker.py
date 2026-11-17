import time,logging,glob,os
import redis
from pathlib import Path
from src.config.settings import REDIS_URL,SPILL_ROOT
from src.config.database import Session,ensure_schema
from src.models.trace import Trace
from src.services.operators import execute
from src.services.spill import execute_spill
from src.services.audit import log
q=redis.Redis.from_url(REDIS_URL)
def sweep_stale_spill():
 # directories left by a crashed worker can never carry a published result;
 # remove the hidden temporary build dirs so retries start clean
 for path in glob.glob(os.path.join(SPILL_ROOT,'.trace-*')):
  try:
   if os.path.isdir(path):
    import shutil;shutil.rmtree(path,ignore_errors=True)
  except OSError:logging.exception('stale spill sweep failed')
ensure_schema();sweep_stale_spill()
while True:
 Path('/tmp/worker-heartbeat').touch()
 try:
  item=q.blpop('operator-traces',timeout=2)
  if not item:continue
  run_id=int(item[1])
  with Session() as s:
   row=s.get(Trace,run_id)
   if not row or row.status!='queued':continue
   row.status='running';snapshot=row.snapshot;mode=row.mode or 'memory';s.commit()
  try:
   if mode=='spill':result=execute_spill(snapshot['dataset'],snapshot['plan']['steps'],run_id)
   else:result=execute(snapshot['dataset'],snapshot['plan']['steps'])
   # result and completed state switch in one transaction: readers never see a
   # completed trace without its payload (or a spill payload without manifest)
   with Session() as s:
    r=s.get(Trace,run_id);r.result=result;r.status='completed';s.commit()
   log('worker','trace.completed',str(run_id))
  except Exception as exc:
   logging.exception('trace failed')
   with Session() as s:
    r=s.get(Trace,run_id);r.status='failed';r.error=str(exc)[:1000];s.commit()
 except Exception:logging.exception('worker loop');time.sleep(2)
