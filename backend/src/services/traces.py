import os
import redis
from fastapi import HTTPException
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models.dataset import Dataset
from src.models.plan import Plan
from src.models.trace import Trace
from src.services.spill import SPILL_ROOT
from src.utils.serialization import record
queue=redis.Redis.from_url(REDIS_URL)
def list_items():
 with Session() as s:return [record(r) for r in s.query(Trace).order_by(Trace.id.desc()).limit(100).all()]
def detail(item_id):
 with Session() as s:
  row=s.get(Trace,item_id)
  if not row:raise HTTPException(404,'未找到执行轨迹')
  return record(row)
def create(data):
 with Session() as s:
  p=s.get(Plan,data.plan_id)
  if not p:raise HTTPException(404,'未找到算子计划')
  d=s.get(Dataset,p.dataset_id)
  row=Trace(name=data.name,plan_id=p.id,dataset_id=d.id,status='queued',mode=data.mode,snapshot={'dataset':record(d),'plan':record(p),'mode':data.mode})
  s.add(row);s.commit();s.refresh(row);result=record(row)
 try:queue.rpush('operator-traces',result['id'])
 except redis.RedisError:
  with Session() as s:r=s.get(Trace,result['id']);r.status='failed';r.error='队列不可用，请创建新任务';s.commit()
  raise HTTPException(503,'队列不可用，请稍后重试')
 return result
def spill_artifact_ok(item_id):
 """A completed spill trace is only exportable with manifest + every shard."""
 base=os.path.join(SPILL_ROOT,f'trace-{item_id}')
 manifest=os.path.join(base,'manifest.json')
 if not os.path.exists(manifest):return False
 import json
 try:
  with open(manifest,encoding='utf-8') as f:doc=json.load(f)
  all_files={e['name'] for e in doc['files']}
  output_files=doc['output_files']
 except (OSError,ValueError,KeyError):return False
 if not set(output_files).issubset(all_files):return False
 return all(os.path.exists(os.path.join(base,n)) for n in all_files)
