import json
from fastapi import APIRouter,Depends,HTTPException
from fastapi.responses import Response,StreamingResponse
from src.middlewares.auth import current,writer
from src.schemas.business import TraceInput
from src.services import traces
from src.services.spill import build_export
from src.controllers.business import start_trace
router=APIRouter()
@router.get('/traces')
def listing(u=Depends(current)):return traces.list_items()
@router.post('/traces')
def create(data:TraceInput,u=Depends(writer)):return start_trace(data,u)
@router.get('/traces/{item_id}')
def detail(item_id:int,u=Depends(current)):return traces.detail(item_id)
@router.get('/traces/{item_id}/export')
def export(item_id:int,u=Depends(current)):
 item=traces.detail(item_id)
 if item['status']!='completed':raise HTTPException(409,'仅已完成的执行可以导出')
 if (item.get('mode') or 'memory')=='spill':
  if not traces.spill_artifact_ok(item_id):raise HTTPException(409,'外排结果缺少落盘分片或清单，无法导出')
  return StreamingResponse(build_export(item),media_type='application/json',headers={'Content-Disposition':f'attachment; filename="trace-{item_id}.json"'})
 return Response(json.dumps(item,ensure_ascii=False,indent=2),media_type='application/json',headers={'Content-Disposition':f'attachment; filename="trace-{item_id}.json"'})
