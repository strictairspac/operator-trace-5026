# 松原 · 分析算子执行剖面仪

松原数据库教学研发组 6 人用真实 Python scan/filter/hash join/group 算子，复查一条分析查询的每一步输出。

## 场景与模块
上游是设备可靠性课程的采样文件、传感器表和分析需求。过去在白板画算子、手动算中间行数，再把截图发给助教；重复键丢失、空结果误判、输入版本变化后无法复现实验是三个痛点。本站保存数据集与有界计划 DSL，worker 真执行并保存不可变快照/逐步摘要，下游讲师和同学只读复查。

数据集录入/编辑、算子计划编排、执行队列、算子剖面/JSON导出四个业务页面；管理员有审计。三个实体 datasets / plans / traces 均贯穿数据库、模型、schema、service、controller、route、前端api/store/page。

## 启动
```sh
cp .env.example .env
docker compose up -d --build
docker compose exec -T backend python -m scripts.seed_demo
```
前端 http://localhost:18244 ；后端 http://localhost:19244/docs ；PostgreSQL localhost:17744，trace / tracepass / operatortrace。结束 `docker compose down -v --remove-orphans` 会删本地数据。

| 角色 | 账号 | 页面与权限 | 动作 |
|---|---|---|---|
| admin | admin@operator.local | 全部含审计 | 复核执行人与完成事件 |
| operator | operator@operator.local | 创建/编辑/执行/导出 | 新建计划并交给worker |
| viewer | viewer@operator.local | 只读和导出 | 观察轨迹，写接口403 |

密码统一 `Trace2026!`。密码PBKDF2，JWT八小时；演示凭据和密钥仅限本地，部署前更换。

## 一天剧本与种子
1. 09:00 operator 登录读取「松原日班温升采样」。
2. 09:05 查看12传感器、240采样，北场/南场/试验间三分区。
3. 09:10 新建 `shift-20261117` 数据集，备注 `daily-readings-20261117.json`。
4. 09:15 保存高于50度→按sensor_id连接→按zone平均的计划。
5. 09:20 提交 `shift-20261117`，观察worker排队到完成。
6. 09:25 逐算子复查输入/输出行数与结果预览。
7. 09:30 导出 `trace-<id>.json`，viewer只读验证。
8. 10:00 admin 查看审计。

seed三账号、三个数据集（240行日班、空数据、重复右键）、三个对应计划。固定公式，无随机数；自然键补缺不覆盖用户修改，连续运行不新增。

## 算法与边界
这是教学内核模型，不是PostgreSQL执行器剖面。PostgreSQL真实保存业务元数据；Python worker真实计算表扫描、数值/相等筛选、构建哈希桶与内连接、Decimal累积分组。右表重复键保留倍数，空集合输出空集合。分组avg/sum保留6位小数，输出按规范JSON排序，SHA-256使用排序key紧凑JSON UTF-8；相同输入和计划结果摘要相同。每步输出输入/输出行数、耗时、明细、前5行与checksum。scan 的 input_rows=0 表示从源读取，detail.scanned_rows为扫描源行数；row_visits是扫描/过滤/哈希构建与探测/聚合各步逻辑行访问总和，不代表物理I/O。

DSL最多6步，首步仅scan readings；filter eq/gte/lte；hash_join仅readings.sensor_id=sensors.sensor_id且最多一次；group仅value的count/sum/avg。禁止eval、任意代码和任意SQL。最多100传感器、2000采样、10000连接输出行（内存模式）；超限失败，不保存部分结果。JSON非法、字段错误、非法算子、孤立引用、重复名有可恢复错误。任务使用Redis简单单消费者，进程崩溃后的running任务不自动重试，保留供人工复查后新建任务。

### 外排执行模式（spill）
执行队列可在内存（memory）与外排（spill）两种模式间选择，DSL、输入快照与计算语义完全一致。外排模式沿用100传感器/2000采样/6步的现有输入上限，仅把连接输出上限放宽到200000行；除不可变输入快照外，任何时刻内存中的中间记录按500行封顶（`src/services/spill.py` 的 BATCH），单个热键与大量分组同样受限：哈希构建侧仍受100传感器上限约束，探测结果直接流式落盘，分组先做有界外部排序（每路仅保留一个堆头，每轮归并只产出一个文件）再单组聚合。

每次执行在共享卷 `SPILL_ROOT`（默认 `/artifacts/spill`）下使用独立临时目录构建，数据分片先写 `*.tmp` 再原子改名，manifest 最后写入，整个目录通过一次 rename 原子发布；manifest 同时记录全部分片与最终输出分片清单。写入中途失败会清理整个临时目录、轨迹保持 failed 且不暴露半份文件；结果与 completed 状态在 worker 的单个数据库事务内切换。完整结果不进数据库 JSON，只保留逐算子行数/摘要/前5行预览与 checksum，导出接口按 manifest 流式拼接全部输出行；内存模式的旧轨迹仍按原整份 JSON 查看与导出。验证见 `backend/tests/spill_verify.py`（stdlib，`python3 -I tests/spill_verify.py`），覆盖热键倍增、大量分组、空输入、200k连接后继续筛选分组、落盘故障清理与两种模式结果一致性。

## 技术、目录与本地开发
Python3.11 / FastAPI / SQLAlchemy / psycopg / Redis / PostgreSQL16；原生 HTML/CSS/JS ES modules / nginx1.27。五服务frontend/backend/worker/db/redis均健康探针，命名卷数据库/Redis/共享产物。DB/Redis健康后启动backend，backend健康后启worker/frontend。

backend/src按models/schemas/services/controllers/routes/middlewares/config/constants/utils分层；backend/scripts/seed_demo.py；backend/migrations元数据建表版本。frontend/src按api/stores/pages/components/common/hooks/router/types/constants/utils分层。三共享组件table/status/feedback，两hooks forms/poll。角色/状态/算子枚举在backend/src/constants/enums.py与frontend/src/constants/enums.js；权限/auth、worker状态、operator校验、前端状态展示使用相同枚举值。请求追踪和全局错误中间件、JWT授权、业务审计贯穿模块。

石墨黄铜工程蓝图视觉，双字体Noto Sans SC/Space Mono来自Google Fonts，离线回退；响应桌面/平板/手机。环境变量见.env.example：COMPOSE_PROJECT_NAME、三端口、JWT_SECRET、SEED_DEMO。DATABASE_URL/REDIS_URL由Compose提供。SEED_DEMO启动失败只记日志。

本地后端：pip install -r backend/requirements.txt，设置DATABASE_URL、REDIS_URL、JWT_SECRET，在backend运行uvicorn src.main:app --reload；前端需代理/api，不支持file://。截图、测试、日志和报告在批次根test-artifacts/gb-4105；仓库内验证代码仅 `backend/tests/spill_verify.py`（标准库，无第三方依赖），运行产物不入库。License MIT。
