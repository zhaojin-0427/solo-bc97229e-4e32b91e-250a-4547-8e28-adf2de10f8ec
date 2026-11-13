# 遥测数据合并服务

把零散采集的 CSV 合成较少的 Parquet，降低下游逐文件取数开销。
Flask + PyArrow + SQLite（目录库），上传即生成 Parquet，后台合并，原子切换目录。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

页面入口即可演示完整流程：上传 CSV → 勾选 ≥2 个文件合并 → 观察进度条 → 下载全库 CSV。

## 本地开发

```bash
pip install -r app/requirements.txt
DATA_DIR=/tmp/data python app/app.py        # 或 gunicorn，见 app/Dockerfile
```

生成演示数据：

```bash
python scripts/gen_sample.py /tmp/sample 4 5000   # 4 个文件，每个 5000 行
curl -F "file=@/tmp/sample/site-batch-1.csv" http://localhost:8000/upload
```

## 功能与保证

| 需求 | 实现 |
| --- | --- |
| 上传 CSV（站点/UTC秒/读数，按文本保留） | 三列均为 `pa.string`，重复行不去重；每次上传生成一个 Parquet |
| 页面列出活动文件及行数 | 首页表格 + 实时合并进度/错误（轮询 `/api/status`） |
| ≥2 个文件后台合并，同时只执行一次 | 进程内锁 + 目录库状态检查，冲突返回提示 |
| 扫描/写文件不占目录库写事务 | 扫描与写出只读 Parquet、写临时文件；目录库仅有秒级短事务（进度、切换） |
| 切换只替换所选文件，不漏新上传 | 切换事务仅 `UPDATE` 所选 id 为 retired 并插入新文件；期间上传是新 id，不受影响 |
| 新文件完整落盘后目录一起切换 | `fsync` → `os.replace` 原子改名 → 单个 SQLite 事务完成新旧替换，不丢行、不重复计入 |
| 下载固定受理时文件集合 | 受理时快照 active 文件并加引用计数；慢下载不阻塞合并 |
| 下载收尾不占请求线程 | 结束/断开只在请求线程投递任务；释放引用与回收在受限后台线程池执行（`RELEASE_WORKERS`），线程数有上限 |
| 目录库写锁被长时间占用 | 收尾任务遇 `database is locked` 退避重试，解锁后自动补完，不丢失、无需重启或再次下载；期间健康检查与状态页（WAL 只读）仍可响应 |
| 旧文件延迟回收 | 退役文件在引用归零（下载结束/断开）后才删除；共享文件的多个下载各自只释放自身引用；后台另有周期回收兜底 |
| 崩溃恢复 | 重启时：未切换的合并标记失败、保留旧数据、清除 tmp/孤儿残件；已切换的保留新数据并接续回收；下载不跨进程存活，引用清零后回收 |
| 内存不随总行数增长 | 上传/合并/下载全链路按批流式（`iter_batches`，默认 2 万行/批） |

## 目录与状态

```
/data
  catalog.db          # SQLite 目录库（WAL）：files / merges / file_refs
  parquet/            # 活动 + 已退役待回收的 Parquet
  tmp/                # 落盘中的临时文件（上传、合并输出）
```

合并状态机：`scanning → writing → committing → switched → done`（或 `failed`）。
`committing` 中的切换是单个事务：插入新文件（active）+ 所选输入置 retired + 合并置
`switched`，要么全部生效要么回滚，崩溃也只落在事务外两侧的可恢复状态。

## 演示崩溃恢复

```yaml
# docker-compose.yml 中取消注释，放慢合并便于观察
MERGE_BATCH_DELAY_MS: "100"
```

1. 上传几个大 CSV，发起合并；
2. 进度进行中 `docker compose kill web`；
3. `docker compose up`：合并标记「失败（进程中断）」，旧文件仍在、残件已清，行数不变。

## 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `/data` | 数据目录（挂卷以持久化） |
| `BATCH_ROWS` | `20000` | 流式批大小（内存上界） |
| `DB_BUSY_TIMEOUT_MS` | `30000` | 目录库写事务单次忙等上限；超时后由后台收尾任务退避重试兜底 |
| `RELEASE_WORKERS` | `4` | 下载收尾后台线程数上限（等待写锁不占服务请求线程） |
| `MERGE_BATCH_DELAY_MS` | `0` | 演示钩子：每批合并延时 |
| `DOWNLOAD_BATCH_DELAY_MS` | `0` | 演示钩子：每批下载延时（模拟慢下载） |
| `SECRET_KEY` | 内置 dev 值 | Flask session 密钥 |

## 回归测试

```bash
pip install -r app/requirements.txt -r app/requirements-dev.txt
python -m pytest tests/ -v
```

覆盖原始故障场景：另一 SQLite 连接长时间持有目录库写锁时断开下载——
收尾任务转入受限后台线程池重试（请求线程不被占满，健康检查/状态页仍可响应），
解锁后自动补完释放并回收无读者的旧文件；并发断连各自只释放自身引用；
存活慢下载完整读完固定快照（文件不提前删）；重启回收保持不变。

## API 摘要

- `POST /upload`（multipart `file`）— CSV → Parquet 入库
- `GET /download` — 受理时刻快照的全库 CSV
- `POST /merge`（表单 `file_ids`，≥2 个）— 启动后台合并
- `GET /api/status` — 文件数/总行数/最近合并进度与错误/下载收尾队列（`release`：排队、执行中、线程上限）
- `GET /healthz` — 健康检查
