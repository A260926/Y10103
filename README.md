# 补偿流程演练场（Compensation Playground）

用本地确定性 adapter 在浏览器/JSON API 中复现多步骤业务“部分成功后的恢复与回退”。
不连接任何外部服务：每一步的 execute / confirm / compensate 都由发布时冻结的脚本驱动，
时钟和 adapter 均可注入（测试使用假时钟）。

## 构建与演示

```bash
./gradlew classes                                   # 编译安装检查
./gradlew test                                      # 14 个 JUnit5 用例
./gradlew run --args='--port 5233'                  # 启动演示（自动建库 + 预置流程）
# 可选：--db <sqlite 文件>   --no-worker（关闭后台自动 worker/自动对账）  --no-seed
```

打开 http://localhost:5233 ，页面标题为“补偿流程演练场”。
后台 worker 会自动推进，并把 UNKNOWN 结果通过 confirm（模拟对账）判定；
预置的 `trip-compensation` 会停在 `FAILED_COMPENSATION`，点击“继续补偿（游标续跑）”可走完回退。

## 持久化协议（每步严格按顺序落库）

1. 先写 `intents`（带 fencing token / lease generation）
2. 调用本地 adapter（事务外）
3. 写 `receipts`（外部收据；旧代次的迟到收据 `accepted=0` 且不推进状态）
4. 推进 `step_runs.status`

结果为 `UNKNOWN` 时步骤进入 `AWAIT_CONFIRM`，只能经 confirm 判定，**不会直接重做**。
若 worker 在 intent 之后、receipt 之前崩溃，恢复时该步骤同样落到 `AWAIT_CONFIRM`（只允许 confirm）。

步骤状态机：

```
PENDING -> INTENT_WRITTEN -> RECEIPT_WRITTEN -> CONFIRMED
                 \-> AWAIT_CONFIRM（UNKNOWN / 崩溃恢复 / 人工判定）
RECEIPT_WRITTEN|AWAIT_CONFIRM|CONFIRMED -> FAILED
CONFIRMED -> COMPENSATING -> COMPENSATED | COMPENSATE_FAILED
```

实例状态：`RUNNING / AWAIT_MANUAL / COMPENSATING / FAILED_COMPENSATION / COMPENSATED / COMPLETED / LOCKED`。

## 补偿语义

- 一支分支失败后，已成功的步骤按 **DAG 依赖逆序（reverse topological）** 补偿。
- 仍在运行的分支先到“稳定边界”：中途（intent 无 receipt）的步骤被转为 AWAIT_CONFIRM，
  补偿游标不会越过它；操作人确认后再继续。
- 补偿失败：步骤置 `COMPENSATE_FAILED`、实例置 `FAILED_COMPENSATION`，
  并持久化“下一次可继续的游标”（`comp_cursor`）。不会被粗暴标记成已回退。
  `POST /api/instances/{id}/resume-compensation` 从该游标续跑。

## 租约与 fencing token

- worker 必须先持有实例租约；续租/接管使用 `BEGIN IMMEDIATE` 原子地读改写，
  每次换代 `lease_generation + 1`，fencing token 即 generation。
- 旧 worker 晚到的收据/确认/补偿若 generation 落后，会被拒收并记录审计事件
  （`receipt_rejected_stale_fence` 等），不能推进新代次。

## 幂等与版本冲突

- 启动请求带 `idemKey`：同键同内容返回原实例；同键不同内容返回 `409 conflict`。
- 人工确认使用步骤的 `expectedVersion`（`step_version` 乐观锁）：
  两个操作者同时批准同一 UNKNOWN 步骤，只有一个成功，另一个得到 `409 version_conflict`。

## 派生重试

`POST /api/instances/{id}/derive`：

- 旧实例置 `LOCKED`，不可再修改；新实例独立执行。
- 仅复用“已确认”且“解析后参数输入摘要（SHA-256 截断）仍一致”的步骤；
  上游结果已失效或输入改变的步骤重新执行。

## JSON API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/api/health` | 健康检查 |
| POST | `/api/definitions` | 发布定义（冻结图/参数/adapter 版本/摘要） |
| GET  | `/api/definitions` | 定义列表 |
| POST | `/api/instances` | 启动（`defId`,`idemKey`,`input`） |
| GET  | `/api/instances` | 实例列表 |
| GET  | `/api/instances/{id}` | 完整视图：DAG、每步 intent/receipt/confirm/compensate、租约代次、补偿顺序/游标、事件 |
| POST | `/api/instances/{id}/tick` | 手动 worker 推进一拍 |
| POST | `/api/instances/{id}/confirm/{stepId}` | 人工判定（`verdict`,`operator`,`expectedVersion`） |
| POST | `/api/instances/{id}/resume-compensation` | 从 failed_compensation 游标继续 |
| POST | `/api/instances/{id}/derive` | 派生重试（`idemKey`,`input?`） |

## 步骤脚本（adapter 行为）

每个步骤声明 `execute` / `confirm` / `compensate`，可以是字符串（单次）或数组（按尝试次数取值，
最后一个重复）。判定词：`SUCCESS` / `FAILURE` / `TIMEOUT` / `UNKNOWN`，
可带 `result`、`delayMs`、`error`。参数支持模板：`${input.xxx}`、`${steps.<id>.result.<field>}`。

## 代码结构

```
app/src/main/java/compapp/
  model/      领域模型、JSON、时钟接口
  db/         SQLite schema 与 Store（Tx 包装、短事务）
  adapter/    Adapter 接口、Verdict、脚本化确定性 adapter
  engine/     DefinitionService（发布/冻结/DAG/逆序）、LeaseManager（fencing 租约）、
              Engine（执行状态机/confirm/补偿游标/幂等/派生重试/崩溃恢复）、Inputs（模板+摘要）
  web/        JDK HttpServer JSON API + 静态页
app/src/test/java/compapp/engine/EngineTest.java   # 14 个用例，时钟与 adapter 注入
```
