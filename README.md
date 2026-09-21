# 🎙️ 发言席位（Speak Queue）

适合**线下讨论、会议投屏**使用的多人发言席位 Web 应用：

- 创建者一键生成 **6 位房间码**，即主持人；
- 参与者用**昵称加入**，自行**申请 / 取消发言**；
- 排队者之间可**协商换位**：发起邀请、对方接受/拒绝，位置原子交换，冲突自动失效；
- 主持人可**拖动排序**、**开始 / 结束发言**、将成员移出队列、**一键清空**；
- 主持人上述四类操作支持**安全撤销**：只回滚受影响的队列位置与当前发言，
  之后出现任何版本化变更即立即失效，撤销记录全程留下可见审计；
- 大屏展示当前发言人、大号**发言计时**、发言队列、在场成员与**操作记录**；
- 弱网/刷新/休眠后**自动断线重连**，恢复一致的队列顺序与当前发言计时；
- 所有队列命令携带客户端所见**版本号**，服务端**原子校验 + 单调递增**，
  过期命令被拒绝并返回最新快照，多端操作不会互相覆盖。

技术栈：Node.js + Express + ws（WebSocket），前端为原生 HTML / CSS / JavaScript，无构建步骤。

---

## 快速开始（Docker）

需要安装 Docker（含 Compose v2）。

```bash
docker compose up --build
```

该命令会自动完成依赖安装、镜像构建并启动服务。启动后浏览器访问：

> **http://localhost:3000**

主持人打开页面 → 「创建房间」→ 把 6 位房间码（或「复制邀请链接」）投给参与者；
参与者打开同一地址输入房间码和昵称即可加入。

停止服务：

```bash
docker compose down
```

> 若希望使用其他宿主机端口（例如 8080）：
>
> ```bash
> PORT=8080 docker compose up --build
> ```
>
> 然后访问 http://localhost:8080

---

## 本地直接运行（不使用 Docker）

```bash
npm install
npm start
```

默认监听 http://localhost:3000 。

运行端到端测试（会自动在 3099 端口拉起一个测试服务实例）：

```bash
npm test
```

---

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `3000` | 服务监听端口。compose 中同时用它控制宿主机映射端口（`${PORT:-3000}:3000`） |
| `HOST` | `0.0.0.0` | 监听地址 |
| `WS_PATH` | `/ws` | WebSocket 挂载路径 |
| `ROOM_TTL_MS` | `14400000`（4 小时） | 房间内**所有人离线**后，房间保留多久再回收 |
| `LOG_LIMIT` | `500` | 服务端保留的操作记录最大条数（超出滚动淘汰） |
| `SWAP_RESOLVED_TTL_MS` | `30000`（30 秒） | 已结束的换位邀请（接受/拒绝/撤回/失效）在房间快照中继续保留的时长，供前端展示操作反馈 |

compose.yaml 的 `environment` 段可直接修改容器内配置；宿主机端口可通过
`docker compose up --build` 前设置 `PORT` 环境变量覆盖。

---

## 使用说明

### 主持人（创建房间者）

- 页面会显示黄色「主持人」标识；创建信息保存在本浏览器，刷新/断线自动恢复主持身份。
- **拖动**队列条目前的 ⠿ 手柄即可调整发言顺序（鼠标与触屏均可）。
- 每条队列可点 **▶ 开始** 请其发言（自动从队列移除并开始计时），舞台区可随时**结束发言**。
- **安全撤销**：拖动排序、移出队列、开始 / 结束发言后，队列下方会出现绿色
  **可撤销**条，显示最近一次操作摘要（如「请『阿东』开始发言」），点 **↩ 撤销**
  可原子恢复受影响的队列位置与当前发言：
  - 仅允许撤销**最近一次**主持人操作；操作后只要出现**任一版本化状态变更**
    （成员加入 / 离开 / 断线、申请或取消发言、换位邀请变化、清空记录等），
    撤销条立即变为红色并展示**失效原因**，无法再撤销。
  - 撤销不会回滚成员在线状态、身份凭证或无关日志；恢复发言沿用原
    `startedAt`，发言计时连续不重置。
  - 每次成功撤销都会在操作记录中追加一条青色**审计记录**（`audit`）。
  - 版本冲突时 `undo_host` 同样返回 `STALE_VERSION` 与最新快照，客户端自动同步。
- **一键清空**：清空发言队列、结束当前发言并重置操作记录。
- 另有单独的**清空记录**按钮，只清空操作记录。

### 参与者

- 输入房间码 + 昵称加入；断线后自动用原身份重连（刷新页面也会恢复席位与队列位置）。
- 点 **🙋 申请发言** 排入队尾，排队中可随时**取消申请**。
- **协商换位**：排队后，队列中其他成员条目上会出现 **⇄ 换位** 按钮，点击即向对方
  发起换位邀请；对方可**接受**或**拒绝**，你也可以在对方回应前**撤回邀请**。
  - 每人同一时刻最多参与一条待处理邀请；邀请中条目会显示「⇄ 换位邀请/被邀中」标签。
  - 队列上方分别展示**我发起的**（等待回应/撤回）与**我收到的**（接受/拒绝）邀请卡。
  - 接受时服务端重新校验双方仍在发起时的原位置，再**原子交换**；
    若任一方已取消申请、开始发言、被移除、离开，或主持人重排/清空，
    邀请自动**失效**并在操作记录中注明原因，队列保持不变。
- 正在发言时无法重复申请；离线成员在队列中会标注「离线」，重连后自动归位。

### 投屏建议

页面采用大字号、高对比布局；建议使用全屏浏览器（F11）。窗口较窄时布局自动变为单列。

---

## 协议与一致性设计

所有消息均为 WebSocket 文本帧（JSON）。

**客户端 → 服务端**（除 `join` / `leave` 外均需带 `version`）：

```
{ "type": "request_speak",  "version": 12 }
{ "type": "cancel_speak",   "version": 12 }
{ "type": "swap_invite",    "version": 12, "participantId": "p_xxx" }
{ "type": "swap_accept",    "version": 12, "swapId": "sw_xxx" }
{ "type": "swap_reject",    "version": 12, "swapId": "sw_xxx" }
{ "type": "swap_cancel",    "version": 12, "swapId": "sw_xxx" }
{ "type": "start_speaking", "version": 12, "participantId": "p_xxx" }   // 主持人
{ "type": "end_speaking",   "version": 12 }                             // 主持人
{ "type": "reorder",        "version": 12, "order": ["p_a", "p_b"] }    // 主持人
{ "type": "remove",         "version": 12, "participantId": "p_xxx" }   // 主持人
{ "type": "undo_host",      "version": 12 }                             // 主持人：安全撤销
{ "type": "clear",          "version": 12 }                             // 主持人
{ "type": "clear_logs",     "version": 12 }                             // 主持人
```

房间快照携带 `undo` 字段，描述最近一次主持人操作的可逆状态：

```
{
  "op": "start_speaking",     // reorder | remove | start_speaking | end_speaking
  "status": "available",      // available（可撤销）| invalid（已被介入变更作废）
  "version": 12,              // 撤销锚点：该操作提交后的版本；undo_host 必须携带此版本
  "ts": 1726…,
  "summary": "请「阿东」开始发言",  // 客户端直接展示的操作摘要
  "reason": null,             // invalid 时的机器可读失效原因
  "reasonText": null          // invalid 时的可读失效原因
}
```

- 只有 `reorder / remove / start_speaking / end_speaking` 四类主持人操作会武装
  逆操作；之后**任一**成功提交的版本化状态变更都会把 `undo.status` 置为
  `invalid` 并注明原因（`queue_changed / speaking_changed / presence_changed /
  swap_changed / cleared / logs_cleared / state_changed`）。
- `undo_host` 与普通命令一样经过版本守卫：版本不匹配返回
  `{ type:"error", code:"STALE_VERSION", snapshot:{…最新快照…} }`；
  无可撤销操作返回 `UNDO_NONE`，已失效返回 `UNDO_INVALIDATED`（均附最新快照）。
- 撤销原子恢复受影响的队列位置与当前发言（恢复发言沿用原 `startedAt`），
  **不**回滚成员在线状态、身份凭证或无关日志，仅追加一条 `kind:"audit"` 的
  可见审计记录；撤销本身也是一次版本化提交，撤销后 `undo` 为 `null`（不提供重做）。

换位邀请记录随房间快照的 `swaps` 字段广播：

```
{
  "id": "sw_…", "from": "p_a", "to": "p_b",
  "version": 12, "fromPos": 0, "toPos": 2,          // 发起时的队列版本与位置（0 基下标）
  "status": "pending",        // pending | accepted | rejected | cancelled | invalid
  "reason": null,             // 失效/结束原因，如 reordered、left、position_changed 等
  "ts": 1726…, "resolvedAt": null
}
```

- `swap_invite` 与普通队列命令一样严格校验版本；`swap_accept / swap_reject /
  swap_cancel` 只要求携带 `version`，不强制版本相等——接受的真正冲突以双方位置
  重新校验为准，队尾无关变动（版本增长但位置未变）不会使操作失败。
- 接受时若 `order[fromPos] === from && order[toPos] === to` 不成立，邀请置为
  `invalid`（`reason: "position_changed"`）且**队列保持不变**；校验通过则在同一
  次同步状态变更中交换两个下标。
- 一方取消申请、开始发言、被主持人移除、主动离开，或主持人重排 / 一键清空时，
  相关待处理邀请统一置为 `invalid` 并在操作记录注明原因（断线保留邀请，重连后可继续）。

- 服务端在 Node 单事件循环中**同步**处理每条命令：校验 `version === 当前版本`
  后立即修改状态、`version += 1`，并向房间内所有连接广播快照——整个过程不可被其他命令插入。
- 版本不匹配时返回 `{ type:"error", code:"STALE_VERSION", snapshot:{…最新快照…} }`，
  客户端用其覆盖本地状态后再渲染，杜绝过期覆盖。
- 每次快照附带 `serverTs`，客户端据此修正本机时钟偏差，发言计时基于服务端的
  `speaking.startedAt`，重连后计时连续不重置。
- 断线不会立即删除席位：成员标记为离线、队列位置与当前发言均保留；
  参与者凭 `participantId + secret`、主持人凭 `hostToken` 重连恢复。

完整消息清单见 `server.js` 顶部注释，行为用例见 `test-e2e.js`。

---

## 项目结构

```
.
├── server.js          # Express + WebSocket 服务端（房间、版本化命令、协商换位、记录、重连）
├── public/
│   ├── index.html     # 首页与房间视图
│   ├── style.css      # 投屏优化样式
│   └── app.js         # WS 客户端、自动重连、渲染、拖拽排序、计时
├── test-e2e.js        # 端到端协议测试（npm test）
├── Dockerfile
├── compose.yaml
└── package.json
```

## 说明 / 限制

- 房间状态保存在**内存**中（适合一次线下会议场景）：进程重启或房间超过
  `ROOM_TTL_MS` 无人在线后数据清除；无外部数据库依赖。
- 主持人能力由服务端通过 `hostToken` 校验，令牌仅保存在创建者浏览器的 localStorage 中。
