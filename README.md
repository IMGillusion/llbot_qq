# llbot_qq

**QQ 对接子项目**。对接 [LLBot](https://github.com/LLOneBot/LuckyLilliaBot)（OneBot 11 协议）
的 QQ 客户端，做三件事：

1. **收消息**：OneBot11 反向 WebSocket 客户端，自动重连（指数退避）。
2. **消息缓存**：所有消息原始 JSON 落盘 JSONL，按「触发」滚动文件，供长期回顾。
3. **触发 + HTTP API**：按规则决定哪些消息交给上层 AI 处理，并暴露只监听本机的
   HTTP API 供 AI 执行 QQ 操作（发消息、管理、透传任意 OneBot11 动作）。

一句话：让一个常驻 AI 能**看群里说了啥、判断要不要回、再调 API 去回**。

## 文件

| 文件 | 职责 |
|---|---|
| `main.py` | 入口：WS 客户端 + 缓存 + 触发 + HTTP API 组装，常驻 |
| `bot.py` | OneBot11 反向 WS 客户端（自动重连、`call(action, params)` 调动作） |
| `trigger.py` | 触发逻辑：@我 / @全体 / 含名字 → 100%，群 5%，私聊 50%，其他 1% |
| `cache.py` | 消息缓存：JSONL 落盘，按触发滚动文件（文件名=首条消息的自然语言时间） |
| `media.py` | 媒体落地：图/文件/语音/视频下载到 media/，缓存里记 media_path |
| `api.py` | HTTP API（FastAPI，只监听 127.0.0.1）：具名端点 + `/onebot` 透传 |
| `config.yaml` | 配置（QQ 号 / WS 地址 / API 端口 / 触发概率 / 发送节流 / 缓存） |

## 依赖

- 一个跑起来的 **LLBot**（提供 OneBot11 反向 WS，默认 `ws://127.0.0.1:3001`）
- Python 3.10+
- `fastapi`、`uvicorn`、`websockets`、`aiohttp`、`pyyaml`、`httpx`

## 用法

```bash
uv venv .venv --python 3.10
uv pip install --python .venv fastapi uvicorn websockets aiohttp pyyaml httpx

# 常驻入口（由 supervisor / systemd 拉起）
.venv/bin/python main.py
```

起来后 `http://127.0.0.1:8765/status` 看状态（`bot_connected: true` 即 QQ 在线）。

### 常用 API（节选）

```bash
# 发群消息
curl -s -X POST http://127.0.0.1:8765/send_group \
  -H 'Content-Type: application/json' \
  -d '{"group_id": 123456, "text": "大家好"}'

# 发私聊
curl -s -X POST http://127.0.0.1:8765/send_private \
  -H 'Content-Type: application/json' \
  -d '{"user_id": 10001, "text": "嗨"}'

# 任意 OneBot11 动作透传（封装新能力不用改代码）
curl -s -X POST http://127.0.0.1:8765/onebot \
  -H 'Content-Type: application/json' \
  -d '{"action": "get_group_member_list", "params": {"group_id": 123456}}'
```

## 配置

`config.yaml`：

| 段 | 键 | 默认 | 说明 |
|---|---|---|---|
| `qq` | id | YOUR_QQ_ID | **你的 QQ 号**（supervisor 靠它渲染启动命令，@ 判定也用它） |
| | name | 幻日 | AI 在 QQ 世界的名字，也是核心触发词 |
| `onebot` | ws_url | ws://127.0.0.1:3001 | LLBot 的反向 WS 地址 |
| `api` | host / port | 127.0.0.1 / 8765 | HTTP API 监听（只绑本机） |
| `trigger` | group_probability | 0.05 | 群消息触发概率 |
| | private_probability | 0.50 | 私聊触发概率 |
| | other_probability | 0.01 | 其他事件触发概率 |
| `media` | dir / max_age_days | media / 30 | 媒体落地目录 + 自动清理天数 |
| `send` | sec_per_char | 0.25 | 发送节流：每字耗时（模拟真人打字节奏） |
| | min_interval / max_interval | 1.5 / 8.0 | 同一目标两条消息最短/最长间隔 |
| `cache` | dir / max_lines | cache / 50 | 缓存目录 + 单文件行数上限（超限滚动新文件） |

## 触发规则

| 条件 | 触发 |
|---|---|
| 消息 @ 了自己（at 段指向自己 QQ 号，不看显示名） | 100% |
| 消息 @ 了全体成员 | 100% |
| 文本含 AI 名字 | 100% |
| 群消息（其余） | 5% |
| 私聊消息 | 50% |
| 其他事件（好友/加群/系统通知） | 1% |

## 设计要点 / 已知坑

- **只监听 127.0.0.1**：HTTP API 不出本机，安全边界靠这一条。
- **发送节流**：同一目标两条消息间隔按本条字数算（约 0.25s/字，1.5~8s 封顶），
  系统层硬控制，模拟真人打字、防刷屏。
- **`/onebot` 透传**：OneBot11 任意动作都能透传，AI 以后要封装新能力不用改代码。
- **跨线程调 WS**：FastAPI 同步端点跑在线程池，OneBot11 WS 客户端在主事件循环，
  跨线程用 `asyncio.run_coroutine_threadsafe`。
- **消息里带 token/密钥会被 QQ 服务端打码**：收机密文件要走文件消息，别走正文。

—— 幻日出品
