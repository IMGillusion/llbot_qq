"""OneBot 11 反向 WebSocket 客户端：对接 LLBot。

- 自动重连（指数退避）
- 收到事件 → on_event 回调（缓存 + 触发）
- 提供 call(action, params)：通过同一条 WS 调 OneBot11 动作（幻日执行 QQ 操作走这里）
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any, Awaitable, Callable

import websockets

log = logging.getLogger("llbot_qq.bot")

EventHandler = Callable[[dict], Awaitable[None]]


class OneBot11Client:
    def __init__(self, ws_url: str, token: str, on_event: EventHandler):
        self.ws_url = ws_url
        self.token = token
        self.on_event = on_event
        self.ws: Any = None
        self.api_loop: "asyncio.AbstractEventLoop | None" = None
        self.pending: dict[str, asyncio.Future] = {}
        self.stopped = False
        self.connected = False
        self.login_info: dict = {}

    # ── 主循环（自动重连）─────────────────────────────────────
    async def run(self) -> None:
        backoff = 2
        while not self.stopped:
            try:
                headers = {}
                if self.token:
                    headers["Authorization"] = f"Bearer {self.token}"
                async with websockets.connect(
                    self.ws_url, additional_headers=headers,
                    ping_interval=30, ping_timeout=20, max_size=2**24,
                ) as ws:
                    self.ws = ws
                    self.connected = True
                    backoff = 2
                    log.info("OneBot11 WS 已连接: %s", self.ws_url)
                    await self._handshake(ws)
                    # 拉一次登录信息（用于 /status）
                    try:
                        info = await self.call("get_login_info")
                        self.login_info = (info or {}).get("data", {})
                    except Exception as e:
                        log.warning("get_login_info 失败: %s", e)
                    async for frame in ws:
                        if self.stopped:
                            break
                        try:
                            data = json.loads(frame)
                        except (TypeError, json.JSONDecodeError):
                            continue
                        if "status" in data:
                            self._resolve_call(data)
                        else:
                            try:
                                await self.on_event(data)
                            except Exception:
                                log.exception("on_event 处理异常")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self.stopped:
                    break
                log.warning("WS 断开/异常: %s，%ds 后重连", e, backoff)
                self.connected = False
                self._fail_all_pending(f"连接断开: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
        self.connected = False

    async def _handshake(self, ws: Any) -> None:
        """OneBot11 反向 WS 握手：set_listener（不强制，但标准实现都认）。"""
        try:
            await ws.send(json.dumps({
                "action": "set_listener",
                "params": {"endpoint": self.ws_url},
            }))
        except Exception as e:
            log.debug("set_listener 握手失败（可忽略）: %s", e)

    # ── API 调用 ───────────────────────────────────────────────
    def _resolve_call(self, data: dict) -> None:
        echo = data.get("echo")
        fut = self.pending.pop(echo, None) if echo else None
        if fut is not None and not fut.done():
            fut.set_result(data)

    def _fail_all_pending(self, reason: str) -> None:
        for echo, fut in list(self.pending.items()):
            if not fut.done():
                fut.set_exception(ConnectionError(reason))
        self.pending.clear()

    async def call(self, action: str, params: dict | None = None,
                   timeout: float = 30.0) -> dict:
        """调用 OneBot11 动作，返回完整响应（{status, retcode, data, echo}）。"""
        if self.ws is None or not self.connected:
            raise ConnectionError("OneBot11 WS 未连接")
        echo = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self.pending[echo] = fut
        try:
            await self.ws.send(json.dumps(
                {"action": action, "params": params or {}, "echo": echo},
                ensure_ascii=False,
            ))
            resp = await asyncio.wait_for(fut, timeout)
        finally:
            self.pending.pop(echo, None)
        if resp.get("status") not in ("ok", "async", "good"):
            raise RuntimeError(f"OneBot11 {action} 失败: {resp}")
        return resp

    def stop(self) -> None:
        self.stopped = True
        self._fail_all_pending("客户端停止")
