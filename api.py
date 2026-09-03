"""llbot_qq HTTP API —— 供幻日（hermes）执行 QQ 操作。

设计原则：
- 常用操作给具名端点（好记好调）
- /onebot 透传任意 OneBot11 动作（幻日以后封装新能力不用改代码）
- 只监听 127.0.0.1，不出本机

实现说明：
FastAPI 的同步端点跑在线程池里，而 OneBot11 WS 客户端在主事件循环上。
跨线程调用 = asyncio.run_coroutine_threadsafe(coro, client.api_loop)。
client.api_loop 由 main.py 启动时注入。
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import threading
import time
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

log = logging.getLogger("llbot_qq.api")


# ── 请求模型 ───────────────────────────────────────────────────
class GroupMsg(BaseModel):
    group_id: int
    text: str


class PrivateMsg(BaseModel):
    user_id: int
    text: str


class RawMsg(BaseModel):
    """发任意消息，message 是 OneBot11 message segment 数组或纯文本"""
    message: Any
    group_id: int | None = None
    user_id: int | None = None


class RequestOp(BaseModel):
    flag: str
    approve: bool = True
    reason: str = ""


class GroupOp(BaseModel):
    group_id: int
    user_id: int | None = None
    duration: int | None = None  # 秒
    reject_add_request: bool = False


class OneBotAction(BaseModel):
    action: str
    params: dict = {}


class SendThrottle:
    """发送节流：同一目标（群/私聊）两条消息之间强制间隔。

    间隔与**本条消息字数成正比**（模拟真人打字：话越长打得越久），
    再夹在 min_interval / max_interval 之间。从系统层面保证幻日
    不会连发刷屏——即使 AI 忘了 sleep 也拦得住。
    """

    def __init__(self, sec_per_char: float = 0.25,
                 min_interval: float = 1.5, max_interval: float = 8.0):
        self.sec_per_char = max(0.05, sec_per_char)
        self.min_interval = max(0.5, min_interval)
        self.max_interval = max(self.min_interval, max_interval)
        self._last: dict[str, float] = {}
        self._lock = threading.Lock()

    def _key(self, group_id=None, user_id=None) -> str:
        if group_id is not None:
            return f"g:{group_id}"
        if user_id is not None:
            return f"u:{user_id}"
        return "x:global"

    @staticmethod
    def _text_len(message) -> int:
        """估算消息字数：str 按长度；段数组按 text 段拼接。"""
        if isinstance(message, str):
            return len(message)
        if isinstance(message, list):
            n = 0
            for seg in message:
                if isinstance(seg, dict) and seg.get("type") == "text":
                    n += len(str(seg.get("data", {}).get("text", "")))
            return n
        return len(str(message or ""))

    def wait(self, message=None, group_id=None, user_id=None) -> float:
        """阻塞直到允许发送；返回实际等待秒数。间隔 = 字数×每字耗时。"""
        n = self._text_len(message)
        interval = min(max(n * self.sec_per_char, self.min_interval),
                       self.max_interval)
        key = self._key(group_id, user_id)
        with self._lock:
            now = time.monotonic()
            last = self._last.get(key, 0.0)
            need = last + interval - now
            if need > 0:
                time.sleep(need)
                now = time.monotonic()
            self._last[key] = now
            return max(need, 0.0)


def build_app(client, cache, cfg: dict, media=None) -> FastAPI:
    app = FastAPI(title="llbot_qq API", version="1.2")
    name = cfg["qq"]["name"]
    # 发送节流：同目标强制间隔，间隔与字数成正比（读 config.yaml 的 send 段）
    send_cfg = cfg.get("send", {})
    throttle = SendThrottle(
        sec_per_char=float(send_cfg.get("sec_per_char", 0.25)),
        min_interval=float(send_cfg.get("min_interval", 1.5)),
        max_interval=float(send_cfg.get("max_interval", 8.0)),
    )

    def _call(action: str, params: dict | None = None) -> dict:
        """同步端点里跨线程调 WS 客户端。"""
        if client.api_loop is None:
            raise HTTPException(status_code=503, detail="API loop 未就绪")
        try:
            fut = asyncio.run_coroutine_threadsafe(
                client.call(action, params or {}), client.api_loop
            )
            return fut.result(timeout=60)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))

    # ── 状态 ───────────────────────────────────────────────────
    @app.get("/status")
    def status() -> dict:
        from core.paths import LLBOT_QQ_QUEUE_DIR as Q
        queue = {
            sub: len(list((Q / sub).glob("*.json")))
            for sub in ("pending", "processing", "done", "failed")
        }
        return {
            "bot_connected": client.connected,
            "login": client.login_info,          # {user_id, nickname}
            "bot_name": name,
            "current_cache_file": str(cache.current_file) if cache.current_file else None,
            "recent_cache": [str(p) for p in cache.recent_files(5)],
            "queue": queue,
        }

    # ── 发消息（带节流：间隔与字数成正比，模拟真人打字）────────
    @app.post("/send_group")
    def send_group(m: GroupMsg) -> dict:
        throttle.wait(message=m.text, group_id=m.group_id)
        return _call("send_group_msg", {"group_id": m.group_id, "message": m.text})

    @app.post("/send_private")
    def send_private(m: PrivateMsg) -> dict:
        throttle.wait(message=m.text, user_id=m.user_id)
        return _call("send_private_msg", {"user_id": m.user_id, "message": m.text})

    @app.post("/send_msg")
    def send_msg(m: RawMsg) -> dict:
        throttle.wait(message=m.message, group_id=m.group_id, user_id=m.user_id)
        params: dict = {"message": m.message}
        if m.group_id is not None:
            params["group_id"] = m.group_id
        if m.user_id is not None:
            params["user_id"] = m.user_id
        return _call("send_msg", params)

    # ── 好友 / 加群请求 ────────────────────────────────────────
    @app.post("/approve_friend")
    def approve_friend(o: RequestOp) -> dict:
        return _call("set_friend_add_request",
                     {"flag": o.flag, "approve": True})

    @app.post("/reject_friend")
    def reject_friend(o: RequestOp) -> dict:
        return _call("set_friend_add_request",
                     {"flag": o.flag, "approve": False, "reject_message": o.reason})

    @app.post("/approve_group")
    def approve_group(o: RequestOp) -> dict:
        return _call("set_group_add_request",
                     {"flag": o.flag, "subtype": "invite", "approve": True})

    @app.post("/reject_group")
    def reject_group(o: RequestOp) -> dict:
        return _call("set_group_add_request",
                     {"flag": o.flag, "subtype": "invite", "approve": False})

    # ── 群管理 ─────────────────────────────────────────────────
    @app.post("/group_ban")
    def group_ban(o: GroupOp) -> dict:
        return _call("set_group_ban", {
            "group_id": o.group_id,
            "user_id": o.user_id or 0,
            "duration": o.duration or 0,
        })

    @app.post("/group_kick")
    def group_kick(o: GroupOp) -> dict:
        return _call("set_group_kick", {
            "group_id": o.group_id,
            "user_id": o.user_id or 0,
            "reject_add_request": o.reject_add_request,
        })

    @app.post("/group_leave")
    def group_leave(g: GroupMsg) -> dict:
        return _call("set_group_leave", {"group_id": g.group_id})

    # ── 信息查询 ───────────────────────────────────────────────
    @app.get("/login_info")
    def login_info() -> dict:
        return client.login_info

    @app.get("/group_list")
    def group_list() -> dict:
        return _call("get_group_list")

    @app.get("/friend_list")
    def friend_list() -> dict:
        return _call("get_friend_list")

    # ── 通用透传（幻日封装新能力用这个）──────────────────────
    @app.post("/onebot")
    def onebot(a: OneBotAction) -> dict:
        return _call(a.action, a.params)

    # ── 媒体管理 ─────────────────────────────────────────────
    @app.get("/media_stats")
    def media_stats() -> dict:
        if media is None:
            return {"enabled": False}
        return {"enabled": True, **media.stats()}

    @app.post("/media_cleanup")
    def media_cleanup() -> dict:
        if media is None:
            raise HTTPException(status_code=404, detail="媒体模块未启用")
        return media.cleanup()

    # ── 全局消息回顾（跨群聚合）───────────────────────────────
    @app.get("/recent_messages")
    def recent_messages(minutes: int = 30, max_items: int = 60) -> dict:
        """最近 N 分钟内所有群/私聊消息的易读摘要（按时间倒序）。

        幻日判断"要不要回、回哪个群"前先调这个，别只盯着触发的群。
        """
        from datetime import datetime, timedelta
        cutoff = datetime.now() - timedelta(minutes=minutes)
        items: list[dict] = []
        for f in cache.recent_files(50):
            try:
                lines = f.read_text(encoding="utf-8").splitlines()
            except OSError:
                continue
            for line in lines:
                try:
                    ev = json.loads(line)
                except Exception:
                    continue
                t = ev.get("time")
                if not t:
                    continue
                try:
                    when = datetime.fromtimestamp(int(t))
                except Exception:
                    continue
                if when < cutoff:
                    continue
                pt = ev.get("post_type")
                if pt != "message":
                    continue
                # 提取文本 + 媒体标记
                segs = ev.get("message")
                texts, media_marks = [], []
                if isinstance(segs, list):
                    for s in segs:
                        if not isinstance(s, dict):
                            continue
                        st = s.get("type")
                        if st == "text":
                            texts.append(str(s.get("data", {}).get("text", "")))
                        elif st in ("image", "file", "record", "video"):
                            media_marks.append(f"[{st}]")
                sender = ev.get("sender", {}) or {}
                items.append({
                    "time": when.strftime("%H:%M:%S"),
                    "msg_type": ev.get("message_type", ""),
                    "group_id": ev.get("group_id"),
                    "user_id": ev.get("user_id"),
                    "nickname": sender.get("nickname", "") or sender.get("card", "") or "?",
                    "text": "".join(texts),
                    "media": media_marks,
                })
        items.sort(key=lambda x: x["time"], reverse=True)
        return {"count": len(items), "window_minutes": minutes, "items": items[:max_items]}

    return app
