#!/usr/bin/env python3
"""llbot_qq 子项目入口：OneBot11 WS 客户端 + 消息缓存 + 触发 + HTTP API。

由 supervisor 拉起:  .venv/bin/python subprojects/llbot_qq/main.py
"""
from __future__ import annotations

import asyncio
import json
import logging
import subprocess
import sys
import time
import uuid
from pathlib import Path

# 项目根加入 sys.path（core/ 所在处）
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import uvicorn  # noqa: E402
import yaml  # noqa: E402

from core.paths import LLBOT_QQ_DIR, LLBOT_QQ_QUEUE_DIR, LOG_DIR  # noqa: E402
from subprojects.llbot_qq.api import build_app  # noqa: E402
from subprojects.llbot_qq.bot import OneBot11Client  # noqa: E402
from subprojects.llbot_qq.cache import MessageCache  # noqa: E402
from subprojects.llbot_qq.media import MediaStore  # noqa: E402
from subprojects.llbot_qq.trigger import should_trigger  # noqa: E402

LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [llbot_qq] %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "llbot_qq-service.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("llbot_qq.main")

# 幻日常驻终端 tmux session（与 core/hermes_worker.py 的 TMUX_SESSION 保持一致）
HERMES_TMUX_SESSION = "huanri"


def inject_to_hermes(text: str, session: str = HERMES_TMUX_SESSION) -> bool:
    """往幻日常驻终端注入一行（tmux send-keys -l + Enter，手法同 hermes_worker）。"""
    try:
        r = subprocess.run(
            ["tmux", "send-keys", "-t", session, "-l", text],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode != 0:
            log.warning("tmux send-keys 失败: %s", r.stderr.strip()[:200])
            return False
        subprocess.run(
            ["tmux", "send-keys", "-t", session, "Enter"],
            capture_output=True, text=True, timeout=10,
        )
        return True
    except Exception:
        log.exception("注入常驻终端异常")
        return False


def load_config() -> dict:
    with open(LLBOT_QQ_DIR / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def make_enqueue(name: str, probs: dict):
    """返回入队函数：触发时写任务到 queue/pending（按当前缓存文件去重）。"""

    def enqueue(source: str, reason: str, event: dict, cache_file: Path) -> bool:
        # 去重：同一 message_id 已在队列中则跳过（防 WS 重复推送/同一条消息重复触发）
        msg_id = str(event.get("message_id", ""))
        for sub in ("pending", "processing"):
            d = LLBOT_QQ_QUEUE_DIR / sub
            if not d.exists():
                continue
            for jf in d.glob("*.json"):
                try:
                    data = json.loads(jf.read_text(encoding="utf-8"))
                except Exception:
                    continue
                if msg_id and str((data.get("trigger_event") or {}).get("message_id", "")) == msg_id:
                    log.info("去重：消息 %s 已有任务在队列中（%s），跳过重复触发",
                             msg_id, jf.name)
                    return False
        # 优先级：含名字/私聊 = high（可打断低优先级任务），其余 = low
        high = ("消息含名字" in reason) or (source == "private_message")
        job = {
            "id": time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6],
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "source": source,
            "reason": reason,
            "priority": "high" if high else "low",
            "trigger_event": event,
            "current_cache_file": str(cache_file),
            "cache_dir": str(LLBOT_QQ_DIR / "cache"),
            "api_base": "http://127.0.0.1:%d" % 8765,
            "status": "pending",
        }
        pending = LLBOT_QQ_QUEUE_DIR / "pending"
        pending.mkdir(parents=True, exist_ok=True)
        tmp = pending / f".tmp-{job['id']}.json"
        tmp.write_text(json.dumps(job, ensure_ascii=False, indent=1), encoding="utf-8")
        tmp.rename(pending / f"{job['id']}.json")
        log.info("已入队触发任务 %s (source=%s, file=%s)", job["id"], source,
                 cache_file.name)
        return True

    return enqueue


def classify_source(event: dict) -> str:
    pt = event.get("post_type", "")
    if pt == "message":
        return "group_message" if event.get("message_type") == "group" else "private_message"
    if pt == "request":
        return "request:" + event.get("request_type", "?")
    if pt == "notice":
        return "notice:" + event.get("notice_type", "?")
    return pt or "unknown"


async def amain() -> None:
    cfg = load_config()
    name = cfg["qq"]["name"]
    self_id = str(cfg["qq"].get("id", ""))
    probs = cfg["trigger"]
    cache = MessageCache(
        LLBOT_QQ_DIR / cfg.get("cache", {}).get("dir", "cache"),
        max_lines=int(cfg.get("cache", {}).get("max_lines", 50)),
    )
    enqueue = make_enqueue(name, probs)
    media_cfg = cfg.get("media", {})
    media = MediaStore(
        LLBOT_QQ_DIR / media_cfg.get("dir", "media"),
        max_age_days=int(media_cfg.get("max_age_days", 30)),
    )
    async def on_event(event: dict) -> None:
        post_type = event.get("post_type")
        # 日志心跳/生命周期（meta_event）一律不缓存不触发
        if post_type == "meta_event" or post_type == "meta":
            return
        # 媒体落地：图片/文件/语音/视频下载到本地，media_path 写回事件
        if post_type == "message" and media_cfg.get("enabled", True):
            try:
                await media.save_event_media(event)
            except Exception:
                log.exception("媒体落地异常（不阻塞消息处理）")
        trig, reason = should_trigger(event, name, probs, self_id=self_id)
        if trig:
            # 触发消息追加进当前缓存文件（不开新文件），知会这个文件；
            # 下一条普通消息会自动滚动到新时间命名的新文件
            cache_file = cache.append_trigger(event)
            enqueue(classify_source(event), reason, event, cache_file)
        else:
            cache.append_message(event)
        _notify_sealed()

    def _notify_sealed() -> None:
        """缓存文件行满封档 → 先注入幻日常驻终端回看，再让它滚到新文件。

        （本体 2026-09-01：满了不能直接跳走，得先把满的缓存推给幻日过一遍。）
        """
        p = cache.sealed_file
        if p is None:
            return
        cache.sealed_file = None
        try:
            n = sum(1 for _ in open(p, encoding="utf-8"))
        except OSError:
            n = 0
        log.info("缓存行满封档 %s（%d 行），注入回看提醒", p.name, n)
        text = (f"[缓存已满] {p} 满了{cache.max_lines}行已封档，"
                f"先把这个文件里的对话读一遍、没办的事办了，新文件在后面继续")
        if not inject_to_hermes(text):
            log.warning("缓存已满提醒注入失败: %s", p.name)

    client = OneBot11Client(cfg["onebot"]["ws_url"],
                            cfg["onebot"].get("ws_token", ""),
                            on_event)
    # 注入 file_fetcher：LLBot 文件消息只有 file_id，靠 get_file 拿本地路径
    async def _get_file(file_id: str):
        try:
            resp = await client.call("get_file", {"file_id": file_id})
            d = resp.get("data") or {}
            if d.get("file"):
                return d["file"], d.get("file_name", "")
            return None
        except Exception as e:
            log.warning("get_file 失败 (%s...): %s", file_id[:16], e)
            return None
    media.file_fetcher = _get_file
    app = build_app(client, cache, cfg, media)

    # 注入主事件循环（API 同步端点跨线程调用用）
    client.api_loop = asyncio.get_running_loop()

    async def _daily_cleanup() -> None:
        """每天凌晨 4 点清理过期媒体。"""
        while True:
            try:
                await asyncio.sleep(3600 * 24)
                media.cleanup()
            except asyncio.CancelledError:
                return
            except Exception:
                log.exception("媒体清理任务异常")

    cleanup_task = asyncio.create_task(_daily_cleanup())
    # 启动时先清一次（处理停机期间累积的过期媒体）
    try:
        media.cleanup()
    except Exception:
        log.exception("启动时媒体清理异常")

    config = uvicorn.Config(app, host=cfg["api"]["host"],
                            port=cfg["api"]["port"], log_level="warning")
    server = uvicorn.Server(config)
    bot_task = asyncio.create_task(client.run())
    log.info("llbot_qq 服务启动: API=%s:%s  WS目标=%s",
             cfg["api"]["host"], cfg["api"]["port"], cfg["onebot"]["ws_url"])
    try:
        await server.serve()
    finally:
        client.stop()
        bot_task.cancel()
        cleanup_task.cancel()
        await media.close()
        try:
            await bot_task
        except asyncio.CancelledError:
            pass
        log.info("llbot_qq 服务已退出")


if __name__ == "__main__":
    asyncio.run(amain())
