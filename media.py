"""媒体落地：图片/文件/语音/视频消息下载到本地 media/ 目录。

- 事件里的 message 段若有 image/file/record/video，取 url 下载
- 下载成功后在原段的 data 里写 media_path（本地绝对路径），
  缓存 JSON 里就带路径，幻日以后可以读文件/看图
- 下载失败不阻塞：记一条日志，消息照常缓存（只是没有本地媒体）
- 清理：按天分目录，超过 max_age_days 的目录整体删除
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import httpx

log = logging.getLogger("llbot_qq.media")

MEDIA_TYPES = {"image", "file", "record", "video"}
# 文件名黑名单字符（防路径穿越/注入）
_BAD = set('/\\:*?"<>|')


def _sanitize(name: str) -> str:
    return "".join("_" if ch in _BAD or ord(ch) < 32 else ch for ch in name)


def _ext_from(url: str, fallback: str) -> str:
    """从 url 或文件名猜扩展名。"""
    path = urlparse(url).path
    ext = Path(path).suffix.lower()
    if ext and len(ext) <= 8 and ext[1:].isalnum():
        return ext
    fext = Path(fallback).suffix.lower()
    if fext and len(fext) <= 8 and fext[1:].isalnum():
        return fext
    return ".bin"


class MediaStore:
    def __init__(self, media_dir: Path, max_age_days: int = 30,
                 file_fetcher=None):
        """file_fetcher: async (file_id) -> (本地路径str, 文件名str) | None。

        用于 LLBot 新协议：文件消息只有 file_id 没有 url，必须调
        OneBot11 get_file 动作拿本地路径（main.py 注入）。
        """
        self.dir = Path(media_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.max_age_days = max_age_days
        self.file_fetcher = file_fetcher
        self._client = httpx.AsyncClient(
            timeout=15.0,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (Qbot3.1 llbot_qq)"},
        )

    # ── 下载 ───────────────────────────────────────────────────
    async def save_event_media(self, event: dict) -> dict:
        """扫描事件的 message 段，下载媒体，写 media_path。返回原 event（原地修改）。"""
        msg = event.get("message")
        if not isinstance(msg, list):
            return event
        day_dir = self.dir / datetime.now().strftime("%Y-%m-%d")
        day_dir.mkdir(parents=True, exist_ok=True)
        for seg in msg:
            if not isinstance(seg, dict) or seg.get("type") not in MEDIA_TYPES:
                continue
            data = seg.get("data") or {}
            url = str(data.get("url") or "").strip()
            fname = str(data.get("file") or "").strip()
            file_id = str(data.get("file_id") or "").strip()
            if url:
                path = await self._download(day_dir, url, fname)
            elif file_id and self.file_fetcher:
                # LLBot 新协议：文件消息只有 file_id，调 get_file 拿本地路径
                path = await self._fetch_by_id(day_dir, file_id, fname)
            else:
                # 没有 url 也没有 file_id 的跳过
                continue
            if path:
                data["media_path"] = str(path)
        return event

    async def _fetch_by_id(self, day_dir: Path, file_id: str, fname: str) -> Path | None:
        """通过 OneBot11 get_file 拿文件（LLBot 把文件放 data/temp/），复制进 media/。"""
        try:
            resp = await self.file_fetcher(file_id)
            if not resp:
                return None
            src, name = resp
            src_path = Path(src)
            if not src_path.exists():
                log.warning("get_file 返回的路径不存在: %s", src)
                return None
            base = _sanitize(name or src_path.name) or "file.bin"
            import hashlib
            h = hashlib.md5(file_id.encode()).hexdigest()[:8]
            target = day_dir / f"{time.strftime('%H%M%S')}-{h}-{base}"
            if target.exists() and target.stat().st_size > 0:
                return target
            import shutil
            shutil.copy2(src_path, target)
            log.info("媒体落地(file_id): %s (%d bytes)", target.name, target.stat().st_size)
            return target
        except Exception as e:
            log.warning("get_file 落地失败 (file_id=%s...): %s", file_id[:16], e)
            return None

    async def _download(self, day_dir: Path, url: str, fname: str) -> Path | None:
        try:
            ext = _ext_from(url, fname)
            base = _sanitize(fname) or f"media{ext}"
            if not base.lower().endswith(ext):
                base = f"{base}{ext}"
            # 防重名：md5 前 8 位 + 时间戳
            import hashlib
            h = hashlib.md5(url.encode()).hexdigest()[:8]
            target = day_dir / f"{time.strftime('%H%M%S')}-{h}-{base}"
            if target.exists() and target.stat().st_size > 0:
                return target
            resp = await self._client.get(url)
            resp.raise_for_status()
            target.write_bytes(resp.content)
            log.info("媒体落地: %s (%d bytes)", target.name, len(resp.content))
            return target
        except Exception as e:
            log.warning("媒体下载失败 (%s): %s", url[:60], e)
            return None

    # ── 清理 ───────────────────────────────────────────────────
    def cleanup(self) -> dict:
        """删除超过 max_age_days 的媒体目录。返回统计。"""
        cutoff = datetime.now() - timedelta(days=self.max_age_days)
        removed_dirs, removed_bytes = 0, 0
        for d in sorted(self.dir.glob("????-??-??")):
            try:
                d_date = datetime.strptime(d.name, "%Y-%m-%d")
            except ValueError:
                continue
            if d_date < cutoff:
                size = sum(f.stat().st_size for f in d.glob("*") if f.is_file())
                for f in d.glob("*"):
                    try:
                        f.unlink()
                    except OSError:
                        pass
                try:
                    d.rmdir()
                except OSError:
                    pass
                removed_dirs += 1
                removed_bytes += size
                log.info("媒体清理: 删除 %s (%d bytes)", d.name, size)
        return {"removed_dirs": removed_dirs, "removed_bytes": removed_bytes}

    def stats(self) -> dict:
        files = [f for d in self.dir.glob("????-??-??") for f in d.glob("*") if f.is_file()]
        total = sum(f.stat().st_size for f in files)
        return {"file_count": len(files), "total_bytes": total, "max_age_days": self.max_age_days}

    async def close(self) -> None:
        await self._client.aclose()
