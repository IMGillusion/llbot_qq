"""消息缓存：长期落盘，供幻日后回顾记忆。

语义（项目要求）：
- 所有消息（日志心跳除外）都以原始完整 JSON 存成 JSONL。
- 缓存文件放在 cache/ 下，文件名 = 该文件第一条消息进入时的
  「精确自然语言时间」，如: 2026年8月31日19时45分07秒.jsonl
- 每次触发开一个新文件：触发消息成为新文件的第一行；
  后续消息（到下一次触发前）追加进当前文件。
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime
from pathlib import Path


def natural_time(when: datetime) -> str:
    """精确自然语言时间：2026年8月31日19时45分07秒"""
    return (f"{when.year}年{when.month}月{when.day}日"
            f"{when.hour}时{when.minute:02d}分{when.second:02d}秒")


class MessageCache:
    def __init__(self, cache_dir: Path, max_lines: int = 50):
        self.dir = Path(cache_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.current_file: Path | None = None
        self.rollover_pending = False  # 触发后置 True，下一条普通消息滚动新文件
        self.max_lines = max_lines     # 单文件行数上限，超限自动滚动新文件
        self.sealed_file: Path | None = None  # 刚因行满被封档的文件（待知会幻日回看），
                                              # 由主流程读取后负责清掉

    # ── 文件管理 ───────────────────────────────────────────────
    def _unique_path(self, base_name: str) -> Path:
        """同名（同一秒内多次触发）时自动加序号。"""
        p = self.dir / base_name
        if not p.exists():
            return p
        i = 2
        while (self.dir / f"{base_name}-{i}.jsonl").exists():
            i += 1
        return self.dir / f"{base_name}-{i}.jsonl"

    def _open_current(self, when: datetime) -> Path:
        self.current_file = self._unique_path(natural_time(when) + ".jsonl")
        # 先落个空文件标记
        self.current_file.touch()
        return self.current_file

    def _over_limit(self) -> bool:
        """当前文件是否超过行数上限（触发/追加前检查，超限就滚动）。"""
        if self.current_file is None or not self.current_file.exists():
            return False
        try:
            # 快速数行：读文件统计 \n；小文件直接读，大文件用分块
            with open(self.current_file, "rb") as f:
                n = sum(1 for _ in f)
            return n >= self.max_lines
        except OSError:
            return False

    # ── 写入 ───────────────────────────────────────────────────
    def _append(self, line: str) -> Path:
        with self._lock:
            path: Path = self.current_file or self._open_current(datetime.now())
            self.current_file = path
            with open(path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
            return path

    def append_message(self, event: dict) -> Path:
        """普通消息：当前文件行满则封档滚动（记 sealed_file 供主流程知会幻日回看）；
        触发后第一条则滚动到触发后的新文件。"""
        with self._lock:
            if self._over_limit():
                self.sealed_file = self.current_file  # 行满封档，等幻日回看
                self._open_current(datetime.now())
                self.rollover_pending = False
            elif self.rollover_pending:
                self._open_current(datetime.now())
                self.rollover_pending = False
            return self._append(json.dumps(event, ensure_ascii=False))

    def append_trigger(self, event: dict) -> Path:
        """触发消息：当前文件行满则先封档滚动（记 sealed_file）；
        追加后标记下一条普通消息滚动。"""
        with self._lock:
            if self._over_limit():
                self.sealed_file = self.current_file  # 行满封档，等幻日回看
                self._open_current(datetime.now())
            path = self._append(json.dumps(event, ensure_ascii=False))
            self.rollover_pending = True
            return path

    # 兼容旧名：begin_session = append_trigger（触发消息进当前文件）
    def begin_session(self, event: dict) -> Path:
        return self.append_trigger(event)

    # ── 查询 ───────────────────────────────────────────────────
    def recent_files(self, n: int = 10) -> list[Path]:
        """按文件名（=时间）倒序的最近 n 个缓存文件。"""
        files = sorted(self.dir.glob("*.jsonl"), reverse=True)
        return files[:n]
