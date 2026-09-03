"""触发逻辑：什么时候把消息交给幻日处理。

规则（项目要求）：
- 消息里 @ 了幻日（at 段指向自己的 QQ 号，不看显示名）→ 100% 触发
- 消息里 @ 了全体成员（mention_all 段）→ 100% 触发（2026-09-01 本体要求）
- 文本含 AI 名字（默认「幻日」）→ 100% 触发
- 群消息 → 5% 概率
- 私聊消息 → 20% 概率
- 其他事件（好友请求、加群请求、系统通知等）→ 1% 概率
- 日志心跳（meta_event）→ 上游已过滤，这里不处理
"""
from __future__ import annotations

import random
import re
from typing import Any

_AT_CQ_RE = re.compile(r"\[CQ:at[^\]]*?qq=(\d+)")


def at_self(event: dict, self_id: str) -> bool:
    """消息里是否 @ 了本人。

    优先看 at 段的 data.qq（按 QQ 号比，跟显示名无关）；
    再兜底扫 raw_message 里的 CQ:at 码（有的上游不给分段）。
    """
    self_id = str(self_id or "")
    if not self_id:
        return False
    msg = event.get("message")
    if isinstance(msg, list):
        for seg in msg:
            if isinstance(seg, dict) and seg.get("type") == "at":
                if str(seg.get("data", {}).get("qq", "")) == self_id:
                    return True
    raw = event.get("raw_message") or event.get("raw")
    if isinstance(raw, str):
        for m in _AT_CQ_RE.finditer(raw):
            if m.group(1) == self_id:
                return True
    return False


def at_all(event: dict) -> bool:
    """消息里是否 @ 了全体成员。

    llbot V8.1.9 实测（2026-09-01）：@全员 进来是 at 段 data.qq=="all"
    （不是独立 mention_all 段），raw 里是 [CQ:at,qq=all]。两种都判。
    """
    msg = event.get("message")
    if isinstance(msg, list):
        for seg in msg:
            if not isinstance(seg, dict):
                continue
            if seg.get("type") == "mention_all":
                return True
            if seg.get("type") == "at" and str(seg.get("data", {}).get("qq", "")) == "all":
                return True
    raw = event.get("raw_message") or event.get("raw")
    if isinstance(raw, str) and "[CQ:at,qq=all]" in raw:
        return True
    return False


def extract_text(event: dict) -> str:
    """从 OneBot11 消息事件里提取纯文本。"""
    # 优先 raw 字段；否则拼 text 段
    raw = event.get("raw")
    if isinstance(raw, str):
        return raw
    msg = event.get("message")
    if isinstance(msg, str):
        return msg
    if isinstance(msg, list):
        return "".join(
            seg.get("data", {}).get("text", "")
            for seg in msg
            if isinstance(seg, dict) and seg.get("type") == "text"
        )
    return ""


def should_trigger(event: dict, name: str, probs: dict, self_id: str = "") -> tuple[bool, str]:
    """返回 (是否触发, 原因)。"""
    post_type = event.get("post_type", "")

    if post_type == "message":
        if at_self(event, self_id):
            return True, f"消息@了{name}"
        if at_all(event):
            return True, "消息@了全体成员"
        text = extract_text(event)
        if name and name in text:
            return True, f"消息含名字「{name}」"
        msg_type = event.get("message_type", "")
        if msg_type == "group":
            p = float(probs.get("group_probability", 0.05))
        else:
            p = float(probs.get("private_probability", 0.20))
    else:
        # notice / request 等其他事件
        p = float(probs.get("other_probability", 0.01))

    if random.random() < p:
        return True, f"概率触发({p:.0%})"
    return False, ""
