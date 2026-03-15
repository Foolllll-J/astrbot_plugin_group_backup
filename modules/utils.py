from __future__ import annotations

import aiohttp
from datetime import datetime
from pathlib import Path
from typing import Any

from astrbot.api import logger


def format_timestamp(timestamp: Any) -> str:
    if isinstance(timestamp, (int, float)) and timestamp > 0:
        return datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
    return "未知"


def format_essence_content(raw_content: Any) -> str:
    content_str = ""
    if isinstance(raw_content, list):
        for seg in raw_content:
            if seg.get("type") == "text":
                content_str += seg.get("data", {}).get("text", "")
            elif seg.get("type") == "at":
                content_str += f"@{seg.get('data', {}).get('qq', '')} "
            elif seg.get("type") == "image":
                content_str += "[图片]"
            elif seg.get("type") == "face":
                content_str += "[表情]"
            else:
                content_str += f"[{seg.get('type', '未知')}]"
    else:
        content_str = str(raw_content)
    return content_str


async def download_file(
    semaphore,
    url: str,
    save_path: Path,
    overwrite: bool = False,
) -> bool:
    if not overwrite and save_path.exists():
        logger.debug(f"文件已存在，跳过下载: {save_path}")
        return True

    async with semaphore:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=300) as response:
                    if response.status == 200:
                        content = await response.read()
                        if overwrite and save_path.exists():
                            import hashlib

                            with open(save_path, "rb") as f:
                                old_content = f.read()
                            if hashlib.md5(content).hexdigest() == hashlib.md5(old_content).hexdigest():
                                return False

                        save_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(save_path, "wb") as f:
                            f.write(content)
                        logger.debug(f"成功保存文件: {save_path}")
                        return True
                    logger.warning(f"下载文件失败 {url}: HTTP {response.status}")
        except Exception as e:
            logger.error(f"下载过程出错 {url}: {e}")
    return False

