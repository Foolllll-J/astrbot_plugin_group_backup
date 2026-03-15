from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from astrbot.api import logger

def get_latest_backup_data(plugin, group_id: int) -> Dict[str, Any]:
    """获取最近一次备份的数据"""
    group_dir = Path(plugin.plugin_data_dir) / str(group_id)
    if not group_dir.exists():
        return {}

    # 查找时间戳目录
    backups = [d for d in group_dir.iterdir() if d.is_dir() and d.name.replace("_", "").isdigit()]
    if not backups:
        return {}

    # 按时间戳排序
    latest_backup_dir = sorted(backups, key=lambda x: x.name)[-1]

    data = {}
    for file_path in latest_backup_dir.glob("*.json"):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = json.load(f)
                data[file_path.stem] = content
        except Exception as e:
            logger.warning(f"加载上一次备份文件 {file_path} 失败: {e}")

    return data


def append_log(plugin, group_id: int, log_name: str, log_entry: Dict[str, Any]):
    """追加日志记录"""
    log_dir = Path(plugin.plugin_data_dir) / str(group_id) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{log_name}.json"

    logs = []
    if log_file.exists():
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                logs = json.load(f)
        except:
            logs = []

    logs.append({
        "log_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        **log_entry
    })

    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(logs, f, ensure_ascii=False, indent=4)
    logger.debug(f"已追加日志到 {log_file}，键: {list(log_entry.keys())}")


def archive_deleted_items(plugin, group_id: int, item_type: str, items: List[Any]):
    """归档已删除的项目到回收站"""
    archive_dir = Path(plugin.plugin_data_dir) / str(group_id) / "logs"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_file = archive_dir / "deleted_items.json"

    archive = {}
    if archive_file.exists():
        try:
            with open(archive_file, "r", encoding="utf-8") as f:
                archive = json.load(f)
        except:
            archive = {}

    if item_type not in archive:
        archive[item_type] = []

    for item in items:
        archive[item_type].append({
            "deleted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "content": item
        })

    with open(archive_file, "w", encoding="utf-8") as f:
        json.dump(archive, f, ensure_ascii=False, indent=4)
    logger.debug(f"已归档 {len(items)} 个已删除项目（类型: '{item_type}'）到 {archive_file}")
