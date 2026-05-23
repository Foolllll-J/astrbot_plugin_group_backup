import asyncio
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, StarTools
from astrbot.api import logger
from .modules.album_service import (
    backup_albums as backup_albums_service,
    normalize_album_list_response,
    normalize_album_media_response,
)
from .modules.utils import download_file, format_essence_content
from .modules.storage_service import append_log, archive_deleted_items, get_latest_backup_data
from .modules.backup_service import delete_group_backup_command, group_backup_command
from .modules.export_service import group_export_command
from .modules.restore_service import group_recall_command, group_restore_command


class GroupBackupPlugin(Star):
    def __init__(self, context: Context, config: Optional[Dict] = None):
        super().__init__(context)
        self.config = config if config else {}
        self.plugin_data_dir = StarTools.get_data_dir("astrbot_plugin_group_backup")
        self.download_semaphore = asyncio.Semaphore(5) # 限制并发下载数
        
        self.admin_users = [int(u) for u in self.config.get("admin_users", [])]
        self.backup_options = self.config.get("backup_options", ["群信息", "群头像", "群成员", "群公告", "群精华", "群相册", "群荣誉"])
        self.restore_options = self.config.get("restore_options", ["群名称", "群头像", "群昵称", "群头衔", "群管理", "群相册"])
        self.recall_interval = int(self.config.get("recall_interval", 60)) # 默认 60 秒
        
        self.field_map = {
            "QQ号": "user_id",
            "昵称": "nickname",
            "群昵称": "card",
            "权限": "role",
            "等级": "level",
            "头衔": "title",
            "加群时间": "join_time",
            "最后发言": "last_sent_time",
        }
        self._is_llbot = False
        self._backend_client_id: int | None = None

    async def _ensure_backend_detected(self, client) -> None:
        if client is None:
            return

        client_id = id(client)
        if self._backend_client_id == client_id:
            return

        self._backend_client_id = client_id
        self._is_llbot = False

        try:
            version_info = await client.api.call_action("get_version_info")
            app_name = version_info.get("app_name") if isinstance(version_info, dict) else None
            if app_name is None and isinstance(version_info, dict) and isinstance(version_info.get("data"), dict):
                app_name = version_info["data"].get("app_name")
            self._is_llbot = app_name == "LLOneBot"
            logger.debug(
                f"[group_backup] 协议端探测结果: app_name={app_name or 'unknown'}, "
                f"backend={'llbot' if self._is_llbot else 'napcat'}"
            )
        except Exception as e:
            logger.warning(f"[group_backup] 启动时探测协议端失败，默认按 NapCat 处理: {e}")

    def _format_timestamp(self, timestamp):
        """格式化时间戳"""
        if isinstance(timestamp, (int, float)) and timestamp > 0:
            return datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
        return "未知"

    def _normalize_album_list_response(self, payload: Any) -> List[Dict[str, Any]]:
        return normalize_album_list_response(payload)

    def _normalize_album_media_response(self, payload: Any) -> List[Dict[str, Any]]:
        return normalize_album_media_response(payload)

    async def _get_group_album_list(self, client, group_id: int | str):
        await self._ensure_backend_detected(client)
        if self._is_llbot:
            api = getattr(client, "api", None)
            if api is None:
                raise RuntimeError("llbot client.api 不可用，无法获取群相册列表")
            result = await api.call_action("get_group_album_list", group_id=str(group_id))
            return result
        return await client.get_qun_album_list(group_id=str(group_id))

    def _format_essence_content(self, raw_content):
        """格式化精华消息内容"""
        return format_essence_content(raw_content)

    async def _download_file(self, url: str, save_path: Path, overwrite: bool = False):
        """下载文件，如果已存在且未开启 overwrite 则跳过"""
        return await download_file(self.download_semaphore, url, save_path, overwrite)

    def _get_latest_backup_data(self, group_id: int) -> Dict[str, Any]:
        return get_latest_backup_data(self, group_id)

    def _append_log(self, group_id: int, log_name: str, log_entry: Dict[str, Any]):
        return append_log(self, group_id, log_name, log_entry)

    def _archive_deleted_items(self, group_id: int, item_type: str, items: List[Any]):
        return archive_deleted_items(self, group_id, item_type, items)

    async def _backup_albums(self, client, group_id: int, latest_data: Dict = None):
        return await backup_albums_service(self, client, group_id, latest_data)

    @filter.command("群备份")
    async def group_backup(self, event: AstrMessageEvent, group_id_arg: str = ""):
        """群备份 [群号]：备份当前群或指定群数据到本地"""
        await self._ensure_backend_detected(getattr(event, "bot", None))
        async for ret in group_backup_command(self, event, group_id_arg):
            yield ret

    @filter.command("删除群备份")
    async def delete_group_backup(self, event: AstrMessageEvent, group_id_arg: str = ""):
        """删除群备份 [群号]：物理删除指定群组的所有备份数据"""
        await self._ensure_backend_detected(getattr(event, "bot", None))
        async for ret in delete_group_backup_command(self, event, group_id_arg):
            yield ret
    @filter.command("群导出")
    async def group_export(self, event: AstrMessageEvent, args: str = ""):
        """群导出 [群号] [选项...]：导出指定数据。选项可选：群信息、群成员、群公告、群精华、群荣誉、群相册"""
        await self._ensure_backend_detected(getattr(event, "bot", None))
        async for ret in group_export_command(self, event, args):
            yield ret

    @filter.command("群恢复")
    async def group_restore(self, event: AstrMessageEvent, group_id_arg: str = ""):
        """群恢复 [群号]：将指定群或当前群的备份数据恢复到当前群"""
        await self._ensure_backend_detected(getattr(event, "bot", None))
        async for ret in group_restore_command(self, event, group_id_arg):
            yield ret

    @filter.command("群友召回", alias={"群召回", "群友找回", "群员召回", "群员找回"})
    async def group_recall(self, event: AstrMessageEvent):
        """群友召回 [群等级] [群号] [消息文本] 或 [群号] [群等级] [消息文本]"""
        await self._ensure_backend_detected(getattr(event, "bot", None))
        async for ret in group_recall_command(self, event):
            yield ret
