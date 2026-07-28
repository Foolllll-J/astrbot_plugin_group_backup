import asyncio
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, StarTools
from astrbot.api import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from .modules.album_service import (
    backup_albums as backup_albums_service,
    normalize_album_list_response,
    normalize_album_media_response,
)
from .modules.utils import download_file, format_essence_content
from .modules.storage_service import (
    append_log,
    archive_deleted_items,
    get_latest_backup_data,
    load_cron_configs,
    save_cron_configs,
)
from .modules.backup_service import (
    delete_group_backup_command,
    group_backup_command,
    parse_backup_args,
    scheduled_backup,
)
from .modules.export_service import group_export_command
from .modules.restore_service import group_recall_command, group_restore_command
from .modules.email_notifier import SmtpConfig
from apscheduler.triggers.cron import CronTrigger


class GroupBackupPlugin(Star):
    def __init__(self, context: Context, config: Optional[Dict] = None):
        super().__init__(context)
        self.config = config if config else {}
        self.plugin_data_dir = StarTools.get_data_dir("astrbot_plugin_group_backup")
        self.download_semaphore = asyncio.Semaphore(5)  # 限制并发下载数

        self._scheduler = AsyncIOScheduler()
        self._bot_client = None
        self._bound_platform_id: str | None = None

        self.admin_users = [int(u) for u in self.config.get("admin_users", [])]
        self.backup_options = self.config.get(
            "backup_options",
            ["群信息", "群头像", "群成员", "群公告", "群精华", "群相册", "群荣誉"],
        )
        self.restore_options = self.config.get(
            "restore_options",
            ["群名称", "群头像", "群昵称", "群头衔", "群管理", "群相册"],
        )
        self.recall_interval = int(self.config.get("recall_interval", 60))  # 默认 60 秒
        self._smtp_config: SmtpConfig | None = self._read_smtp_config()

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
            app_name = (
                version_info.get("app_name") if isinstance(version_info, dict) else None
            )
            if (
                app_name is None
                and isinstance(version_info, dict)
                and isinstance(version_info.get("data"), dict)
            ):
                app_name = version_info["data"].get("app_name")
            self._is_llbot = app_name == "LLOneBot"
            logger.debug(
                f"[group_backup] 协议端探测结果: app_name={app_name or 'unknown'}, "
                f"backend={'llbot' if self._is_llbot else 'napcat'}"
            )
        except Exception as e:
            logger.warning(
                f"[group_backup] 启动时探测协议端失败，默认按 NapCat 处理: {e}"
            )

    def _read_smtp_config(self) -> SmtpConfig | None:
        cfg = self.config.get("email_smtp", {}) or {}
        host = str(cfg.get("smtp_host", "") or "").strip()
        if not host:
            return None
        user = str(cfg.get("smtp_user", "") or "").strip()
        return SmtpConfig(
            host=host,
            port=int(cfg.get("smtp_port", 465)),
            use_tls=bool(cfg.get("smtp_use_tls", True)),
            user=user,
            password=str(cfg.get("smtp_password", "") or ""),
            sender=str(cfg.get("email_from", "") or "").strip() or user,
        )

    def _format_timestamp(self, timestamp):
        """格式化时间戳"""
        if isinstance(timestamp, (int, float)) and timestamp > 0:
            return datetime.fromtimestamp(float(timestamp)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
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
            result = await api.call_action(
                "get_group_album_list", group_id=str(group_id)
            )
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

    async def _try_bind_bot_from_platform_manager(self) -> bool:
        try:
            platform_manager = getattr(self.context, "platform_manager", None)
            get_insts = getattr(platform_manager, "get_insts", None)
            if not callable(get_insts):
                return False
            platforms = get_insts()
            aiocqhttp_platforms = []
            for p in platforms:
                try:
                    if p.meta().name == "aiocqhttp":
                        aiocqhttp_platforms.append(p)
                except Exception:
                    continue
            if not aiocqhttp_platforms:
                return False
            # 优先匹配已保存的 platform_id
            target = None
            if self._bound_platform_id:
                for p in aiocqhttp_platforms:
                    if p.meta().id == self._bound_platform_id:
                        target = p
                        break
            # 兜底：仅一个 aiocqhttp 实例
            if target is None and len(aiocqhttp_platforms) == 1:
                target = aiocqhttp_platforms[0]
            if target is None:
                logger.warning(
                    "[group_backup] 存在多个 aiocqhttp 实例且无匹配 platform_id，跳过绑定"
                )
                return False
            bot_client = target.get_client()
            if bot_client is None:
                return False
            self._bot_client = bot_client
            self._bound_platform_id = target.meta().id
            await self._ensure_backend_detected(self._bot_client)
            logger.info(
                f"[group_backup] 已绑定 bot (platform_id={self._bound_platform_id})"
            )
            return True
        except Exception as e:
            logger.warning(f"[group_backup] 绑定 bot 客户端失败: {e}")
            return False

    async def _delayed_start_scheduler(self):
        await asyncio.sleep(30)
        if self._bot_client is None:
            await self._try_bind_bot_from_platform_manager()
        if self._bot_client:
            configs = load_cron_configs(self)
            for gid_str, entry in configs.items():
                try:
                    cron = entry["cron"] if isinstance(entry, dict) else entry
                    gid = int(gid_str)
                    self._schedule_cron_job(gid, cron)
                    logger.info(f"[group_backup] 延迟恢复定时备份: 群 {gid} -> {cron}")
                except (ValueError, Exception) as e:
                    logger.warning(
                        f"[group_backup] 延迟恢复定时备份失败 {gid_str}: {e}"
                    )

    async def initialize(self):
        # 从已有 cron 配置中恢复 platform_id
        configs = load_cron_configs(self)
        for entry in configs.values():
            pid = entry.get("platform_id") if isinstance(entry, dict) else None
            if pid:
                self._bound_platform_id = pid
                break

        await self._try_bind_bot_from_platform_manager()
        self._scheduler.start()
        for gid_str, entry in configs.items():
            try:
                cron = entry["cron"] if isinstance(entry, dict) else entry
                gid = int(gid_str)
                self._schedule_cron_job(gid, cron)
                logger.info(f"[group_backup] 已恢复定时备份: 群 {gid} -> {cron}")
            except (ValueError, Exception) as e:
                logger.warning(f"[group_backup] 恢复定时备份失败 {gid_str}: {e}")
        if self._bot_client is None:
            asyncio.create_task(self._delayed_start_scheduler())

    def _ensure_event_bot_bound(self, event: AstrMessageEvent):
        if self._bot_client is not None:
            return
        candidate = getattr(event, "bot", None)
        if candidate is not None:
            self._bot_client = candidate

    def _get_bot(self):
        return self._bot_client

    def _cron_job_id(self, group_id: int) -> str:
        return f"group_backup_cron_{group_id}"

    def _schedule_cron_job(self, group_id: int, cron_expr: str):
        job_id = self._cron_job_id(group_id)
        trigger = CronTrigger.from_crontab(cron_expr)
        self._scheduler.add_job(
            scheduled_backup,
            id=job_id,
            trigger=trigger,
            args=[self, group_id],
            replace_existing=True,
            misfire_grace_time=120,
            coalesce=True,
            max_instances=1,
        )
        logger.info(f"[group_backup] 定时备份已设置: 群 {group_id} -> {cron_expr}")

    def _remove_cron_job(self, group_id: int):
        job_id = self._cron_job_id(group_id)
        if self._scheduler.get_job(job_id):
            self._scheduler.remove_job(job_id)
            logger.info(f"[group_backup] 定时备份已取消: 群 {group_id}")

    @filter.command("群备份")
    async def group_backup(self, event: AstrMessageEvent):
        """群备份 [群号]：备份当前群或指定群数据到本地。支持定时：输入标准5段cron表达式可设置定时备份。"""
        self._ensure_event_bot_bound(event)
        await self._ensure_backend_detected(self._bot_client)

        parts = event.message_str.strip().split(maxsplit=1)
        group_id_arg = parts[1] if len(parts) > 1 else ""

        if not group_id_arg:
            async for ret in group_backup_command(self, event, group_id_arg):
                yield ret
            return

        group_id, cron_expr, is_stop = parse_backup_args(group_id_arg)

        if group_id is None and cron_expr is None and not is_stop:
            async for ret in group_backup_command(self, event, group_id_arg):
                yield ret
            return

        is_admin = event.is_admin()
        user_id = int(event.get_sender_id())
        if not is_admin and (not self.admin_users or user_id not in self.admin_users):
            yield event.plain_result("❌ 此指令仅限管理员使用")
            return

        if is_stop:
            target_group = group_id if group_id is not None else event.get_group_id()
            if not target_group:
                yield event.plain_result(
                    "❌ 请在群聊中使用此指令，或在指令后跟随群号。"
                )
                return
            target_group = int(target_group)
            self._remove_cron_job(target_group)
            configs = load_cron_configs(self)
            configs.pop(str(target_group), None)
            save_cron_configs(self, configs)
            yield event.plain_result(f"✅ 已取消群 {target_group} 的定时备份。")
            return

        if cron_expr is None:
            async for ret in group_backup_command(self, event, group_id_arg):
                yield ret
            return

        target_group_id = group_id if group_id is not None else event.get_group_id()
        if not target_group_id:
            yield event.plain_result("❌ 请在群聊中使用此指令，或在指令后跟随群号。")
            return
        target_group_id = int(target_group_id)

        try:
            self._schedule_cron_job(target_group_id, cron_expr)
            configs = load_cron_configs(self)
            platform_id = getattr(event, "get_platform_id", lambda: None)()
            configs[str(target_group_id)] = {
                "cron": cron_expr,
                "platform_id": platform_id,
            }
            save_cron_configs(self, configs)
            yield event.plain_result(
                f"✅ 群 {target_group_id} 定时备份已设置：{cron_expr}"
            )
        except Exception as e:
            yield event.plain_result(f"❌ 定时表达式无效: {e}")

    @filter.command("删除群备份")
    async def delete_group_backup(
        self, event: AstrMessageEvent, group_id_arg: str = ""
    ):
        """删除群备份 [群号]：物理删除指定群组的所有备份数据"""
        self._ensure_event_bot_bound(event)
        await self._ensure_backend_detected(self._bot_client)
        async for ret in delete_group_backup_command(self, event, group_id_arg):
            yield ret

    @filter.command("群导出")
    async def group_export(self, event: AstrMessageEvent, args: str = ""):
        """群导出 [群号] [选项...]：导出指定数据。选项可选：群信息、群成员、群公告、群精华、群荣誉、群相册"""
        self._ensure_event_bot_bound(event)
        await self._ensure_backend_detected(self._bot_client)
        async for ret in group_export_command(self, event, args):
            yield ret

    @filter.command("群恢复")
    async def group_restore(self, event: AstrMessageEvent, group_id_arg: str = ""):
        """群恢复 [群号]：将指定群或当前群的备份数据恢复到当前群"""
        self._ensure_event_bot_bound(event)
        await self._ensure_backend_detected(self._bot_client)
        async for ret in group_restore_command(self, event, group_id_arg):
            yield ret

    @filter.command("群友召回", alias={"群召回", "群友找回", "群员召回", "群员找回"})
    async def group_recall(self, event: AstrMessageEvent):
        """群友召回 [群等级] [群号] [消息文本] 或 [群号] [群等级] [消息文本]"""
        self._ensure_event_bot_bound(event)
        await self._ensure_backend_detected(self._bot_client)
        async for ret in group_recall_command(self, event):
            yield ret
