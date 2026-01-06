import os
import json
import asyncio
import pandas as pd
import aiohttp
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import List, Dict, Any, Optional
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, StarTools, register
from astrbot.api import logger
from astrbot.core.platform.message_type import MessageType
import base64

@register(
    "astrbot_plugin_group_backup",
    "Foolllll",
    "群备份插件，备份群成员、公告、精华等数据",
    "0.0.1",
    "https://github.com/Foolllll-J/astrbot_plugin_group_backup"
)
class GroupBackupPlugin(Star):
    def __init__(self, context: Context, config: Optional[Dict] = None):
        super().__init__(context)
        self.config = config if config else {}
        self.plugin_data_dir = StarTools.get_data_dir("astrbot_plugin_group_backup")
        self.admin_users = [int(u) for u in self.config.get("admin_users", [])]
        self.download_semaphore = asyncio.Semaphore(5) # 限制并发下载数
        self.default_backup_options = ["群信息", "群头像", "群成员", "群公告", "精华消息", "群相册", "群荣誉"]
        
        # 字段映射：配置项名 -> API 返回的键名
        self.field_map = {
            "QQ号": "user_id",
            "昵称": "nickname",
            "群昵称": "card",
            "权限": "role",
            "加群时间": "join_time",
            "最后发言": "last_sent_time",
        }

    @property
    def backup_options(self) -> List[str]:
        return self.config.get("backup_options", self.default_backup_options)

    def _format_timestamp(self, timestamp):
        """格式化时间戳"""
        if isinstance(timestamp, (int, float)) and timestamp > 0:
            return datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
        return "未知"

    async def _download_file(self, url: str, save_path: Path):
        """下载文件，如果已存在则跳过"""
        if save_path.exists():
            logger.info(f"文件已存在，跳过下载: {save_path}")
            return True
        async with self.download_semaphore:
            try:
                logger.info(f"正在从 {url} 下载文件到 {save_path}")
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=300) as resp:
                        if resp.status == 200:
                            save_path.parent.mkdir(parents=True, exist_ok=True)
                            with open(save_path, "wb") as f:
                                f.write(await resp.read())
                            return True
            except Exception as e:
                logger.error(f"下载文件失败 {url}: {e}")
        return False

    def _get_latest_backup_data(self, group_id: int) -> Dict[str, Any]:
        """获取最近一次备份的数据"""
        group_dir = Path(self.plugin_data_dir) / str(group_id)
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
                    if file_path.name == "metadata.json":
                        data["metadata"] = content
                    else:
                        data[file_path.stem] = content
            except Exception as e:
                logger.warning(f"加载上一次备份文件 {file_path} 失败: {e}")
        
        return data

    def _append_log(self, group_id: int, log_name: str, log_entry: Dict[str, Any]):
        """追加日志记录"""
        log_dir = Path(self.plugin_data_dir) / str(group_id) / "logs"
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
        logger.info(f"已追加日志到 {log_file}: {log_entry}")

    def _archive_deleted_items(self, group_id: int, item_type: str, items: List[Any]):
        """归档已删除的项目到回收站"""
        archive_dir = Path(self.plugin_data_dir) / str(group_id) / "logs"
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
        logger.info(f"已归档 {len(items)} 个已删除的项目（类型: '{item_type}'）到 {archive_file}")

    @filter.command("群备份")
    async def group_backup(self, event: AstrMessageEvent, group_id_arg: str = ""):
        """群备份 [群号]：备份当前群或指定群数据到本地 JSON"""
        # 权限检查：Bot 管理员 或 配置项中的管理员
        is_admin = event.is_admin()
        user_id = int(event.get_sender_id())
        if not is_admin and (not self.admin_users or user_id not in self.admin_users):
            yield event.plain_result(f"❌ 此指令仅限管理员使用")
            return

        target_group_id = group_id_arg.strip()
        if not target_group_id:
            target_group_id = event.get_group_id()
        
        if not target_group_id:
            yield event.plain_result("请在群聊中使用此指令，或在指令后跟随群号。")
            return

        try:
            group_id = int(target_group_id)
            client = event.bot
            
            yield event.plain_result(f"正在开始备份群 {group_id} 的数据...")

            # 加载上一次备份的数据用于增量对比
            latest_data = self._get_latest_backup_data(group_id)
            
            # 1. 获取基础信息
            group_info = await client.get_group_info(group_id=group_id)
            logger.info(f"API 响应 (get_group_info): {json.dumps(group_info, ensure_ascii=False)}")
            
            # 1.1 获取群头像
            if "群头像" in self.backup_options:
                try:
                    avatar_url = f"http://p.qlogo.cn/gh/{group_id}/{group_id}/640/"
                    avatar_save_path = Path(self.plugin_data_dir) / str(group_id) / "group_avatar.png"
                    await self._download_file(avatar_url, avatar_save_path)
                except Exception as e:
                    logger.warning(f"获取群头像失败: {e}")

            # 1.2 获取详细信息
            group_detail = {}
            if "群信息" in self.backup_options:
                try:
                    group_detail = await client.api.call_action("get_group_detail_info", group_id=group_id)
                    logger.info(f"API 响应 (get_group_detail_info): {json.dumps(group_detail, ensure_ascii=False)}")
                except Exception as e:
                    logger.warning(f"获取群信息失败: {e}")

            # 2. 获取成员列表并精简
            members = []
            if "群成员" in self.backup_options:
                raw_members = await client.get_group_member_list(group_id=group_id)
                logger.info(f"API 响应 (get_group_member_list): 获取到 {len(raw_members)} 名成员。")
                essential_keys = ["user_id", "nickname", "card", "role", "join_time", "last_sent_time"]
                for m in raw_members:
                    members.append({k: m.get(k) for k in essential_keys if k in m})
                
                # 增量对比群成员
                if latest_data and "members" in latest_data:
                    old_members_map = {m["user_id"]: m for m in latest_data["members"]}
                    new_members_map = {m["user_id"]: m for m in members}
                    
                    # 谁进群了
                    joiners = [m for uid, m in new_members_map.items() if uid not in old_members_map]
                    # 谁退群了
                    leavers = [m for uid, m in old_members_map.items() if uid not in new_members_map]
                    
                    if joiners:
                        logger.info(f"检测到新成员进群: {joiners}")
                        for m in joiners:
                            self._append_log(group_id, "member_changes", {"type": "入群", "user_id": m["user_id"], "nickname": m.get("nickname")})
                    if leavers:
                        logger.info(f"检测到成员退群: {leavers}")
                        for m in leavers:
                            self._append_log(group_id, "member_changes", {"type": "退群", "user_id": m["user_id"], "nickname": m.get("nickname")})
            
            # 3. 获取公告
            notices = []
            if "群公告" in self.backup_options:
                try:
                    # 使用下划线开头的 API 通常需要 call_action
                    notices = await client.api.call_action("_get_group_notice", group_id=group_id)
                    logger.info(f"API 响应 (_get_group_notice): {json.dumps(notices, ensure_ascii=False)}")
                    
                    # 增量对比群公告
                    if latest_data and "notices" in latest_data:
                        old_notices_map = {n["notice_id"]: n for n in latest_data["notices"]}
                        new_notices_map = {n["notice_id"]: n for n in notices}
                        
                        deleted_notices = [n for nid, n in old_notices_map.items() if nid not in new_notices_map]
                        if deleted_notices:
                            logger.info(f"检测到已删除的群公告: {deleted_notices}")
                            self._archive_deleted_items(group_id, "notices", deleted_notices)
                            for n in deleted_notices:
                                self._append_log(group_id, "content_changes", {"type": "公告已删除", "notice_id": n["notice_id"]})
                except Exception as e:
                    logger.warning(f"获取群公告失败: {e}")
                
            # 4. 获取精华消息
            essence = []
            if "精华消息" in self.backup_options:
                try:
                    essence = await client.get_essence_msg_list(group_id=group_id)
                    logger.info(f"API 响应 (get_essence_msg_list): {json.dumps(essence, ensure_ascii=False)}")
                    
                    # 增量对比精华消息
                    if latest_data and "essence" in latest_data:
                        old_essence_map = {e["message_id"]: e for e in latest_data["essence"]}
                        new_essence_map = {e["message_id"]: e for e in essence}
                        
                        deleted_essence = [e for mid, e in old_essence_map.items() if mid not in new_essence_map]
                        if deleted_essence:
                            logger.info(f"检测到已删除的精华消息: {deleted_essence}")
                            self._archive_deleted_items(group_id, "essence", deleted_essence)
                            for e in deleted_essence:
                                self._append_log(group_id, "content_changes", {"type": "精华消息已删除", "message_id": e["message_id"]})
                except Exception as e:
                    logger.warning(f"获取精华消息失败: {e}")
                
            # 5. 获取群荣誉
            honors = {}
            if "群荣誉" in self.backup_options:
                try:
                    honors = await client.get_group_honor_info(group_id=group_id)
                    logger.info(f"API 响应 (get_group_honor_info): {json.dumps(honors, ensure_ascii=False)}")
                except Exception as e:
                    logger.warning(f"获取群荣誉失败: {e}")

            # 6. 获取群相册并备份原图
            albums = []
            album_media_map = {}
            if "群相册" in self.backup_options:
                try:
                    albums = await client.api.call_action("get_qun_album_list", group_id=str(group_id))
                    logger.info(f"API 响应 (get_qun_album_list): {json.dumps(albums, ensure_ascii=False)}")
                    if albums:
                        yield event.plain_result(f"发现 {len(albums)} 个相册，正在备份原图...")
                        for album in albums:
                            album_id = album.get("album_id")
                            album_name = album.get("name", album_id)
                            
                            # 处理相册改名
                            if latest_data and "albums" in latest_data:
                                old_albums = {a["album_id"]: a.get("name") for a in latest_data["albums"]}
                                if album_id in old_albums and old_albums[album_id] != album_name:
                                    old_name = old_albums[album_id]
                                    if old_name:
                                        old_path = Path(self.plugin_data_dir) / str(group_id) / "albums" / old_name
                                        new_path = Path(self.plugin_data_dir) / str(group_id) / "albums" / album_name
                                        if old_path.exists() and not new_path.exists():
                                            logger.info(f"检测到相册改名: {old_name} -> {album_name}。正在重命名文件夹。")
                                            import shutil
                                            try:
                                                shutil.move(str(old_path), str(new_path))
                                                self._append_log(group_id, "content_changes", {
                                                    "type": "相册已改名",
                                                    "album_id": album_id,
                                                    "old_name": old_name,
                                                    "new_name": album_name
                                                })
                                            except Exception as e:
                                                logger.error(f"重命名相册文件夹失败: {e}")
                            
                            media_list = []
                            attach_info = ""
                            while True:
                                result = await client.api.call_action("get_group_album_media_list", group_id=str(group_id), album_id=album_id, attach_info=attach_info)
                                if result and "m_media" in result:
                                    media_list.extend(result["m_media"])
                                    if result.get("is_finished") or not result.get("attach_info"):
                                        break
                                    attach_info = result["attach_info"]
                                else:
                                    break
                            
                            album_media_map[album_id] = media_list
                            
                            # 下载原图
                            download_tasks = []
                            timestamp_folder = datetime.now().strftime("%Y%m%d_%H%M%S")
                            album_save_dir = Path(self.plugin_data_dir) / str(group_id) / "albums" / album_name
                            for media in media_list:
                                # 优先尝试原图 URL
                                url = media.get("origin_url") or media.get("pre_url") or media.get("url")
                                if url:
                                    media_id = media.get("media_id", datetime.now().timestamp())
                                    file_ext = ".jpg" # 默认为 jpg
                                    # 简单判断后缀
                                    if "video" in media.get("media_type", "").lower():
                                        file_ext = ".mp4"
                                    
                                    save_path = album_save_dir / f"{media_id}{file_ext}"
                                    download_tasks.append(self._download_file(url, save_path))
                            
                            if download_tasks:
                                await asyncio.gather(*download_tasks)
                except Exception as e:
                    logger.error(f"备份群相册失败: {e}")

            # 7. 增量对比群相册（处理已删除的图片/相册）
            if "群相册" in self.backup_options and latest_data and "album_media" in latest_data:
                try:
                    old_album_media = latest_data["album_media"]
                    # 查找已删除的相册
                    for old_album_id, old_media_list in old_album_media.items():
                        if old_album_id not in album_media_map:
                            # 整个相册被删了
                            self._append_log(group_id, "content_changes", {"type": "相册已删除", "album_id": old_album_id})
                            old_albums = {a["album_id"]: a["name"] for a in latest_data.get("albums", [])}
                            old_name = old_albums.get(old_album_id)
                            if old_name:
                                src_dir = Path(self.plugin_data_dir) / str(group_id) / "albums" / old_name
                                if src_dir.exists():
                                    dst_dir = Path(self.plugin_data_dir) / str(group_id) / "logs" / "deleted_items" / "albums" / old_name
                                    dst_dir.parent.mkdir(parents=True, exist_ok=True)
                                    import shutil
                                    if dst_dir.exists(): shutil.rmtree(dst_dir)
                                    logger.info(f"正在将已删除的相册目录从 {src_dir} 移动到 {dst_dir}")
                                    shutil.move(str(src_dir), str(dst_dir))
                        else:
                            # 相册还在，检查里面的图片有没有被删
                            new_media_ids = {m["media_id"] for m in album_media_map[old_album_id]}
                            deleted_media = [m for m in old_media_list if m["media_id"] not in new_media_ids]
                            
                            if deleted_media:
                                # 记录日志
                                for m in deleted_media:
                                    self._append_log(group_id, "content_changes", {
                                        "type": "媒体文件已删除", 
                                        "album_id": old_album_id, 
                                        "media_id": m["media_id"]
                                    })
                                
                                # 将被删图片移动到回收站
                                # 找到当前相册的文件夹名
                                current_albums = {a["album_id"]: a["name"] for a in albums}
                                album_name = current_albums.get(old_album_id)
                                if album_name:
                                    for m in deleted_media:
                                        # 尝试不同的可能后缀
                                        for ext in [".jpg", ".mp4", ".png"]:
                                            src_file = Path(self.plugin_data_dir) / str(group_id) / "albums" / album_name / f"{m['media_id']}{ext}"
                                            if src_file.exists():
                                                dst_file = Path(self.plugin_data_dir) / str(group_id) / "logs" / "deleted_items" / "albums" / album_name / f"{m['media_id']}{ext}"
                                                dst_file.parent.mkdir(parents=True, exist_ok=True)
                                                logger.info(f"正在将已删除的媒体文件从 {src_file} 移动到 {dst_file}")
                                                import shutil
                                                shutil.move(str(src_file), str(dst_file))
                                                break
                except Exception as e:
                    logger.error(f"处理已删除相册图片时出错: {e}")

            # 准备数据目录
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = Path(self.plugin_data_dir) / str(group_id) / timestamp
            backup_path.mkdir(parents=True, exist_ok=True)
            
            # 保存 JSON
            data_to_save = {
                "group_info": group_info,
                "group_detail": group_detail,
                "members": members,
                "notices": notices,
                "essence": essence,
                "honors": honors,
                "albums": albums,
                "album_media": album_media_map,
                "backup_time": datetime.now().isoformat()
            }
            
            for key, val in data_to_save.items():
                file_name = f"{key}.json" if key != "backup_time" else "metadata.json"
                if key == "backup_time":
                    val = {"backup_time": val}
                save_file_path = backup_path / file_name
                with open(save_file_path, "w", encoding="utf-8") as f:
                    json.dump(val, f, ensure_ascii=False, indent=4)
                logger.info(f"成功保存备份快照文件: {save_file_path}")
            
            yield event.plain_result(f"✅ 群 {group_id} 备份成功！\n数据已保存至插件数据目录。")
            
        except Exception as e:
            logger.error(f"群备份出错: {e}")
            yield event.plain_result(f"❌ 备份失败: {e}")

    @filter.command("群导出")
    async def group_export(self, event: AstrMessageEvent, group_id_arg: str = ""):
        """群导出 [群号]：导出当前群或指定群数据为 Excel 并发送"""
        # 权限检查：Bot 管理员 或 配置项中的管理员
        is_admin = event.is_admin()
        user_id = int(event.get_sender_id())
        if not is_admin and (not self.admin_users or user_id not in self.admin_users):
            yield event.plain_result(f"❌ 此指令仅限管理员使用")
            return

        target_group_id = group_id_arg.strip()
        if not target_group_id:
            target_group_id = event.get_group_id()
        
        if not target_group_id:
            yield event.plain_result("请在群聊中使用此指令，或在指令后跟随群号。")
            return

        try:
            group_id = int(target_group_id)
            client = event.bot
            
            yield event.plain_result(f"正在导出群 {group_id} 的数据...")

            output_buffer = BytesIO()
            with pd.ExcelWriter(output_buffer, engine="openpyxl") as writer:
                # 1. 导出群概况 (群信息)
                if "群信息" in self.backup_options:
                    try:
                        detail = await client.api.call_action("get_group_detail_info", group_id=group_id)
                        if detail:
                            # 将字典转换为列表形式以便导出
                            detail_list = [{"属性": k, "值": v} for k, v in detail.items()]
                            
                            # 如果备份了头像，添加头像路径信息
                            if "群头像" in self.backup_options:
                                avatar_path = Path(self.plugin_data_dir) / str(group_id) / "group_avatar.png"
                                if avatar_path.exists():
                                    detail_list.append({"属性": "本地头像路径", "值": str(avatar_path)})
                            
                            pd.DataFrame(detail_list).to_excel(writer, index=False, sheet_name="群概况")
                    except Exception as e:
                        logger.warning(f"导出群概况失败: {e}")

                # 2. 导出群成员
                if "群成员" in self.backup_options:
                    try:
                        members = await client.get_group_member_list(group_id=group_id)
                        processed_members = []
                        for m in members:
                            item = {}
                            for opt, api_key in self.field_map.items():
                                val = m.get(api_key, "")
                                if api_key in ["join_time", "last_sent_time"]:
                                    val = self._format_timestamp(val)
                                elif api_key == "role":
                                    val = {"owner": "群主", "admin": "管理员", "member": "成员"}.get(val, val)
                                item[opt] = val
                            processed_members.append(item)
                        pd.DataFrame(processed_members).to_excel(writer, index=False, sheet_name="群成员")
                    except Exception as e:
                        logger.warning(f"导出群成员失败: {e}")

                # 3. 导出群公告
                if "群公告" in self.backup_options:
                    try:
                        notices = await client.api.call_action("_get_group_notice", group_id=group_id)
                        if notices:
                            processed_notices = []
                            for n in notices:
                                processed_notices.append({
                                    "发布者": n.get("sender_id"),
                                    "发布时间": self._format_timestamp(n.get("publish_time")),
                                    "内容": n.get("content")
                                })
                            pd.DataFrame(processed_notices).to_excel(writer, index=False, sheet_name="群公告")
                    except Exception as e:
                        logger.warning(f"导出群公告失败: {e}")

                # 4. 导出精华消息
                if "精华消息" in self.backup_options:
                    try:
                        essence = await client.get_essence_msg_list(group_id=group_id)
                        if essence:
                            processed_essence = []
                            for e in essence:
                                processed_essence.append({
                                    "发送者": e.get("sender_id"),
                                    "发送时间": self._format_timestamp(e.get("msg_time")),
                                    "内容": e.get("content"),
                                    "操作者": e.get("operator_id")
                                })
                            pd.DataFrame(processed_essence).to_excel(writer, index=False, sheet_name="精华消息")
                    except Exception as e:
                        logger.warning(f"导出精华消息失败: {e}")

                # 5. 导出群荣誉
                if "群荣誉" in self.backup_options:
                    try:
                        honors = await client.get_group_honor_info(group_id=group_id)
                        if honors:
                            honor_list = []
                            # 处理龙王、群霸等荣誉
                            for honor_type, honor_data in honors.items():
                                if isinstance(honor_data, dict) and "user_id" in honor_data:
                                    honor_list.append({"荣誉类型": honor_type, "QQ号": honor_data.get("user_id"), "描述": honor_data.get("nickname")})
                                elif isinstance(honor_data, list):
                                    for h in honor_data:
                                        honor_list.append({"荣誉类型": honor_type, "QQ号": h.get("user_id"), "描述": h.get("nickname")})
                            if honor_list:
                                pd.DataFrame(honor_list).to_excel(writer, index=False, sheet_name="群荣誉")
                    except Exception as e:
                        logger.warning(f"导出群荣誉失败: {e}")

                # 6. 导出群相册列表
                if "群相册" in self.backup_options:
                    try:
                        albums = await client.api.call_action("get_qun_album_list", group_id=str(group_id))
                        if albums:
                            processed_albums = []
                            for a in albums:
                                processed_albums.append({
                                    "相册名": a.get("name"),
                                    "相册ID": a.get("album_id"),
                                    "图片数量": a.get("pic_cnt"),
                                    "创建者": a.get("create_user"),
                                    "创建时间": self._format_timestamp(a.get("create_time"))
                                })
                            pd.DataFrame(processed_albums).to_excel(writer, index=False, sheet_name="群相册列表")
                    except Exception as e:
                        logger.warning(f"导出群相册失败: {e}")
            
            file_content = output_buffer.getvalue()
            if not file_content:
                yield event.plain_result("❌ 未能获取到任何数据进行导出。")
                return

            file_name = f"群{group_id}_全数据导出_{datetime.now().strftime('%Y%m%d')}.xlsx"
            file_content_base64 = base64.b64encode(file_content).decode("utf-8")
            
            # 确定发送目标
            if event.message_obj.type == MessageType.GROUP_MESSAGE:
                await client.upload_group_file(
                    group_id=int(event.get_group_id()),
                    file=f"base64://{file_content_base64}",
                    name=file_name
                )
            else:
                await client.upload_private_file(
                    user_id=int(event.get_sender_id()),
                    file=f"base64://{file_content_base64}",
                    name=file_name
                )
            
            yield event.plain_result(f"✅ 群 {group_id} 数据导出成功，文件已上传。")

        except Exception as e:
            logger.error(f"群导出出错: {e}")
            yield event.plain_result(f"❌ 导出失败: {e}")
