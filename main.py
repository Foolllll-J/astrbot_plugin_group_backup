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

    async def _download_file(self, url: str, save_path: Path, overwrite: bool = False):
        """下载文件，如果已存在且未开启 overwrite 则跳过"""
        if not overwrite and save_path.exists():
            logger.info(f"文件已存在，跳过下载: {save_path}")
            return True
        
        async with self.download_semaphore:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=300) as response:
                        if response.status == 200:
                            content = await response.read()
                            
                            # 如果是覆盖模式，且文件已存在，先检查是否有变化
                            if overwrite and save_path.exists():
                                import hashlib
                                with open(save_path, "rb") as f:
                                    old_content = f.read()
                                if hashlib.md5(content).hexdigest() == hashlib.md5(old_content).hexdigest():
                                    return False # 内容无变化
                            
                            save_path.parent.mkdir(parents=True, exist_ok=True)
                            with open(save_path, "wb") as f:
                                f.write(content)
                            logger.info(f"成功保存文件: {save_path}")
                            return True # 内容有变化或新下载
                        else:
                            logger.warning(f"下载文件失败 {url}: HTTP {response.status}")
            except Exception as e:
                logger.error(f"下载过程出错 {url}: {e}")
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
            
            # 1. 获取详细信息 (包含基础信息)
            group_detail = {}
            if "群信息" in self.backup_options:
                try:
                    raw_detail = await client.api.call_action("get_group_detail_info", group_id=group_id)
                    logger.debug(f"API 响应 (get_group_detail_info): {json.dumps(raw_detail, ensure_ascii=False)}")
                    
                    # 精简群详细信息
                    essential_detail_keys = [
                        "groupCode", "groupName", "ownerUin", "memberNum", "maxMemberNum", 
                        "groupMemo", "groupCreateTime", "activeMemberNum", "groupGrade",
                        "group_all_shut", "groupClassText"
                    ]
                    group_detail = {k: raw_detail.get(k) for k in essential_detail_keys if k in raw_detail}
                            
                except Exception as e:
                    logger.warning(f"获取群信息失败: {e}")

            # 1.1 获取群头像
            if "群头像" in self.backup_options:
                try:
                    avatar_url = f"http://p.qlogo.cn/gh/{group_id}/{group_id}/640/"
                    avatar_dir = Path(self.plugin_data_dir) / str(group_id)
                    avatar_save_path = avatar_dir / "group_avatar.png"
                    temp_avatar_path = avatar_dir / "temp_avatar.png"
                    
                    # 先下载到临时文件
                    await self._download_file(avatar_url, temp_avatar_path, overwrite=True)
                    
                    if temp_avatar_path.exists():
                        import hashlib
                        is_updated = True
                        if avatar_save_path.exists():
                            with open(avatar_save_path, "rb") as f:
                                old_md5 = hashlib.md5(f.read()).hexdigest()
                            with open(temp_avatar_path, "rb") as f:
                                new_md5 = hashlib.md5(f.read()).hexdigest()
                            
                            if old_md5 == new_md5:
                                is_updated = False
                        
                        if is_updated:
                            if avatar_save_path.exists():
                                # 归档旧头像
                                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                archive_path = avatar_dir / "logs" / "deleted_items" / f"avatar_{timestamp}.png"
                                archive_path.parent.mkdir(parents=True, exist_ok=True)
                                import shutil
                                shutil.copy2(avatar_save_path, archive_path)
                                self._append_log(group_id, "content_changes", {"type": "群头像更新", "old_avatar": archive_path.name})
                                logger.info(f"检测到群头像更新，旧头像已归档: {archive_path.name}")
                            
                            # 应用新头像
                            if avatar_save_path.exists(): avatar_save_path.unlink()
                            temp_avatar_path.rename(avatar_save_path)
                        else:
                            # 无变化，删除临时文件
                            temp_avatar_path.unlink()
                except Exception as e:
                    logger.warning(f"获取群头像失败: {e}")

            # 2. 获取成员列表并精简
            members = []
            if "群成员" in self.backup_options:
                raw_members = await client.get_group_member_list(group_id=group_id)
                logger.debug(f"API 响应 (get_group_member_list): 获取到 {len(raw_members)} 名成员。")
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
                    raw_notices = await client.api.call_action("_get_group_notice", group_id=group_id)
                    logger.debug(f"API 响应 (_get_group_notice): {json.dumps(raw_notices, ensure_ascii=False)}")
                    
                    # 精简公告信息
                    for n in raw_notices:
                        notices.append({
                            "notice_id": n.get("notice_id"),
                            "sender_id": n.get("sender_id"),
                            "publish_time": n.get("publish_time"),
                            "text": n.get("message", {}).get("text", "")
                        })
                    
                    # 增量对比群公告
                    if latest_data and "notices" in latest_data:
                        old_notices_map = {n["notice_id"]: n for n in latest_data["notices"]}
                        new_notices_map = {n["notice_id"]: n for n in notices}
                        
                        # 检测新增
                        joiners = [n for nid, n in new_notices_map.items() if nid not in old_notices_map]
                        if joiners:
                            for n in joiners:
                                self._append_log(group_id, "content_changes", {"type": "新增公告", "notice_id": n["notice_id"], "text": n["text"]})
                        
                        # 检测删除
                        deleted_notices = [n for nid, n in old_notices_map.items() if nid not in new_notices_map]
                        if deleted_notices:
                            logger.info(f"检测到已删除的群公告: {deleted_notices}")
                            self._archive_deleted_items(group_id, "notices", deleted_notices)
                            for n in deleted_notices:
                                self._append_log(group_id, "content_changes", {"type": "公告已删除", "notice_id": n["notice_id"]})
                        
                        # 检测编辑 (ID 相同但内容或其他属性变化)
                        for nid, new_n in new_notices_map.items():
                            if nid in old_notices_map:
                                old_n = old_notices_map[nid]
                                # 只要 text 变化了，就认为被编辑了
                                if new_n.get("text") != old_n.get("text"):
                                    logger.info(f"检测到公告已编辑 (ID: {nid})")
                                    self._append_log(group_id, "content_changes", {
                                        "type": "公告已编辑", 
                                        "notice_id": nid,
                                        "old_text": old_n.get("text"),
                                        "new_text": new_n.get("text")
                                    })
                except Exception as e:
                    logger.warning(f"获取群公告失败: {e}")
                
            # 4. 获取精华消息
            essence = []
            if "精华消息" in self.backup_options:
                try:
                    raw_essence = await client.get_essence_msg_list(group_id=group_id)
                    logger.debug(f"API 响应 (get_essence_msg_list): {json.dumps(raw_essence, ensure_ascii=False)}")
                    
                    # 精简精华消息
                    for e in raw_essence:
                        essence.append({
                            "message_id": e.get("message_id"),
                            "sender_id": e.get("sender_id"),
                            "sender_nick": e.get("sender_nick"),
                            "operator_id": e.get("operator_id"),
                            "operator_nick": e.get("operator_nick"),
                            "operator_time": e.get("operator_time"),
                            "content": e.get("content")
                        })
                    
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
                    logger.debug(f"API 响应 (get_group_honor_info): {json.dumps(honors, ensure_ascii=False)}")
                except Exception as e:
                    logger.warning(f"获取群荣誉失败: {e}")

            # 6. 获取群相册并备份原图
            albums = []
            album_media_map = {}
            if "群相册" in self.backup_options:
                try:
                    raw_albums = await client.api.call_action("get_qun_album_list", group_id=str(group_id))
                    logger.debug(f"API 响应 (get_qun_album_list): {json.dumps(raw_albums, ensure_ascii=False)}")
                    if raw_albums:
                        logger.info(f"发现 {len(raw_albums)} 个相册，正在备份原图...")
                        for album in raw_albums:
                            album_id = album.get("album_id")
                            album_name = album.get("name", album_id)
                            
                            albums.append({
                                "album_id": album_id,
                                "name": album_name,
                                "create_time": album.get("create_time"),
                                "modify_time": album.get("modify_time"),
                                "creator_nick": album.get("creator", {}).get("nick"),
                                "upload_number": album.get("upload_number")
                            })
                            
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
                            
                            # 检查相册是否有更新（通过修改时间）
                            media_list = []
                            is_album_updated = True
                            if latest_data and "albums" in latest_data and "album_media" in latest_data:
                                old_album_info = next((a for a in latest_data["albums"] if a["album_id"] == album_id), None)
                                if old_album_info and str(old_album_info.get("modify_time")) == str(album.get("modify_time")):
                                    # 修改时间未变，尝试复用上次的媒体列表
                                    media_list = latest_data["album_media"].get(album_id, [])
                                    if media_list:
                                        is_album_updated = False
                                        logger.info(f"相册 {album_name} 修改时间未变，跳过 API 请求，复用上次备份的 {len(media_list)} 个媒体记录。")
                            
                            if is_album_updated:
                                try:
                                    # 根据 log.txt 格式，API 返回一个包含 "media_list" 的对象
                                    result = await client.api.call_action("get_group_album_media_list", group_id=str(group_id), album_id=album_id)
                                    logger.debug(f"获取相册 {album_name}({album_id}) 媒体列表结果: {json.dumps(result, ensure_ascii=False)}")
                                    
                                    raw_media_list = []
                                    if isinstance(result, dict):
                                        # 检查日志中的 media_list 键
                                        if "media_list" in result:
                                            raw_media_list = result["media_list"]
                                        elif "media" in result:
                                            raw_media_list = result["media"]
                                        elif "m_media" in result:
                                            raw_media_list = result["m_media"]
                                        elif "album" in result:
                                            album_info = result["album"]
                                            if "cover" in album_info and "image" in album_info["cover"]:
                                                raw_media_list = [album_info["cover"]]
                                    elif isinstance(result, list):
                                        raw_media_list = result
                                    
                                    for m in raw_media_list:
                                        # 提取媒体详情：log 显示媒体项包含 "image" 或 "video"
                                        media_detail = m.get("image") or m.get("video") or m
                                        
                                        # 提取 URL：URL 在 photo_url 列表中
                                        photo_urls = media_detail.get("photo_url", [])
                                        # 优先选择 spec 1 (原图) 或 6 (高清)，如果没有则取第一个
                                        best_url = ""
                                        if photo_urls:
                                            # 尝试寻找 spec 1 或 6
                                            for p in photo_urls:
                                                if p.get("spec") in [1, 6]:
                                                    best_url = p.get("url", {}).get("url", "")
                                                    break
                                            if not best_url:
                                                best_url = photo_urls[0].get("url", {}).get("url", "")
                                        
                                        # 如果没有 photo_url，尝试 default_url
                                        if not best_url:
                                            best_url = media_detail.get("default_url", {}).get("url", "")

                                        media_list.append({
                                            "media_id": media_detail.get("lloc") or m.get("media_id") or m.get("id"),
                                            "url": best_url,
                                            "media_type": m.get("type") or m.get("media_type"),
                                            "upload_time": m.get("upload_time")
                                        })
                                except Exception as e:
                                    logger.error(f"获取相册 {album_id} 媒体列表失败: {e}")
                            
                            album_media_map[album_id] = media_list
                            
                            # 下载原图（如果是新相册、已更新或本地文件缺失，_download_file 会处理）
                            if media_list:
                                if is_album_updated:
                                    logger.info(f"正在下载相册 {album_name} 中的 {len(media_list)} 个媒体文件...")
                                download_tasks = []
                                album_save_dir = Path(self.plugin_data_dir) / str(group_id) / "albums" / album_name
                                album_save_dir.mkdir(parents=True, exist_ok=True)
                                
                                for media in media_list:
                                    url = media.get("url")
                                    if url:
                                        media_id = media.get("media_id")
                                        if not media_id: continue
                                        
                                        file_ext = ".jpg" 
                                        media_type = str(media.get("media_type", "")).lower()
                                        if "video" in media_type or media_type == "2":
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
            group_base_dir = Path(self.plugin_data_dir) / str(group_id)
            backup_path = group_base_dir / timestamp
            backup_path.mkdir(parents=True, exist_ok=True)
            
            # 保存 JSON
            data_to_save = {
                "group_detail": group_detail,
                "members": members,
                "notices": notices,
                "essence": essence,
                "honors": honors,
                "albums": albums,
                "album_media": album_media_map
            }
            
            # 执行保存
            for key, val in data_to_save.items():
                file_name = f"{key}.json"
                save_file_path = backup_path / file_name
                with open(save_file_path, "w", encoding="utf-8") as f:
                    json.dump(val, f, ensure_ascii=False, indent=4)
                logger.info(f"成功保存备份快照文件: {save_file_path}")
            
            # 删除除当前刚创建的备份以外的所有旧快照文件夹
            all_backups = sorted([d for d in group_base_dir.iterdir() if d.is_dir() and d.name.replace("_", "").isdigit()], key=lambda x: x.name)
            for old_backup in all_backups:
                if old_backup.name != timestamp:
                    try:
                        import shutil
                        shutil.rmtree(str(old_backup))
                        logger.info(f"已清理旧备份快照: {old_backup.name}")
                    except Exception as e:
                        logger.error(f"清理旧备份失败 {old_backup.name}: {e}")
            
            yield event.plain_result(f"✅ 群 {group_id} 备份成功！\n（已根据最新数据更新快照，并保留历史变更日志）")
            
        except Exception as e:
            logger.error(f"群备份出错: {e}")
            yield event.plain_result(f"❌ 备份失败: {e}")

    @filter.command("删除群备份")
    async def delete_group_backup(self, event: AstrMessageEvent, group_id_arg: str = ""):
        """删除群备份 [群号]：物理删除指定群组的所有备份数据"""
        # 权限检查
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
            group_dir = Path(self.plugin_data_dir) / str(group_id)
            
            if not group_dir.exists():
                yield event.plain_result(f"🔍 未找到群 {group_id} 的备份数据。")
                return

            import shutil
            # 物理删除整个群目录
            shutil.rmtree(str(group_dir))
            
            logger.info(f"管理员 {user_id} 删除了群 {group_id} 的所有备份数据。")
            yield event.plain_result(f"✅ 已成功删除群 {group_id} 的所有备份数据（包括相册和日志）。")

        except Exception as e:
            logger.error(f"删除群备份出错: {e}")
            yield event.plain_result(f"❌ 删除失败: {e}")

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
                            # 映射常用字段为中文
                            display_detail = {
                                "群名称": detail.get("groupName"),
                                "群号": detail.get("groupCode"),
                                "群分类": detail.get("groupClassText"),
                                "群主QQ": detail.get("ownerUin"),
                                "成员人数": detail.get("memberNum"),
                                "最大人数": detail.get("maxMemberNum"),
                                "当前活跃人数": detail.get("activeMemberNum"),
                                "群公告": detail.get("groupMemo"),
                            }
                            # 将字典转换为列表形式以便导出
                            detail_list = [{"属性": k, "值": v} for k, v in display_detail.items() if v is not None]
                            
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
                                msg = n.get("message", {})
                                content = msg.get("text", "")
                                processed_notices.append({
                                    "发布者": n.get("sender_id"),
                                    "发布时间": self._format_timestamp(n.get("publish_time")),
                                    "内容": content
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
                                raw_content = e.get("content", [])
                                content_str = ""
                                if isinstance(raw_content, list):
                                    for seg in raw_content:
                                        if seg.get("type") == "text":
                                            content_str += seg.get("data", {}).get("text", "")
                                        elif seg.get("type") == "at":
                                            content_str += f"@{seg.get('data', {}).get('qq', '')} "
                                        else:
                                            content_str += f"[{seg.get('type')}]"
                                else:
                                    content_str = str(raw_content)

                                processed_essence.append({
                                    "发送者": e.get("sender_id"),
                                    "发送时间": self._format_timestamp(e.get("operator_time")), # log 显示是 operator_time
                                    "内容": content_str,
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
                            # 处理龙王等荣誉
                            for honor_type, honor_data in honors.items():
                                if honor_type == "group_id": continue
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
                                    "图片数量": a.get("upload_number"),
                                    "创建者": a.get("creator", {}).get("nick"),
                                    "创建时间": self._format_timestamp(a.get("create_time")),
                                    "修改时间": self._format_timestamp(a.get("modify_time"))
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
