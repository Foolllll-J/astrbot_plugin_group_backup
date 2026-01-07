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
import zipfile
import shutil

@register(
    "astrbot_plugin_group_backup",
    "Foolllll",
    "群备份插件，备份群成员、公告、精华等数据",
    "0.1",
    "https://github.com/Foolllll-J/astrbot_plugin_group_backup"
)
class GroupBackupPlugin(Star):
    def __init__(self, context: Context, config: Optional[Dict] = None):
        super().__init__(context)
        self.config = config if config else {}
        self.plugin_data_dir = StarTools.get_data_dir("astrbot_plugin_group_backup")
        self.download_semaphore = asyncio.Semaphore(5) # 限制并发下载数
        
        # 字段映射：配置项名 -> API 返回的键名
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

    @property
    def admin_users(self) -> List[int]:
        return [int(u) for u in self.config.get("admin_users", [])]

    @property
    def backup_options(self) -> List[str]:
        return self.config.get("backup_options", ["群信息", "群头像", "群成员", "群公告", "群精华", "群相册", "群荣誉"])

    @property
    def restore_options(self) -> List[str]:
        return self.config.get("restore_options", ["群名称", "群头像", "群昵称", "群头衔", "群管理", "群相册"])

    def _format_timestamp(self, timestamp):
        """格式化时间戳"""
        if isinstance(timestamp, (int, float)) and timestamp > 0:
            return datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
        return "未知"

    def _format_essence_content(self, raw_content):
        """格式化精华消息内容"""
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

    async def _backup_albums(self, client, group_id: int, latest_data: Dict = None):
        """备份群相册，返回 (albums_list, album_media_map)"""
        albums = []
        album_media_map = {}
        try:
            raw_albums = await client.get_qun_album_list(group_id=str(group_id))
            if isinstance(raw_albums, dict) and raw_albums.get("retcode", 0) != 0:
                raise Exception(f"API 响应异常: {raw_albums}")
            logger.debug(f"API 响应 (get_qun_album_list): {json.dumps(raw_albums, ensure_ascii=False)}")
            if raw_albums:
                logger.info(f"发现 {len(raw_albums)} 个相册，正在备份原图...")
                for album in raw_albums:
                    album_id = album.get("album_id")
                    album_name = album.get("name", album_id)
                    
                    # 精简相册信息
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
                    old_media_list = []
                    if latest_data and "albums" in latest_data and "album_media" in latest_data:
                        old_album_info = next((a for a in latest_data["albums"] if a["album_id"] == album_id), None)
                        old_media_list = latest_data["album_media"].get(album_id, [])
                        if old_album_info and str(old_album_info.get("modify_time")) == str(album.get("modify_time")):
                            media_list = old_media_list
                            if media_list:
                                is_album_updated = False
                                logger.debug(f"相册 {album_name} 修改时间未变，跳过 API 请求，复用上次备份的 {len(media_list)} 个媒体记录。")
                    
                    if is_album_updated:
                        try:
                            result = await client.get_group_album_media_list(group_id=str(group_id), album_id=album_id)
                            if isinstance(result, dict) and result.get("retcode", 0) != 0:
                                raise Exception(f"API 响应异常: {result}")
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
                                media_type = m.get("type") # 0:图片, 1:视频 (基于 log.txt)
                                best_url = ""
                                media_id = ""

                                if media_type == 0: # 图片
                                    img_detail = m.get("image")
                                    if img_detail:
                                        media_id = img_detail.get("lloc")
                                        photo_urls = img_detail.get("photo_url", [])
                                        # 优先选择 spec 1 或 6
                                        for p in photo_urls:
                                            if p.get("spec") in [1, 6]:
                                                best_url = p.get("url", {}).get("url", "")
                                                break
                                        if not best_url and photo_urls:
                                            best_url = photo_urls[0].get("url", {}).get("url", "")
                                        if not best_url:
                                            best_url = img_detail.get("default_url", {}).get("url", "")

                                elif media_type == 1: # 视频
                                    video_detail = m.get("video")
                                    if video_detail:
                                        media_id = video_detail.get("id")
                                        # 优先从 video_url 列表获取
                                        video_urls = video_detail.get("video_url", [])
                                        if video_urls:
                                            best_url = video_urls[0].get("url", {}).get("url", "")
                                        # 备选使用直接的 url 字符串
                                        if not best_url:
                                            best_url = video_detail.get("url")

                                if best_url:
                                    media_list.append({
                                        "media_id": media_id or m.get("id"),
                                        "url": best_url,
                                        "media_type": media_type,
                                        "upload_time": m.get("upload_time")
                                    })
                                else:
                                    logger.warning(f"未能从媒体项提取到有效 URL: {json.dumps(m, ensure_ascii=False)}")
                        except Exception as e:
                            logger.error(f"获取相册 {album_id} 媒体列表失败: {e}")
                            if old_media_list:
                                media_list = old_media_list
                                logger.warning(f"由于 API 请求失败，相册 {album_name} 暂时复用旧备份数据。")
                    
                    album_media_map[album_id] = media_list
                    
                    # 下载/检查本地文件
                    if media_list:
                        album_save_dir = Path(self.plugin_data_dir) / str(group_id) / "albums" / album_name
                        album_save_dir.mkdir(parents=True, exist_ok=True)
                        
                        if is_album_updated:
                            logger.info(f"正在下载相册 {album_name} 中的 {len(media_list)} 个媒体文件...")
                            download_tasks = []
                            for media in media_list:
                                url = media.get("url")
                                media_id = media.get("media_id")
                                if url and media_id:
                                    file_ext = ".jpg" 
                                    if media.get("media_type") == 1:
                                        file_ext = ".mp4"
                                    save_path = album_save_dir / f"{media_id}{file_ext}"
                                    download_tasks.append(self._download_file(url, save_path))
                            if download_tasks:
                                await asyncio.gather(*download_tasks)
                        else:
                            # 仅检查缺失文件并警告，不尝试使用过期 URL 下载
                            missing_count = 0
                            for media in media_list:
                                media_id = media.get("media_id")
                                file_ext = ".jpg" if media.get("media_type") == 0 else ".mp4"
                                if not (album_save_dir / f"{media_id}{file_ext}").exists():
                                    missing_count += 1
                            if missing_count > 0:
                                logger.warning(f"相册 {album_name} 有 {missing_count} 个本地文件缺失，但由于相册未更新且 URL 可能已过期，跳过下载。请尝试在相册有新上传后再备份。")
        except Exception as e:
            logger.error(f"备份群相册失败: {e}")
        return albums, album_media_map

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
            
            yield event.plain_result(f"开始备份群 {group_id} 的数据...")

            # 加载上一次备份的数据用于增量对比
            latest_data = self._get_latest_backup_data(group_id)
            
            # 1. 获取详细信息 (包含基础信息)
            group_detail = {}
            if "群信息" in self.backup_options:
                try:
                    raw_detail = await client.get_group_detail_info(group_id=group_id)
                    logger.debug(f"API 响应 (get_group_detail_info): {json.dumps(raw_detail, ensure_ascii=False)}")
                    
                    # 精简群详细信息
                    essential_detail_keys = [
                        "groupCode", "groupName", "ownerUin", "memberNum", "maxMemberNum", 
                        "groupCreateTime", "activeMemberNum", "groupGrade",
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
                essential_keys = ["user_id", "nickname", "card", "role", "level", "title", "join_time", "last_sent_time"]
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
                    raw_notices = await client._get_group_notice(group_id=group_id)
                    logger.debug(f"API 响应 (_get_group_notice): {json.dumps(raw_notices, ensure_ascii=False)}")
                    
                    # 精简公告信息
                    for n in raw_notices:
                        msg = n.get("message", {})
                        notice_item = {
                            "notice_id": n.get("notice_id"),
                            "sender_id": n.get("sender_id"),
                            "publish_time": n.get("publish_time"),
                            "text": msg.get("text", "")
                        }
                        # 解析图片信息
                        images = msg.get("image") or msg.get("images")
                        if images:
                            if not isinstance(images, list):
                                images = [images]
                            
                            processed_images = []
                            notice_img_dir = Path(self.plugin_data_dir) / str(group_id) / "notices_images"
                            for img in images:
                                if isinstance(img, dict):
                                    img_id = img.get("id")
                                    size = "628"
                                    img_url = f"https://gdynamic.qpic.cn/gdynamic/{img_id}/{size}"
                                    
                                    img["url"] = img_url
                                    # 备份图片到本地
                                    ext = ".jpg" 
                                    local_path = notice_img_dir / f"{img_id}{ext}"
                                    success = await self._download_file(img_url, local_path)
                                    if success:
                                        img["local_path"] = str(local_path.relative_to(Path(self.plugin_data_dir) / str(group_id)))
                                            
                                    processed_images.append(img)
                                else:
                                    processed_images.append(img)
                            notice_item["images"] = processed_images
                        notices.append(notice_item)
                    
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
                except Exception as e:
                    logger.warning(f"获取群公告失败: {e}")
                
            # 4. 获取群精华
            essence = []
            if "群精华" in self.backup_options:
                try:
                    raw_essence = await client.get_essence_msg_list(group_id=group_id)
                    logger.debug(f"API 响应 (get_essence_msg_list): {json.dumps(raw_essence, ensure_ascii=False)}")
                    
                    # 确保 raw_essence 是列表
                    if isinstance(raw_essence, dict) and "data" in raw_essence:
                        raw_essence = raw_essence["data"]
                    
                    if raw_essence and isinstance(raw_essence, list):
                        # 精简群精华并获取发送时间
                        for e in raw_essence:
                            # 处理精华消息中的图片
                            essence_img_dir = Path(self.plugin_data_dir) / str(group_id) / "essence_images"
                            content = e.get("content")
                            if content:
                                if not isinstance(content, list):
                                    content = [content]
                                
                                for seg in content:
                                    if isinstance(seg, dict) and seg.get("type") == "image":
                                        data = seg.get("data", {})
                                        img_id = data.get("file_id") or data.get("file")
                                        img_url = data.get("url")
                                        
                                        # 如果没有 URL 但有 ID，构造抓包格式的 URL
                                        if not img_url and img_id:
                                            img_url = f"https://gdynamic.qpic.cn/gdynamic/{img_id}/628"
                                            data["url"] = img_url
                                        
                                        if img_url:
                                            # 备份图片到本地
                                            ext = ".jpg"
                                            file_name = img_id if img_id else hashlib.md5(img_url.encode()).hexdigest()
                                            local_path = essence_img_dir / f"{file_name}{ext}"
                                            success = await self._download_file(img_url, local_path)
                                            if success:
                                                data["local_path"] = str(local_path.relative_to(Path(self.plugin_data_dir) / str(group_id)))

                            essence.append({
                                "message_id": e.get("message_id"),
                                "sender_id": e.get("sender_id"),
                                "sender_nick": e.get("sender_nick"),
                                "operator_id": e.get("operator_id"),
                                "operator_nick": e.get("operator_nick"),
                                "operator_time": e.get("operator_time"),
                                "content": e.get("content")
                            })
                    
                    # 增量对比群精华
                    if latest_data and "essence" in latest_data:
                        old_essence_map = {e["message_id"]: e for e in latest_data["essence"]}
                        new_essence_map = {e["message_id"]: e for e in essence}
                        
                        deleted_essence = [e for mid, e in old_essence_map.items() if mid not in new_essence_map]
                        if deleted_essence:
                            logger.info(f"检测到已删除的群精华: {deleted_essence}")
                            self._archive_deleted_items(group_id, "essence", deleted_essence)
                            for e in deleted_essence:
                                self._append_log(group_id, "content_changes", {"type": "群精华已删除", "message_id": e["message_id"]})
                except Exception as e:
                    logger.warning(f"获取群精华失败: {e}")
                
            # 5. 获取群荣誉
            honors = {}
            if "群荣誉" in self.backup_options:
                try:
                    honors = await client.get_group_honor_info(group_id=group_id, type="all")
                    logger.debug(f"API 响应 (get_group_honor_info): {json.dumps(honors, ensure_ascii=False)}")
                except Exception as e:
                    logger.warning(f"获取群荣誉失败: {e}")

            # 6. 获取群相册并备份原图
            albums = []
            album_media_map = {}
            if "群相册" in self.backup_options:
                albums, album_media_map = await self._backup_albums(client, group_id, latest_data)

            # 7. 增量对比群相册（处理已删除的图片/相册）
            if "群相册" in self.backup_options and latest_data and "album_media" in latest_data:
                try:
                    old_album_media = latest_data["album_media"]
                    # 查找已删除的相册
                    for old_album_id, old_media_list in old_album_media.items():
                        if old_album_id not in album_media_map:
                            # 整个相册被删了
                            self._append_log(group_id, "content_changes", {"type": "相册已删除", "album_id": old_album_id})
                            old_albums_list = latest_data.get("albums", [])
                            old_album_info = next((a for a in old_albums_list if a["album_id"] == old_album_id), {"album_id": old_album_id, "name": "未知相册"})
                            self._archive_deleted_items(group_id, "albums", [old_album_info])
                            
                            old_name = old_album_info.get("name")
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
                                self._archive_deleted_items(group_id, "media", deleted_media)
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
            
            yield event.plain_result(f"✅ 群 {group_id} 备份成功！")
            
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
    async def group_export(self, event: AstrMessageEvent, args: str = ""):
        """群导出 [群号] [选项...]：导出指定数据。选项可选：群信息、群成员、群公告、群精华、群荣誉、群相册"""
        # 权限检查
        is_admin = event.is_admin()
        user_id = int(event.get_sender_id())
        if not is_admin and (not self.admin_users or user_id not in self.admin_users):
            yield event.plain_result(f"❌ 此指令仅限管理员使用")
            return

        # 参数解析
        parts = event.message_str.split()
        arg_list = parts[1:]
        
        target_group_id = ""
        requested_options = []
        all_possible_options = ["群信息", "群成员", "群公告", "群精华", "群荣誉", "群相册"]
        
        for part in arg_list:
            if part in all_possible_options:
                requested_options.append(part)
            elif part.isdigit():
                target_group_id = part
        
        if not target_group_id:
            target_group_id = event.get_group_id()
        
        if not target_group_id:
            yield event.plain_result("请在群聊中使用此指令，或在指令后跟随群号。")
            return

        group_id = int(target_group_id)
        # 如果用户没填选项，则使用配置中的默认选项，但排除群相册（群相册需显式指定）
        if not requested_options:
            requested_options = [opt for opt in all_possible_options if opt in self.backup_options and opt != "群相册"]
            # 如果配置里没开任何项（或只开了相册），则默认导出除相册外的所有
            if not requested_options:
                requested_options = [opt for opt in all_possible_options if opt != "群相册"]

        try:
            client = event.bot
            logger.info(f"收到导出请求: 群号={group_id}, 选项={requested_options}, 原始消息='{event.message_str}'")
            
            # 加载上一次备份的数据用于异常回退
            latest_data = self._get_latest_backup_data(group_id)
            
            yield event.plain_result(f"正在导出群 {group_id} 的数据: {', '.join(requested_options)}...")

            # --- 处理群相册备份与打包 ---
            zip_base64 = None
            if "群相册" in requested_options:
                # 1. 先执行一次备份
                logger.info(f"正在执行群 {group_id} 的相册导出前备份...")
                await self._backup_albums(client, group_id, latest_data)
                
                # 2. 压缩打包
                album_dir = Path(self.plugin_data_dir) / str(group_id) / "albums"
                deleted_dir = Path(self.plugin_data_dir) / str(group_id) / "logs" / "deleted_items"
                
                # 检查目录是否真的包含文件
                def has_files(directory: Path):
                    if not directory.exists(): return False
                    for _, _, files in os.walk(directory):
                        if files: return True
                    return False

                if has_files(album_dir) or has_files(deleted_dir):
                    logger.info(f"正在压缩群 {group_id} 的备份目录（包含相册和已删除项目）...")
                    zip_buffer = BytesIO()
                    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                        # 打包现有相册
                        if has_files(album_dir):
                            for root, dirs, files in os.walk(album_dir):
                                for file in files:
                                    file_path = Path(root) / file
                                    arcname = file_path.relative_to(album_dir.parent)
                                    zf.write(file_path, arcname)
                        
                        # 打包已删除项目（回收站）
                        if has_files(deleted_dir):
                            for root, dirs, files in os.walk(deleted_dir):
                                for file in files:
                                    file_path = Path(root) / file
                                    # 在压缩包内存放在 "回收站" 目录下
                                    arcname = Path("回收站") / file_path.relative_to(deleted_dir)
                                    zf.write(file_path, arcname)
                    
                    zip_content = zip_buffer.getvalue()
                    if zip_content:
                        zip_base64 = base64.b64encode(zip_content).decode("utf-8")
                else:
                    logger.warning(f"群 {group_id} 的相册目录不存在，跳过压缩。")

            # --- 处理 Excel 导出 ---
            excel_base64 = None
            # 只有当请求了除群相册以外的选项时，才生成 Excel
            excel_options = [opt for opt in requested_options if opt != "群相册"]
            
            # 如果只请求了群相册，则不生成 Excel
            if excel_options:
                output_buffer = BytesIO()
                with pd.ExcelWriter(output_buffer, engine="openpyxl") as writer:
                    # 1. 导出群概况 (群信息)
                    if "群信息" in requested_options:
                        detail = {}
                        try:
                            raw_res = await client.get_group_detail_info(group_id=group_id)
                            if isinstance(raw_res, dict) and raw_res.get("retcode", 0) != 0:
                                raise Exception(f"API 响应异常: {raw_res}")
                            detail = raw_res
                        except Exception as e:
                            logger.warning(f"获取实时群概况失败，尝试使用备份数据: {e}")
                            detail = latest_data.get("group_detail", {})
                            
                        if detail:
                            display_detail = {
                                "群名称": detail.get("groupName"),
                                "群号": detail.get("groupCode"),
                                "群分类": detail.get("groupClassText"),
                                "群主QQ": detail.get("ownerUin"),
                                "成员人数": detail.get("memberNum"),
                                "最大人数": detail.get("maxMemberNum"),
                                "当前活跃人数": detail.get("activeMemberNum"),
                            }
                            detail_list = [{"属性": k, "值": v} for k, v in display_detail.items() if v is not None]
                            pd.DataFrame(detail_list).to_excel(writer, index=False, sheet_name="群概况")

                    # 2. 导出群成员
                    if "群成员" in requested_options:
                        members = []
                        try:
                            raw_res = await client.get_group_member_list(group_id=group_id)
                            if isinstance(raw_res, dict) and raw_res.get("retcode", 0) != 0:
                                raise Exception(f"API 响应异常: {raw_res}")
                            members = raw_res
                        except Exception as e:
                            logger.warning(f"获取实时群成员失败，尝试使用备份数据: {e}")
                            members = latest_data.get("members", [])

                        if members:
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

                    # 3. 导出群公告
                    if "群公告" in requested_options:
                        notices = []
                        try:
                            raw_res = await client._get_group_notice(group_id=group_id)
                            if isinstance(raw_res, dict) and raw_res.get("retcode", 0) != 0:
                                raise Exception(f"API 响应异常: {raw_res}")
                            notices = raw_res
                        except Exception as e:
                            logger.warning(f"获取实时群公告失败，尝试使用备份数据: {e}")
                            notices = latest_data.get("notices", [])

                        if notices:
                            processed_notices = []
                            for n in notices:
                                msg = n.get("message", {})
                                content = msg.get("text", "")
                                
                                # 格式化处理：替换 HTML 实体
                                if content:
                                    content = content.replace("&#10;", "\n").replace("&nbsp;", " ")
                                
                                # 检查并添加图片 URL
                                images = msg.get("image") or msg.get("images")
                                if images:
                                    if not isinstance(images, list):
                                        images = [images]
                                    urls = []
                                    for img in images:
                                        if isinstance(img, dict):
                                            img_id = img.get("id")
                                            url = img.get("url")
                                            if not url and img_id:
                                                # 统一使用抓包格式的 URL
                                                url = f"https://gdynamic.qpic.cn/gdynamic/{img_id}/628"
                                            if url:
                                                urls.append(url)
                                    if urls:
                                        content += "\n图片: " + " | ".join(urls)
                                processed_notices.append({
                                    "发布者": n.get("sender_id"),
                                    "发布时间": self._format_timestamp(n.get("publish_time")),
                                    "内容": content
                                })
                            if processed_notices:
                                pd.DataFrame(processed_notices).to_excel(writer, index=False, sheet_name="群公告")

                    # 4. 导出群精华
                    if "群精华" in requested_options:
                        essence = []
                        try:
                            raw_res = await client.get_essence_msg_list(group_id=group_id)
                            if isinstance(raw_res, dict) and raw_res.get("retcode", 0) != 0:
                                raise Exception(f"API 响应异常: {raw_res}")
                            essence = raw_res
                        except Exception as e:
                            logger.warning(f"获取实时群精华失败，尝试使用备份数据: {e}")
                            essence = latest_data.get("essence", [])

                        if isinstance(essence, dict) and "data" in essence:
                            essence = essence["data"]
                        if essence and isinstance(essence, list):
                            processed_essence = []
                            for e in essence:
                                processed_essence.append({
                                    "发送者": e.get("sender_id"),
                                    "设精时间": self._format_timestamp(e.get("operator_time")),
                                    "内容": self._format_essence_content(e.get("content", [])),
                                    "操作者": e.get("operator_id")
                                })
                            pd.DataFrame(processed_essence).to_excel(writer, index=False, sheet_name="群精华")

                    # 5. 导出群荣誉
                    if "群荣誉" in requested_options:
                        honors = {}
                        try:
                            raw_res = await client.get_group_honor_info(group_id=group_id, type="all")
                            if isinstance(raw_res, dict) and raw_res.get("retcode", 0) != 0:
                                raise Exception(f"API 响应异常: {raw_res}")
                            honors = raw_res
                        except Exception as e:
                            logger.warning(f"获取实时群荣誉失败，尝试使用备份数据: {e}")
                            honors = latest_data.get("honors", {})

                        if honors:
                            honor_list = []
                            honor_type_map = {
                                "current_talkative": "龙王",
                                "talkative_list": "龙王历史获得者",
                                "performer_list": "群聊之火",
                                "legend_list": "群聊炽焰",
                                "emotion_list": "快乐源泉",
                                "strong_newbie_list": "善财福禄寿"
                            }
                            for honor_type, honor_data in honors.items():
                                if honor_type == "group_id": continue
                                type_name = honor_type_map.get(honor_type, honor_type)
                                
                                if isinstance(honor_data, dict) and "user_id" in honor_data:
                                    honor_list.append({
                                        "荣誉类型": type_name, 
                                        "QQ号": honor_data.get("user_id"), 
                                        "昵称": honor_data.get("nickname"),
                                        "描述": honor_data.get("description", "")
                                    })
                                elif isinstance(honor_data, list):
                                    for h in honor_data:
                                        honor_list.append({
                                            "荣誉类型": type_name, 
                                            "QQ号": h.get("user_id"), 
                                            "昵称": h.get("nickname"),
                                            "描述": h.get("description", "")
                                        })
                            if honor_list:
                                pd.DataFrame(honor_list).to_excel(writer, index=False, sheet_name="群荣誉")

                    # 6. 导出群相册列表
                    if "群相册" in requested_options:
                        albums_list = []
                        try:
                            raw_res = await client.get_qun_album_list(group_id=str(group_id))
                            if isinstance(raw_res, dict) and raw_res.get("retcode", 0) != 0:
                                raise Exception(f"API 响应异常: {raw_res}")
                            albums_list = raw_res
                        except Exception as e:
                            logger.warning(f"获取实时群相册列表失败，尝试使用备份数据: {e}")
                            albums_list = latest_data.get("albums", [])

                        if albums_list:
                            processed_albums = []
                            for a in albums_list:
                                processed_albums.append({
                                    "相册名": a.get("name"),
                                    "图片数量": a.get("upload_number"),
                                    "创建者": a.get("creator", {}).get("nick"),
                                    "创建时间": self._format_timestamp(a.get("create_time")),
                                    "修改时间": self._format_timestamp(a.get("modify_time"))
                                })
                            pd.DataFrame(processed_albums).to_excel(writer, index=False, sheet_name="群相册列表")
                
                    # 7. 导出已删除的项目（回收站数据）
                    archive_file = Path(self.plugin_data_dir) / str(group_id) / "logs" / "deleted_items.json"
                    if archive_file.exists():
                        try:
                            with open(archive_file, "r", encoding="utf-8") as f:
                                archive = json.load(f)
                            
                            for item_type, items in archive.items():
                                if not items: continue
                                
                                # 只有当用户请求了对应的主要选项时，才导出对应的已删除项目
                                if item_type == "notices" and "群公告" not in requested_options: continue
                                if item_type == "essence" and "群精华" not in requested_options: continue
                                if item_type in ["albums", "media"] and "群相册" not in requested_options: continue
                                
                                sheet_name_map = {
                                    "notices": "已删除公告",
                                    "essence": "已删除精华",
                                    "albums": "已删除相册",
                                    "media": "已删除媒体"
                                }
                                sheet_name = sheet_name_map.get(item_type, f"已删除_{item_type}")
                                
                                processed_items = []
                                for item in items:
                                    deleted_at = item.get("deleted_at", "未知")
                                    content = item.get("content", {})
                                    
                                    if item_type == "notices":
                                        processed_items.append({
                                            "删除时间": deleted_at,
                                            "发布者": content.get("sender_id"),
                                            "发布时间": self._format_timestamp(content.get("publish_time")),
                                            "内容": content.get("text")
                                        })
                                    elif item_type == "essence":
                                        processed_items.append({
                                            "删除时间": deleted_at,
                                            "发送者": content.get("sender_id"),
                                            "设精时间": self._format_timestamp(content.get("operator_time")),
                                            "内容": self._format_essence_content(content.get("content", []))
                                        })
                                    elif item_type == "albums":
                                        processed_items.append({
                                            "删除时间": deleted_at,
                                            "相册ID": content.get("album_id"),
                                            "相册名": content.get("name")
                                        })
                                    elif item_type == "media":
                                        processed_items.append({
                                            "删除时间": deleted_at,
                                            "媒体ID": content.get("media_id"),
                                            "类型": "图片" if content.get("media_type") == 0 else "视频",
                                            "原始URL": content.get("url")
                                        })
                                    else:
                                        # 通用处理
                                        processed_items.append({
                                            "删除时间": deleted_at,
                                            "原始内容": json.dumps(content, ensure_ascii=False)
                                        })
                                
                                if processed_items:
                                    pd.DataFrame(processed_items).to_excel(writer, index=False, sheet_name=sheet_name)
                        except Exception as e:
                            logger.warning(f"导出已删除项目失败: {e}")

                excel_content = output_buffer.getvalue()
                if excel_content:
                    excel_base64 = base64.b64encode(excel_content).decode("utf-8")

            # --- 发送结果 ---
            if not excel_base64 and not zip_base64:
                yield event.plain_result("❌ 未能导出任何数据。")
                return

            # 发送 Excel
            if excel_base64:
                excel_options = [opt for opt in requested_options if opt != "群相册"]
                if len(excel_options) == 1:
                    type_str = excel_options[0]
                else:
                    type_str = "群数据"
                
                excel_name = f"群{group_id}_{type_str}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                # 获取当前指令发出的环境群号，用于文件上传
                current_context_group_id = event.get_group_id()
                if event.message_obj.type == MessageType.GROUP_MESSAGE and current_context_group_id:
                    await client.upload_group_file(group_id=int(current_context_group_id), file=f"base64://{excel_base64}", name=excel_name)
                else:
                    await client.upload_private_file(user_id=int(event.get_sender_id()), file=f"base64://{excel_base64}", name=excel_name)
            
            # 发送相册 ZIP
            if zip_base64:
                zip_name = f"群{group_id}_群相册_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
                current_context_group_id = event.get_group_id()
                if event.message_obj.type == MessageType.GROUP_MESSAGE and current_context_group_id:
                    await client.upload_group_file(group_id=int(current_context_group_id), file=f"base64://{zip_base64}", name=zip_name)
                else:
                    await client.upload_private_file(user_id=int(event.get_sender_id()), file=f"base64://{zip_base64}", name=zip_name)

            yield event.plain_result(f"✅ 群 {group_id} 数据导出成功，文件已上传。")

        except Exception as e:
            logger.error(f"群导出出错: {e}")
            yield event.plain_result(f"❌ 导出失败: {e}")

    @filter.command("群恢复")
    async def group_restore(self, event: AstrMessageEvent, group_id_arg: str = ""):
        """群恢复 [群号]：将指定群或当前群的备份数据恢复到当前群"""
        # 权限检查
        sender_id = int(event.get_sender_id())
        if self.admin_users and sender_id not in self.admin_users:
            yield event.plain_result("❌ 您没有权限执行此指令。")
            return

        current_group_id = event.get_group_id()
        if not current_group_id:
            yield event.plain_result("❌ 请在群聊中使用此指令。")
            return
        current_group_id = int(current_group_id)

        # 确定备份来源群号
        source_group_id = int(group_id_arg) if group_id_arg and group_id_arg.isdigit() else current_group_id

        try:
            client = event.bot
            yield event.plain_result(f"正在从群 {source_group_id} 的备份恢复数据到当前群...")

            # 1. 加载备份数据
            latest_data = self._get_latest_backup_data(source_group_id)
            if not latest_data:
                yield event.plain_result(f"❌ 未找到群 {source_group_id} 的备份数据。")
                return

            restore_options = self.restore_options
            group_info = latest_data.get("group_detail", {})
            
            # 2. 恢复群名称
            if "群名称" in restore_options:
                new_name = group_info.get("groupName")
                if new_name:
                    logger.info(f"正在恢复群名称: {new_name}")
                    await client.set_group_name(group_id=current_group_id, group_name=new_name)
                    logger.info("群名称恢复完成")
                else:
                    logger.warning("备份数据中未找到群名称，跳过恢复")

            # 3. 恢复群头像
            if "群头像" in restore_options:
                # 尝试从备份目录查找头像文件，优先找 group_avatar.png
                avatar_path = Path(self.plugin_data_dir) / str(source_group_id) / "group_avatar.png"
                if not avatar_path.exists():
                    avatar_path = Path(self.plugin_data_dir) / str(source_group_id) / "avatar.png"
                if not avatar_path.exists():
                    avatar_path = Path(self.plugin_data_dir) / str(source_group_id) / "avatar.jpg"
                
                if avatar_path.exists():
                    logger.info(f"正在恢复群头像: {avatar_path}")
                    await client.set_group_portrait(group_id=current_group_id, file=f"file://{avatar_path.absolute()}")
                    logger.info("群头像恢复完成")
                else:
                    logger.warning(f"未找到备份的群头像文件 (尝试过 group_avatar.png, avatar.png, avatar.jpg): {avatar_path}")

            # 4. 恢复群成员设置 (昵称、头衔、管理员)
            if any(opt in restore_options for opt in ["群昵称", "群头衔", "群管理"]):
                backup_members = latest_data.get("members", [])
                if backup_members:
                    # 获取当前群成员列表
                    current_members_raw = await client.get_group_member_list(group_id=current_group_id)
                    current_member_ids = {m.get("user_id") for m in current_members_raw} if current_members_raw else set()
                    
                    restore_count = 0
                    for bm in backup_members:
                        user_id = bm.get("user_id")
                        if user_id not in current_member_ids:
                            continue
                        
                        # 恢复群昵称 (名片)
                        if "群昵称" in restore_options and "card" in bm:
                            await client.set_group_card(group_id=current_group_id, user_id=user_id, card=bm["card"])
                        
                        # 恢复群头衔
                        if "群头衔" in restore_options and "special_title" in bm:
                            await client.set_group_special_title(group_id=current_group_id, user_id=user_id, special_title=bm["special_title"])
                        
                        # 恢复群管理
                        if "群管理" in restore_options and "role" in bm:
                            is_admin = bm["role"] == "admin"
                            if bm["role"] != "owner":
                                await client.set_group_admin(group_id=current_group_id, user_id=user_id, enable=is_admin)
                        
                        restore_count += 1
                        if restore_count % 10 == 0:
                            logger.info(f"已恢复 {restore_count} 名成员的设置...")

                    logger.info(f"群成员设置恢复完成 (共 {restore_count} 人)")

            # 5. 恢复群相册
            if "群相册" in restore_options:
                backup_albums = latest_data.get("albums", [])
                backup_album_media = latest_data.get("album_media", {})
                
                if backup_albums:
                    # 获取当前群相册列表，用于比对同名相册
                    try:
                        current_albums = await client.get_qun_album_list(group_id=str(current_group_id))
                    except:
                        current_albums = []
                    
                    album_name_to_id = {a.get("name"): a.get("album_id") for a in current_albums}
                    
                    for album in backup_albums:
                        album_name = album.get("name")
                        album_id = album.get("album_id")
                        
                        if album_name not in album_name_to_id:
                            logger.warning(f"当前群不存在相册 '{album_name}'，请先手动创建同名相册。跳过此相册恢复。")
                            continue
                        
                        target_album_id = album_name_to_id[album_name]
                        media_list = backup_album_media.get(album_id, [])
                        
                        if not media_list:
                            continue
                        
                        # 获取目标相册已有的媒体列表，避免重复上传
                        try:
                            target_media_raw = await client.get_group_album_media_list(group_id=str(current_group_id), album_id=target_album_id)
                            
                            existing_media_ids = set()
                            
                            # 如果返回的是字典且包含列表字段，尝试提取
                            media_items = []
                            if isinstance(target_media_raw, list):
                                media_items = target_media_raw
                            elif isinstance(target_media_raw, dict):
                                media_items = target_media_raw.get("media_list", target_media_raw.get("list", []))
                            
                            for m in media_items:
                                # 尝试提取各种可能的 ID
                                mid = m.get("media_id") or m.get("id")
                                if not mid and m.get("image"): mid = m.get("image", {}).get("lloc")
                                if not mid and m.get("video"): mid = m.get("video", {}).get("id")
                                if mid: existing_media_ids.add(str(mid))
                        except Exception as e:
                            logger.error(f"获取相册媒体列表失败: {e}")
                            existing_media_ids = set()

                        # 恢复图片
                        album_path = Path(self.plugin_data_dir) / str(source_group_id) / "albums" / album_name
                        if not album_path.exists():
                            continue
                        
                        upload_count = 0
                        for m in media_list:
                            # 仅支持图片恢复，跳过视频 (media_type == 1)
                            if m.get("media_type") != 0:
                                continue
                                
                            m_id = str(m.get("media_id"))
                            if m_id in existing_media_ids:
                                # logger.debug(f"跳过已存在媒体: {m_id}")
                                continue
                            
                            file_ext = ".jpg" 
                            local_file = album_path / f"{m_id}{file_ext}"
                            
                            if local_file.exists():
                                try:
                                    # 调用上传 API
                                    await client.upload_image_to_qun_album(
                                        group_id=str(current_group_id),
                                        album_id=target_album_id,
                                        album_name=album_name,
                                        file=f"file://{local_file.absolute()}"
                                    )
                                    upload_count += 1
                                    if upload_count % 5 == 0:
                                        logger.info(f"相册 '{album_name}' 已上传 {upload_count} 个文件...")
                                except Exception as e:
                                    logger.error(f"上传文件 {local_file} 到相册失败: {e}")

                        logger.info(f"相册 '{album_name}' 恢复完成 (上传 {upload_count} 个新文件)")

            yield event.plain_result(f"✅ 群数据恢复任务已执行完毕。")

        except Exception as e:
            logger.error(f"群恢复出错: {e}")
            import traceback
            logger.error(traceback.format_exc())
            yield event.plain_result(f"❌ 恢复过程中出现错误: {e}")
