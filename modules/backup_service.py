from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from astrbot.api import logger

async def group_backup_command(plugin, event: AstrMessageEvent, group_id_arg: str = ""):
    """群备份 [群号]：备份当前群或指定群数据到本地 JSON"""
    # 权限检查：Bot 管理员 或 配置项中的管理员
    is_admin = event.is_admin()
    user_id = int(event.get_sender_id())
    if not is_admin and (not plugin.admin_users or user_id not in plugin.admin_users):
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
        latest_data = plugin._get_latest_backup_data(group_id)

        # 1. 获取详细信息 (包含基础信息)
        group_detail = {}
        if "群信息" in plugin.backup_options:
            try:
                raw_detail = await client.get_group_detail_info(group_id=group_id)

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
        if "群头像" in plugin.backup_options:
            try:
                avatar_url = f"http://p.qlogo.cn/gh/{group_id}/{group_id}/640/"
                avatar_dir = Path(plugin.plugin_data_dir) / str(group_id)
                avatar_save_path = avatar_dir / "group_avatar.png"
                temp_avatar_path = avatar_dir / "temp_avatar.png"

                # 先下载到临时文件
                await plugin._download_file(avatar_url, temp_avatar_path, overwrite=True)

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
                            plugin._append_log(group_id, "content_changes", {"type": "群头像更新", "old_avatar": archive_path.name})
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
        if "群成员" in plugin.backup_options:
            raw_members = await client.get_group_member_list(group_id=group_id)
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
                    logger.info(f"检测到新成员进群，数量: {len(joiners)}")
                    for m in joiners:
                        plugin._append_log(group_id, "member_changes", {"type": "入群", "user_id": m["user_id"], "nickname": m.get("nickname")})
                if leavers:
                    logger.info(f"检测到成员退群，数量: {len(leavers)}")
                    for m in leavers:
                        plugin._append_log(group_id, "member_changes", {"type": "退群", "user_id": m["user_id"], "nickname": m.get("nickname")})

        # 3. 获取公告
        notices = []
        if "群公告" in plugin.backup_options:
            try:
                raw_notices = await client._get_group_notice(group_id=group_id)

                # 精简公告信息
                for n in raw_notices:
                    msg = n.get("message", {})
                    settings = n.get("settings", {})
                    notice_item = {
                            "notice_id": n.get("notice_id"),
                            "sender_id": n.get("sender_id"),
                            "publish_time": n.get("publish_time"),
                            "text": msg.get("text", ""),
                            "read_num": n.get("read_num"),
                            "settings": {
                                "is_show_edit_card": settings.get("is_show_edit_card"),
                                "tip_window_type": settings.get("tip_window_type"),
                                "confirm_required": settings.get("confirm_required")
                            }
                        }
                    # 解析图片信息
                    images = msg.get("image") or msg.get("images")
                    if images:
                        if not isinstance(images, list):
                            images = [images]

                        processed_images = []
                        notice_img_dir = Path(plugin.plugin_data_dir) / str(group_id) / "notices_images"
                        for img in images:
                            if isinstance(img, dict):
                                img_id = img.get("id")
                                size = "628"
                                img_url = f"https://gdynamic.qpic.cn/gdynamic/{img_id}/{size}"

                                img["url"] = img_url
                                # 备份图片到本地
                                ext = ".jpg" 
                                local_path = notice_img_dir / f"{img_id}{ext}"
                                success = await plugin._download_file(img_url, local_path)
                                if success:
                                    img["local_path"] = str(local_path.relative_to(Path(plugin.plugin_data_dir) / str(group_id)))

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
                            plugin._append_log(group_id, "content_changes", {"type": "新增公告", "notice_id": n["notice_id"], "text": n["text"]})

                    # 检测删除
                    deleted_notices = [n for nid, n in old_notices_map.items() if nid not in new_notices_map]
                    if deleted_notices:
                        logger.info(f"检测到已删除的群公告，数量: {len(deleted_notices)}")
                        plugin._archive_deleted_items(group_id, "notices", deleted_notices)
                        for n in deleted_notices:
                            plugin._append_log(group_id, "content_changes", {"type": "公告已删除", "notice_id": n["notice_id"]})
            except Exception as e:
                logger.warning(f"获取群公告失败: {e}")

        # 4. 获取群精华
        essence = []
        if "群精华" in plugin.backup_options:
            try:
                raw_essence = await client.get_essence_msg_list(group_id=group_id)

                # 确保 raw_essence 是列表
                if isinstance(raw_essence, dict) and "data" in raw_essence:
                    raw_essence = raw_essence["data"]

                if raw_essence and isinstance(raw_essence, list):
                    # 精简群精华并获取发送时间
                    for e in raw_essence:
                        # 处理精华消息中的图片
                        essence_img_dir = Path(plugin.plugin_data_dir) / str(group_id) / "essence_images"
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
                                        success = await plugin._download_file(img_url, local_path)
                                        if success:
                                            data["local_path"] = str(local_path.relative_to(Path(plugin.plugin_data_dir) / str(group_id)))

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
                        logger.info(f"检测到已删除的群精华，数量: {len(deleted_essence)}")
                        plugin._archive_deleted_items(group_id, "essence", deleted_essence)
                        for e in deleted_essence:
                            plugin._append_log(group_id, "content_changes", {"type": "群精华已删除", "message_id": e["message_id"]})
            except Exception as e:
                logger.warning(f"获取群精华失败: {e}")

        # 5. 获取群荣誉
        honors = {}
        if "群荣誉" in plugin.backup_options:
            try:
                honors = await client.get_group_honor_info(group_id=group_id, type="all")
            except Exception as e:
                logger.warning(f"获取群荣誉失败: {e}")

        # 6. 获取群相册并备份原图
        albums = []
        album_media_map = {}
        if "群相册" in plugin.backup_options:
            albums, album_media_map, albums_backup_ok = await plugin._backup_albums(client, group_id, latest_data)

        # 7. 增量对比群相册（处理已删除的图片/相册）
        if "群相册" in plugin.backup_options and latest_data and "album_media" in latest_data:
            try:
                old_album_media = latest_data["album_media"]
                # 查找已删除的相册
                for old_album_id, old_media_list in old_album_media.items():
                    if old_album_id not in album_media_map:
                        # 整个相册被删了
                        plugin._append_log(group_id, "content_changes", {"type": "相册已删除", "album_id": old_album_id})
                        old_albums_list = latest_data.get("albums", [])
                        old_album_info = next((a for a in old_albums_list if a["album_id"] == old_album_id), {"album_id": old_album_id, "name": "未知相册"})
                        plugin._archive_deleted_items(group_id, "albums", [old_album_info])

                        old_name = old_album_info.get("name")
                        if old_name:
                            src_dir = Path(plugin.plugin_data_dir) / str(group_id) / "albums" / old_name
                            if src_dir.exists():
                                dst_dir = Path(plugin.plugin_data_dir) / str(group_id) / "logs" / "deleted_items" / "albums" / old_name
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
                            plugin._archive_deleted_items(group_id, "media", deleted_media)
                            for m in deleted_media:
                                plugin._append_log(group_id, "content_changes", {
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
                                        src_file = Path(plugin.plugin_data_dir) / str(group_id) / "albums" / album_name / f"{m['media_id']}{ext}"
                                        if src_file.exists():
                                            dst_file = Path(plugin.plugin_data_dir) / str(group_id) / "logs" / "deleted_items" / "albums" / album_name / f"{m['media_id']}{ext}"
                                            dst_file.parent.mkdir(parents=True, exist_ok=True)
                                            logger.debug(f"正在将已删除的媒体文件从 {src_file} 移动到 {dst_file}")
                                            import shutil
                                            shutil.move(str(src_file), str(dst_file))
                                            break
            except Exception as e:
                logger.error(f"处理已删除相册图片时出错: {e}")

        # 准备数据目录
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        group_base_dir = Path(plugin.plugin_data_dir) / str(group_id)
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
            logger.debug(f"成功保存备份快照文件: {save_file_path}")

        # 删除除当前刚创建的备份以外的所有旧快照文件夹
        all_backups = sorted([d for d in group_base_dir.iterdir() if d.is_dir() and d.name.replace("_", "").isdigit()], key=lambda x: x.name)
        for old_backup in all_backups:
            if old_backup.name != timestamp:
                try:
                    import shutil
                    shutil.rmtree(str(old_backup))
                    logger.debug(f"已清理旧备份快照: {old_backup.name}")
                except Exception as e:
                    logger.error(f"清理旧备份失败 {old_backup.name}: {e}")

        yield event.plain_result(f"✅ 群 {group_id} 备份成功！")

    except Exception as e:
        logger.error(f"群备份出错: {e}")
        yield event.plain_result(f"❌ 备份失败: {e}")


async def delete_group_backup_command(plugin, event: AstrMessageEvent, group_id_arg: str = ""):
    """删除群备份 [群号]：物理删除指定群组的所有备份数据"""
    # 权限检查
    is_admin = event.is_admin()
    user_id = int(event.get_sender_id())
    if not is_admin and (not plugin.admin_users or user_id not in plugin.admin_users):
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
        group_dir = Path(plugin.plugin_data_dir) / str(group_id)

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
