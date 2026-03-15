from __future__ import annotations

import asyncio
import json
from pathlib import Path

from astrbot.api import logger
from astrbot.core.platform.message_type import MessageType

from .album_service import sort_backup_album_media

async def group_restore_command(plugin, event: AstrMessageEvent, group_id_arg: str = ""):
    """群恢复 [群号]：将指定群或当前群的备份数据恢复到当前群"""
    # 权限检查
    is_admin = event.is_admin()
    sender_id = int(event.get_sender_id())
    if not is_admin and (not plugin.admin_users or sender_id not in plugin.admin_users):
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
        latest_data = plugin._get_latest_backup_data(source_group_id)
        if not latest_data:
            yield event.plain_result(f"❌ 未找到群 {source_group_id} 的备份数据。")
            return

        restore_options = plugin.restore_options
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
            avatar_path = Path(plugin.plugin_data_dir) / str(source_group_id) / "group_avatar.png"
            if not avatar_path.exists():
                avatar_path = Path(plugin.plugin_data_dir) / str(source_group_id) / "avatar.png"
            if not avatar_path.exists():
                avatar_path = Path(plugin.plugin_data_dir) / str(source_group_id) / "avatar.jpg"

            if avatar_path.exists():
                logger.info(f"正在恢复群头像: {avatar_path}")
                await client.set_group_portrait(group_id=current_group_id, file=f"file://{avatar_path.absolute()}")
                logger.info("群头像恢复完成")
            else:
                logger.warning(f"未找到备份的群头像文件 (尝试过 group_avatar.png, avatar.png, avatar.jpg): {avatar_path}")

        # 4. 恢复群公告
        if "群公告" in restore_options:
            backup_notices = latest_data.get("notices", [])
            if backup_notices:
                # 按发布时间升序恢复，保证时间线顺序
                try:
                    backup_notices = sorted(backup_notices, key=lambda x: x.get("publish_time") or 0)
                except Exception as e:
                    logger.warning(f"群公告排序失败，将按备份原顺序恢复: {e}")

                restore_count = 0
                for n in backup_notices:
                    text = n.get("text", "") or ""
                    if text:
                        text = text.replace("&#10;", "\n").replace("&nbsp;", " ")

                    settings = n.get("settings", {}) or {}
                    is_show_edit_card = settings.get("is_show_edit_card")
                    tip_window_type = settings.get("tip_window_type")
                    confirm_required = settings.get("confirm_required")

                    # 旧版 NapCat 没有 settings 字段，默认视为不需要确认
                    if confirm_required is None:
                        confirm_required = 0

                    image_path = None
                    images = n.get("images") or []
                    if images:
                        first = images[0]
                        if isinstance(first, dict):
                            local_rel = first.get("local_path")
                            if local_rel:
                                abs_path = Path(plugin.plugin_data_dir) / str(source_group_id) / local_rel
                                if abs_path.exists():
                                    image_path = str(abs_path.absolute())

                    params = {
                        "group_id": current_group_id,
                        "content": text
                    }
                    if image_path:
                        params["image"] = image_path
                    if is_show_edit_card is not None:
                        params["is_show_edit_card"] = int(is_show_edit_card)
                    if tip_window_type is not None:
                        params["tip_window_type"] = int(tip_window_type)
                    if confirm_required is not None:
                        params["confirm_required"] = int(confirm_required)

                    try:
                        await client._send_group_notice(**params)
                        restore_count += 1
                    except Exception as e:
                        logger.error(f"恢复群公告失败: {e}")

                logger.info(f"群公告恢复完成 (共发送 {restore_count} 条)")

        # 5. 恢复群成员设置 (昵称、头衔、管理员)
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
                    if restore_count % 50 == 0:
                        logger.debug(f"成员设置恢复进度: {restore_count} 人")
                logger.info(f"群成员设置恢复完成 (共 {restore_count} 人)")

        # 6. 恢复群相册
        if "群相册" in restore_options:
            backup_albums = latest_data.get("albums", [])
            backup_album_media = latest_data.get("album_media", {})

            if backup_albums:
                # 获取当前群相册列表，用于比对同名相册
                try:
                    current_albums = plugin._normalize_album_list_response(await client.get_qun_album_list(group_id=str(current_group_id)))
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
                    media_list = sort_backup_album_media(backup_album_media.get(album_id, []))

                    if not media_list:
                        continue

                    # 获取目标相册已有的媒体列表，避免重复上传
                    try:
                        target_media_raw = await client.get_group_album_media_list(group_id=str(current_group_id), album_id=target_album_id)

                        existing_media_ids = set()

                        # 如果返回的是字典且包含列表字段，尝试提取
                        media_items = []
                        media_items = plugin._normalize_album_media_response(target_media_raw)

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
                    album_path = Path(plugin.plugin_data_dir) / str(source_group_id) / "albums" / album_name
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
                                if upload_count % 20 == 0:
                                    logger.debug(f"相册 '{album_name}' 上传进度: {upload_count} 个文件")
                            except Exception as e:
                                logger.error(f"上传文件 {local_file} 到相册失败: {e}")

                    logger.info(f"相册 '{album_name}' 恢复完成 (上传 {upload_count} 个新文件)")

        yield event.plain_result(f"✅ 群数据恢复任务已执行完毕。")

    except Exception as e:
        logger.error(f"群恢复出错: {e}")
        import traceback
        logger.error(traceback.format_exc())
        yield event.plain_result(f"❌ 恢复过程中出现错误: {e}")


async def group_recall_command(plugin, event: AstrMessageEvent):
    """群友召回 [群等级] [群号] [消息文本] 或 [群号] [群等级] [消息文本]"""
    # 权限检查
    is_admin = event.is_admin()
    sender_id = int(event.get_sender_id())
    if not is_admin and (not plugin.admin_users or sender_id not in plugin.admin_users):
        logger.warning(f"[GroupBackup] 用户 {sender_id} 尝试使用召回指令，但无权限。")
        yield event.plain_result("❌ 您没有权限执行此指令。")
        return

    current_group_id = event.get_group_id()
    if not current_group_id:
        yield event.plain_result("❌ 请在群聊中使用此指令。")
        return
    current_group_id = int(current_group_id)

    parts = event.message_str.strip().split()
    if len(parts) < 3:
        yield event.plain_result("❌ 指令格式错误。用法示例：\n/群友召回 123456789 召回消息\n/群友召回 1 123456789 @123456789 召回消息")
        return

    # parts[0] 是指令名，参数从 parts[1] 开始
    args = parts[1:]

    # 解析参数
    level_limit = None
    source_group_id = None

    # 尝试从前两个参数中提取等级和群号
    arg1 = args[0]
    arg2 = args[1] if len(args) > 1 else ""

    # 处理 arg1
    if arg1.isdigit():
        if len(arg1) <= 3:
            level_limit = int(arg1)
        else:
            source_group_id = int(arg1)

    # 处理 arg2
    if arg2.isdigit():
        if len(arg2) <= 3:
            level_limit = int(arg2)
        else:
            source_group_id = int(arg2)

    # 验证提取结果
    if source_group_id is None:
        logger.error(f"[GroupBackup] 召回指令解析失败：未识别到群号。参数: {args}")
        yield event.plain_result("❌ 未能在指令中识别出有效的群号。")
        return

    # 找到最后一个数字参数的索引，之后的全部视为消息文本
    last_digit_idx = -1
    if args[0].isdigit(): last_digit_idx = 0
    if len(args) > 1 and args[1].isdigit(): last_digit_idx = 1

    full_message_text = " ".join(args[last_digit_idx + 1:])
    if not full_message_text:
        logger.error(f"[GroupBackup] 召回指令解析失败：消息内容为空。参数: {args}")
        yield event.plain_result("❌ 消息内容不能为空。")
        return

    # 解析消息内容，识别 @群号
    import re
    recall_message_chain = [] # 存储要发送的消息链项
    segments = re.split(r'(@\d+)', full_message_text)

    client = event.bot
    for segment in segments:
        if not segment: continue

        if segment.startswith("@") and segment[1:].isdigit():
            # 识别到 @群号，需要发送群名片
            card_group_id = segment[1:]

            # 获取群名片数据
            try:
                res = await client.call_action("ArkShareGroup", group_id=str(card_group_id))
                json_data = res
                if isinstance(res, str):
                    try: json_data = json.loads(res)
                    except: pass

                card_data_str = res if isinstance(res, str) else json.dumps(res, ensure_ascii=False)
                token = ""
                if isinstance(json_data, dict) and "config" in json_data:
                    token = json_data["config"].get("token", "")

                recall_message_chain.append({
                    "type": "json",
                    "data": {
                        "data": card_data_str,
                        "config": {"token": token}
                    }
                })
            except Exception as e:
                logger.error(f"[GroupBackup] 获取群名片 {card_group_id} 失败: {e}")
                recall_message_chain.append({"type": "text", "data": {"text": segment}})
        else:
            # 普通文本
            recall_message_chain.append({"type": "text", "data": {"text": segment}})

    if not recall_message_chain:
        yield event.plain_result("❌ 消息内容解析后为空。")
        return

    logger.info(f"[GroupBackup] 正在执行召回。来源群: {source_group_id}, 目标群: {current_group_id}, 等级限制: {level_limit}")

    # 1. 加载备份数据
    latest_data = plugin._get_latest_backup_data(source_group_id)
    if not latest_data or "members" not in latest_data:
        logger.warning(f"[GroupBackup] 召回失败：未找到来源群 {source_group_id} 的成员备份。")
        yield event.plain_result(f"❌ 未找到群 {source_group_id} 的成员备份数据，无法执行召回。")
        return

    backup_members = latest_data["members"]
    logger.debug(f"[GroupBackup] 从备份中加载了 {len(backup_members)} 名成员。")

    # 2. 获取当前群成员列表
    try:
        current_members_raw = await client.get_group_member_list(group_id=current_group_id)
        current_member_ids = {m.get("user_id") for m in current_members_raw} if current_members_raw else set()
        logger.debug(f"[GroupBackup] 当前群已有 {len(current_member_ids)} 名成员。")
    except Exception as e:
        logger.error(f"[GroupBackup] 获取当前群成员列表失败: {e}")
        yield event.plain_result(f"❌ 获取当前群成员失败，无法执行召回过滤。")
        return

    # 3. 筛选符合条件的成员
    targets = []
    for m in backup_members:
        uid = m.get("user_id")
        if not uid: continue

        # 跳过已在新群的成员
        if uid in current_member_ids:
            continue

        # 等级筛选
        if level_limit is not None:
            m_level = m.get("level")
            try:
                if m_level is not None and int(m_level) < level_limit:
                    continue
            except:
                continue

        targets.append(uid)

    if not targets:
        logger.info(f"[GroupBackup] 筛选完成，没有符合条件的召回目标。")
        yield event.plain_result(f"✅ 筛选完毕，没有符合条件且不在本群的目标成员。")
        return

    logger.info(f"[GroupBackup] 筛选出 {len(targets)} 名目标成员。准备开始发送私聊，间隔: {plugin.recall_interval}s")
    yield event.plain_result(f"🔍 筛选出 {len(targets)} 名目标成员，开始私聊召回...")

    # 4. 异步执行发送任务
    async def send_recall_messages():
        success_count = 0
        fail_count = 0
        logger.info(f"[GroupBackup] 开始后台召回任务。总计: {len(targets)} 人")
        for i, target_uid in enumerate(targets):
            try:
                for msg_item in recall_message_chain:
                    await client.send_private_msg(user_id=target_uid, message=[msg_item])
                    await asyncio.sleep(0.2) # 同一用户的分条消息短延迟，避免乱序

                success_count += 1
            except Exception as e:
                fail_count += 1
                logger.error(f"[GroupBackup] [{i+1}/{len(targets)}] 发送召回消息至 {target_uid} 失败: {e}")

            if i < len(targets) - 1:
                await asyncio.sleep(plugin.recall_interval)

        summary = f"📢 群友召回任务完成！\n成功: {success_count}\n失败: {fail_count}"
        logger.info(f"[GroupBackup] 召回任务结束。成功: {success_count}, 失败: {fail_count}")
        try:
            await client.send_group_msg(group_id=current_group_id, message=summary)
        except Exception as e:
            logger.error(f"[GroupBackup] 发送任务总结消息失败: {e}")

    # 创建后台任务
    asyncio.create_task(send_recall_messages())
