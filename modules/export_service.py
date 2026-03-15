from __future__ import annotations

import base64
import json
import os
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path

import pandas as pd
from astrbot.api import logger
from astrbot.core.platform.message_type import MessageType

async def group_export_command(plugin, event: AstrMessageEvent, args: str = ""):
    """群导出 [群号] [选项...]：导出指定数据。选项可选：群信息、群成员、群公告、群精华、群荣誉、群相册"""
    # 权限检查
    is_admin = event.is_admin()
    user_id = int(event.get_sender_id())
    if not is_admin and (not plugin.admin_users or user_id not in plugin.admin_users):
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
        requested_options = [opt for opt in all_possible_options if opt in plugin.backup_options and opt != "群相册"]
        # 如果配置里没开任何项（或只开了相册），则默认导出除相册外的所有
        if not requested_options:
            requested_options = [opt for opt in all_possible_options if opt != "群相册"]

    try:
        client = event.bot
        logger.info(f"收到导出请求: 群号={group_id}, 选项={requested_options}, 原始消息='{event.message_str}'")

        # 加载上一次备份的数据用于异常回退
        latest_data = plugin._get_latest_backup_data(group_id)

        yield event.plain_result(f"正在导出群 {group_id} 的数据: {', '.join(requested_options)}...")

        # --- 处理群相册备份与打包 ---
        zip_base64 = None
        if "群相册" in requested_options:
            # 1. 先执行一次备份
            logger.info(f"正在执行群 {group_id} 的相册导出前备份...")
            await plugin._backup_albums(client, group_id, latest_data)

            # 2. 压缩打包
            album_dir = Path(plugin.plugin_data_dir) / str(group_id) / "albums"
            deleted_dir = Path(plugin.plugin_data_dir) / str(group_id) / "logs" / "deleted_items"

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
                            for opt, api_key in plugin.field_map.items():
                                val = m.get(api_key, "")
                                if api_key in ["join_time", "last_sent_time"]:
                                    val = plugin._format_timestamp(val)
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

                            if content:
                                content = content.replace("&#10;", "\n").replace("&nbsp;", " ")

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
                                            url = f"https://gdynamic.qpic.cn/gdynamic/{img_id}/628"
                                        if url:
                                            urls.append(url)
                                if urls:
                                    content += "\n图片: " + " | ".join(urls)

                            settings = n.get("settings", {})
                            is_show_edit_card = settings.get("is_show_edit_card")
                            tip_window_type = settings.get("tip_window_type")
                            confirm_required = settings.get("confirm_required")
                            read_num = n.get("read_num")

                            notice_data = {
                                "发布者": n.get("sender_id"),
                                "发布时间": plugin._format_timestamp(n.get("publish_time")),
                                "内容": content
                            }

                            if read_num is not None:
                                notice_data["已读人数"] = read_num
                            if is_show_edit_card is not None:
                                notice_data["引导改名片"] = "是" if is_show_edit_card == 1 else "否"
                            if tip_window_type is not None:
                                notice_data["弹窗展示"] = "是" if tip_window_type == 0 else "否"
                            if confirm_required is not None:
                                notice_data["需要确认"] = "是" if confirm_required == 1 else "否"

                            processed_notices.append(notice_data)
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
                                "设精时间": plugin._format_timestamp(e.get("operator_time")),
                                "内容": plugin._format_essence_content(e.get("content", [])),
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
                        albums_list = plugin._normalize_album_list_response(raw_res)
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
                                "创建时间": plugin._format_timestamp(a.get("create_time")),
                                "修改时间": plugin._format_timestamp(a.get("modify_time"))
                            })
                        pd.DataFrame(processed_albums).to_excel(writer, index=False, sheet_name="群相册列表")

                # 7. 导出已删除的项目（回收站数据）
                archive_file = Path(plugin.plugin_data_dir) / str(group_id) / "logs" / "deleted_items.json"
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
                                        "发布时间": plugin._format_timestamp(content.get("publish_time")),
                                        "内容": content.get("text")
                                    })
                                elif item_type == "essence":
                                    processed_items.append({
                                        "删除时间": deleted_at,
                                        "发送者": content.get("sender_id"),
                                        "设精时间": plugin._format_timestamp(content.get("operator_time")),
                                        "内容": plugin._format_essence_content(content.get("content", []))
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
