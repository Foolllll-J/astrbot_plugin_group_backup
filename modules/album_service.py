from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path
from typing import Any, Dict, List, Tuple

from astrbot.api import logger


def normalize_album_list_response(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    data = payload.get("data")
    if isinstance(data, dict):
        album_list = data.get("album_list") or data.get("list")
        if isinstance(album_list, list):
            return [item for item in album_list if isinstance(item, dict)]

    album_list = payload.get("album_list") or payload.get("list")
    if isinstance(album_list, list):
        return [item for item in album_list if isinstance(item, dict)]
    return []


def normalize_album_media_response(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("media_list", "media", "m_media", "list"):
            if isinstance(data.get(key), list):
                return [item for item in data[key] if isinstance(item, dict)]
        album_info = data.get("album")
        if isinstance(album_info, dict):
            cover = album_info.get("cover")
            if isinstance(cover, dict) and isinstance(cover.get("image"), dict):
                return [cover]

    for key in ("media_list", "media", "m_media", "list"):
        if isinstance(payload.get(key), list):
            return [item for item in payload[key] if isinstance(item, dict)]

    album_info = payload.get("album")
    if isinstance(album_info, dict):
        cover = album_info.get("cover")
        if isinstance(cover, dict) and isinstance(cover.get("image"), dict):
            return [cover]
    return []


def sort_backup_album_media(media_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    indexed_media: List[Tuple[int, Dict[str, Any]]] = list(enumerate(media_list))

    def sort_key(item: Tuple[int, Dict[str, Any]]) -> Tuple[int, int, int]:
        original_index, media = item

        upload_time = media.get("upload_time")
        try:
            upload_time = int(upload_time) if upload_time is not None else 10**9
        except (TypeError, ValueError):
            upload_time = 10**9

        return upload_time, original_index

    return [media for _, media in sorted(indexed_media, key=sort_key)]


async def backup_albums(
    plugin,
    client,
    group_id: int,
    latest_data: Dict | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]], bool]:
    albums: List[Dict[str, Any]] = []
    album_media_map: Dict[str, List[Dict[str, Any]]] = {}
    backup_ok = True
    try:
        raw_albums = await client.get_qun_album_list(group_id=str(group_id))
        if isinstance(raw_albums, dict) and raw_albums.get("retcode", 0) != 0:
            raise Exception(f"API 响应异常: {raw_albums}")
        raw_albums = normalize_album_list_response(raw_albums)
        if raw_albums:
            logger.info(f"发现 {len(raw_albums)} 个相册，正在备份原图...")
            for album in raw_albums:
                if not isinstance(album, dict):
                    continue
                album_id = album.get("album_id")
                album_name = album.get("name", album_id)

                albums.append(
                    {
                        "album_id": album_id,
                        "name": album_name,
                        "create_time": album.get("create_time"),
                        "modify_time": album.get("modify_time"),
                        "creator_nick": album.get("creator", {}).get("nick"),
                        "upload_number": album.get("upload_number"),
                    }
                )

                if latest_data and "albums" in latest_data:
                    old_albums = {a["album_id"]: a.get("name") for a in latest_data["albums"]}
                    if album_id in old_albums and old_albums[album_id] != album_name:
                        old_name = old_albums[album_id]
                        if old_name:
                            old_path = Path(plugin.plugin_data_dir) / str(group_id) / "albums" / old_name
                            new_path = Path(plugin.plugin_data_dir) / str(group_id) / "albums" / album_name
                            if old_path.exists() and not new_path.exists():
                                logger.info(f"检测到相册改名: {old_name} -> {album_name}，正在重命名文件夹。")
                                try:
                                    shutil.move(str(old_path), str(new_path))
                                    plugin._append_log(
                                        group_id,
                                        "content_changes",
                                        {
                                            "type": "相册已改名",
                                            "album_id": album_id,
                                            "old_name": old_name,
                                            "new_name": album_name,
                                        },
                                    )
                                except Exception as e:
                                    logger.error(f"重命名相册文件夹失败: {e}")

                media_list: List[Dict[str, Any]] = []
                is_album_updated = True
                old_media_list: List[Dict[str, Any]] = []
                if latest_data and "albums" in latest_data and "album_media" in latest_data:
                    old_album_info = next((a for a in latest_data["albums"] if a["album_id"] == album_id), None)
                    old_media_list = latest_data["album_media"].get(album_id, [])
                    if old_album_info and str(old_album_info.get("modify_time")) == str(album.get("modify_time")):
                        media_list = old_media_list
                        if media_list:
                            is_album_updated = False
                            logger.debug(f"相册 {album_name} 修改时间未变，复用上次备份的 {len(media_list)} 个媒体记录。")

                if is_album_updated:
                    try:
                        result = await client.get_group_album_media_list(group_id=str(group_id), album_id=album_id)
                        if isinstance(result, dict) and result.get("retcode", 0) != 0:
                            raise Exception(f"API 响应异常: {result}")

                        raw_media_list = normalize_album_media_response(result)
                        for m in raw_media_list:
                            if not isinstance(m, dict):
                                continue
                            media_type = m.get("type")
                            best_url = ""
                            media_id = ""

                            if media_type == 0:
                                img_detail = m.get("image")
                                if img_detail:
                                    media_id = img_detail.get("lloc")
                                    photo_urls = img_detail.get("photo_url", [])
                                    for p in photo_urls:
                                        if p.get("spec") in [1, 6]:
                                            best_url = p.get("url", {}).get("url", "")
                                            break
                                    if not best_url and photo_urls:
                                        best_url = photo_urls[0].get("url", {}).get("url", "")
                                    if not best_url:
                                        best_url = img_detail.get("default_url", {}).get("url", "")

                            elif media_type == 1:
                                video_detail = m.get("video")
                                if video_detail:
                                    media_id = video_detail.get("id")
                                    video_urls = video_detail.get("video_url", [])
                                    if video_urls:
                                        best_url = video_urls[0].get("url", {}).get("url", "")
                                    if not best_url:
                                        best_url = video_detail.get("url")

                            if best_url:
                                media_list.append(
                                    {
                                        "media_id": media_id or m.get("id"),
                                        "url": best_url,
                                        "media_type": media_type,
                                        "upload_time": m.get("upload_time"),
                                    }
                                )
                            else:
                                logger.warning(f"未能从媒体项提取到有效 URL，字段: {list(m.keys())}")
                    except Exception as e:
                        logger.error(f"获取相册 {album_id} 媒体列表失败: {e}")
                        if old_media_list:
                            media_list = old_media_list
                            logger.warning(f"由于 API 请求失败，相册 {album_name} 暂时复用旧备份数据。")

                media_list = sort_backup_album_media(media_list)
                album_media_map[album_id] = media_list

                if media_list:
                    album_save_dir = Path(plugin.plugin_data_dir) / str(group_id) / "albums" / album_name
                    album_save_dir.mkdir(parents=True, exist_ok=True)

                    if is_album_updated:
                        logger.debug(f"正在下载相册 {album_name} 中的 {len(media_list)} 个媒体文件...")
                        download_tasks = []
                        for media in media_list:
                            url = media.get("url")
                            media_id = media.get("media_id")
                            if url and media_id:
                                file_ext = ".jpg"
                                if media.get("media_type") == 1:
                                    file_ext = ".mp4"
                                save_path = album_save_dir / f"{media_id}{file_ext}"
                                download_tasks.append(plugin._download_file(url, save_path))
                        if download_tasks:
                            await asyncio.gather(*download_tasks)
                    else:
                        missing_count = 0
                        for media in media_list:
                            media_id = media.get("media_id")
                            file_ext = ".jpg" if media.get("media_type") == 0 else ".mp4"
                            if not (album_save_dir / f"{media_id}{file_ext}").exists():
                                missing_count += 1
                        if missing_count > 0:
                            logger.warning(
                                f"相册 {album_name} 有 {missing_count} 个本地文件缺失，但由于相册未更新且 URL 可能已过期，跳过下载。"
                            )
    except Exception as e:
        backup_ok = False
        logger.error(f"备份群相册失败: {e}")
    return albums, album_media_map, backup_ok
