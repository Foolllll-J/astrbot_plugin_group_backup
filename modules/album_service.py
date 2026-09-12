from __future__ import annotations

import asyncio
import shutil
from pathlib import Path
from typing import Any, Dict, List, Tuple

from astrbot.api import logger


def _first_value(item: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = item.get(key)
        if value is not None and value != "":
            return value
    return None


def _nested_url(item: Any) -> str:
    if isinstance(item, str):
        return item
    if not isinstance(item, dict):
        return ""
    url_info = item.get("url")
    if isinstance(url_info, dict):
        return url_info.get("url", "")
    if isinstance(url_info, str):
        return url_info
    return ""


def _normalize_album_item(item: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(item)
    aliases = {
        "album_id": ("id",),
        "create_time": ("createTime",),
        "modify_time": ("modifyTime",),
        "last_upload_time": ("lastUploadTime",),
        "upload_number": ("uploadNumber", "picNum"),
    }
    for target, sources in aliases.items():
        if normalized.get(target) is None:
            value = _first_value(normalized, *sources)
            if value is not None:
                normalized[target] = value
    return normalized


def normalize_album_list_response(payload: Any) -> List[Dict[str, Any]]:
    containers: list[Any] = [payload]
    if isinstance(payload, dict) and isinstance(payload.get("data"), (dict, list)):
        containers.append(payload["data"])

    for container in containers:
        if isinstance(container, list):
            return [
                _normalize_album_item(item)
                for item in container
                if isinstance(item, dict)
            ]
        if isinstance(container, dict):
            for key in ("album_list", "albumList", "list"):
                album_list = container.get(key)
                if isinstance(album_list, list):
                    return [
                        _normalize_album_item(item)
                        for item in album_list
                        if isinstance(item, dict)
                    ]
    return []


def normalize_album_media_response(payload: Any) -> List[Dict[str, Any]]:
    containers: list[Any] = [payload]
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        containers.append(payload["data"])
    for container in containers:
        if isinstance(container, dict):
            for key in ("media_list", "mediaList"):
                val = container.get(key)
                if isinstance(val, list):
                    return [item for item in val if isinstance(item, dict)]
    return []


async def get_album_media_list(
    plugin,
    client,
    group_id: int | str,
    album_id: int | str,
    attach_info: str | None,
):
    if getattr(plugin, "_backend_type", "napcat") == "snowluma":
        api = getattr(client, "api", None)
        if api is None:
            raise RuntimeError("snowluma client.api 不可用，无法获取群相册媒体")
        return await api.call_action(
            "get_group_album_media_list",
            group_id=int(group_id),
            album_id=str(album_id),
            attach_info=attach_info or "",
        )
    return await client.get_group_album_media_list(
        group_id=str(group_id),
        album_id=album_id,
        attach_info=attach_info,
    )


def _album_change_marker(album: Dict[str, Any]) -> tuple[str, str]:
    timestamp = _first_value(
        album,
        "modify_time",
        "modifyTime",
        "last_upload_time",
        "lastUploadTime",
    )
    count = _first_value(
        album,
        "upload_number",
        "uploadNumber",
        "picNum",
    )
    return str(timestamp) if timestamp is not None else "", str(count) if count is not None else ""


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
        raw_albums = await plugin._get_group_album_list(client, group_id)
        if isinstance(raw_albums, dict) and raw_albums.get("retcode", 0) != 0:
            raise Exception(f"API 响应异常: {raw_albums}")
        raw_albums = normalize_album_list_response(raw_albums)
        if raw_albums:
            logger.info(f"发现 {len(raw_albums)} 个相册，正在备份原图...")
            for album in raw_albums:
                if not isinstance(album, dict):
                    continue
                album_id = album.get("album_id")
                album_name = album.get("name") or album.get("album_name") or album_id

                albums.append(
                    {
                        "album_id": album_id,
                        "name": album_name,
                        "create_time": _first_value(
                            album, "create_time", "createTime"
                        ),
                        "modify_time": _first_value(
                            album, "modify_time", "modifyTime"
                        ),
                        "last_upload_time": _first_value(
                            album, "last_upload_time", "lastUploadTime"
                        ),
                        "creator_nick": (
                            album.get("creator", {}).get("nick")
                            if isinstance(album.get("creator"), dict)
                            else album.get("createnickname")
                        ),
                        "upload_number": _first_value(
                            album, "upload_number", "uploadNumber", "picNum"
                        ),
                    }
                )

                if latest_data and "albums" in latest_data:
                    old_albums = {
                        a["album_id"]: a.get("name") for a in latest_data["albums"]
                    }
                    if album_id in old_albums and old_albums[album_id] != album_name:
                        old_name = old_albums[album_id]
                        if old_name:
                            old_path = (
                                Path(plugin.plugin_data_dir)
                                / str(group_id)
                                / "albums"
                                / old_name
                            )
                            new_path = (
                                Path(plugin.plugin_data_dir)
                                / str(group_id)
                                / "albums"
                                / album_name
                            )
                            if old_path.exists() and not new_path.exists():
                                logger.info(
                                    f"检测到相册改名: {old_name} -> {album_name}，正在重命名文件夹。"
                                )
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
                is_album_updated = False
                old_media_list: List[Dict[str, Any]] = []

                current_change_marker = _album_change_marker(album)

                if (
                    latest_data
                    and "albums" in latest_data
                    and "album_media" in latest_data
                ):
                    # 找到旧备份中该相册的信息
                    old_album_info = next(
                        (a for a in latest_data["albums"] if a["album_id"] == album_id),
                        None,
                    )
                    old_media_list = latest_data["album_media"].get(album_id, [])

                    # 只有当旧信息存在且变化标记一致时，才复用旧数据
                    if (
                        old_album_info
                        and _album_change_marker(old_album_info)
                        == current_change_marker
                    ):
                        media_list = old_media_list
                        is_album_updated = False
                        logger.debug(f"相册 {album_name} 变化标记未变，复用本地记录。")
                    else:
                        is_album_updated = True
                else:
                    is_album_updated = True

                if is_album_updated:
                    media_list = []
                    try:
                        # 分页获取相册媒体列表
                        raw_media_list = []
                        attach_info = None
                        _page = 0
                        _max_pages = 100
                        while True:
                            _page += 1
                            if _page > _max_pages:
                                logger.warning(
                                    f"相册 {album_name} 分页超过 {_max_pages} 页，强制终止"
                                )
                                break
                            result = await get_album_media_list(
                                plugin,
                                client,
                                group_id,
                                album_id,
                                attach_info,
                            )
                            if (
                                isinstance(result, dict)
                                and result.get("retcode", 0) != 0
                            ):
                                raise Exception(f"API 响应异常: {result}")

                            page_media = normalize_album_media_response(result)
                            raw_media_list.extend(page_media)

                            pagination = result
                            if isinstance(result, dict) and isinstance(
                                result.get("data"), dict
                            ):
                                pagination = result["data"]
                            next_has_more = (
                                pagination.get("next_has_more")
                                if isinstance(pagination, dict)
                                else None
                            )
                            if next_has_more is None:
                                next_has_more = (
                                    pagination.get("has_more")
                                    if isinstance(pagination, dict)
                                    else None
                                )
                            next_attach_info = (
                                pagination.get("next_attach_info")
                                if isinstance(pagination, dict)
                                else None
                            )
                            if next_attach_info is None:
                                next_attach_info = (
                                    pagination.get("attach_info")
                                    if isinstance(pagination, dict)
                                    else None
                                )
                            if next_attach_info is None:
                                next_attach_info = (
                                    pagination.get("nextAttachInfo")
                                    if isinstance(pagination, dict)
                                    else None
                                )
                            if next_has_more is not None:
                                if not next_has_more:
                                    break
                            elif not next_attach_info:
                                break
                            if next_attach_info == attach_info:
                                logger.warning(
                                    f"相册 {album_name} attach_info 未变化，终止分页防止死循环"
                                )
                                break
                            attach_info = next_attach_info
                            if not attach_info:
                                break

                        url_extract_fail = 0
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
                                    photo_urls = _first_value(
                                        img_detail, "photo_url", "photoUrls"
                                    ) or []
                                    for p in photo_urls:
                                        if not isinstance(p, dict):
                                            continue
                                        if p.get("spec") in [1, 6]:
                                            best_url = _nested_url(p)
                                            break
                                    if not best_url and photo_urls:
                                        best_url = _nested_url(photo_urls[0])
                                    if not best_url:
                                        default_url = _first_value(
                                            img_detail, "default_url", "defaultUrl"
                                        )
                                        best_url = _nested_url(default_url)

                            elif media_type == 1:
                                video_detail = m.get("video")
                                if video_detail:
                                    media_id = video_detail.get("id")
                                    video_urls = _first_value(
                                        video_detail, "video_url", "videoUrl"
                                    ) or []
                                    if video_urls:
                                        best_url = _nested_url(video_urls[0])
                                    if not best_url:
                                        best_url = video_detail.get("url")

                            if best_url:
                                media_list.append(
                                    {
                                        "media_id": media_id or m.get("id"),
                                        "url": best_url,
                                        "media_type": media_type,
                                        "upload_time": _first_value(
                                            m, "upload_time", "uploadTime"
                                        ),
                                    }
                                )
                            else:
                                url_extract_fail += 1
                        if url_extract_fail:
                            logger.warning(
                                f"相册 {album_name} 中有 {url_extract_fail} 个媒体项无法提取 URL"
                            )
                    except Exception as e:
                        logger.error(f"获取相册 {album_id} 媒体列表失败: {e}")
                        if old_media_list:
                            media_list = old_media_list
                            logger.warning(
                                f"由于 API 请求失败，相册 {album_name} 暂时复用旧备份数据。"
                            )

                media_list = sort_backup_album_media(media_list)
                album_media_map[album_id] = media_list

                if media_list:
                    album_save_dir = (
                        Path(plugin.plugin_data_dir)
                        / str(group_id)
                        / "albums"
                        / album_name
                    )
                    album_save_dir.mkdir(parents=True, exist_ok=True)

                    if is_album_updated:
                        new_media = []
                        existing = 0
                        for media in media_list:
                            url = media.get("url")
                            media_id = media.get("media_id")
                            if url and media_id:
                                file_ext = ".jpg"
                                if media.get("media_type") == 1:
                                    file_ext = ".mp4"
                                save_path = album_save_dir / f"{media_id}{file_ext}"
                                if save_path.exists():
                                    existing += 1
                                    continue
                                new_media.append((url, save_path))
                        if new_media:
                            logger.debug(
                                f"相册 {album_name} 待下载 {len(new_media)} 个新媒体文件"
                                f"{f'（跳过 {existing} 个已存在）' if existing else ''}..."
                            )
                            download_tasks = [
                                plugin._download_file(url, p) for url, p in new_media
                            ]
                            results = await asyncio.gather(*download_tasks)
                            success = sum(1 for r in results if r)
                            failed = len(results) - success
                            logger.info(
                                f"相册 {album_name} 下载完成: 成功 {success}, 失败 {failed}"
                            )
                        else:
                            logger.debug(
                                f"相册 {album_name} 无新增媒体文件，跳过下载{'（已存在 ' + str(existing) + ' 个）' if existing else ''}。"
                            )
                    else:
                        missing_count = 0
                        for media in media_list:
                            media_id = media.get("media_id")
                            file_ext = (
                                ".jpg" if media.get("media_type") == 0 else ".mp4"
                            )
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
