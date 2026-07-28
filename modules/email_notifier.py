from __future__ import annotations

import asyncio
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any

BATCH_SIZE = 30


@dataclass
class SmtpConfig:
    host: str = ""
    port: int = 465
    use_tls: bool = True
    user: str = ""
    password: str = ""
    sender: str = ""


def _send_email_sync(
    config: SmtpConfig, to_addrs: list[str], subject: str, body: str
) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = config.sender
    msg["To"] = config.sender
    msg.set_content(body, charset="utf-8")

    if config.use_tls:
        with smtplib.SMTP_SSL(config.host, config.port, timeout=15) as server:
            if config.user:
                server.login(config.user, config.password)
            server.send_message(msg, to_addrs=to_addrs)
    else:
        with smtplib.SMTP(config.host, config.port, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            if config.user:
                server.login(config.user, config.password)
            server.send_message(msg, to_addrs=to_addrs)


async def send_recall_emails(
    config: SmtpConfig,
    target_uids: list[int],
    recall_text: str,
    old_group_name: str,
    old_group_id: int,
    new_group_name: str,
    new_group_id: int,
    logger: Any,
) -> tuple[int, int]:
    if not config or not config.host or not target_uids:
        return 0, len(target_uids)

    addrs = [f"{uid}@qq.com" for uid in target_uids]
    batches = [addrs[i : i + BATCH_SIZE] for i in range(0, len(addrs), BATCH_SIZE)]

    subject = f"[群友召回] 邀请您加入新群 {new_group_name}({new_group_id})"
    body = (
        f"您曾所在的群 {old_group_name}({old_group_id}) 的成员正在"
        f"新群 {new_group_name}({new_group_id}) 重聚。\n\n"
        f"来自新群的留言：\n{recall_text}"
    )

    success = 0
    failed = 0
    for batch in batches:
        try:
            await asyncio.to_thread(_send_email_sync, config, batch, subject, body)
            success += len(batch)
        except smtplib.SMTPException as exc:
            failed += len(batch)
            logger.error(f"[group_backup] 邮件召回 SMTP 异常: {exc}")
        except OSError as exc:
            failed += len(batch)
            logger.error(f"[group_backup] 邮件召回网络异常: {exc}")
        except Exception as exc:
            failed += len(batch)
            logger.error(f"[group_backup] 邮件召回异常: {exc}")
        if len(batches) > 1:
            await asyncio.sleep(5)

    return success, failed
