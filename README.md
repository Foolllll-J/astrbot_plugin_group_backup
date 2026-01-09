# 📦 QQ群备份插件

![License](https://img.shields.io/badge/license-AGPL--3.0-green?style=flat-square)
![Python](https://img.shields.io/badge/python-3.10+-blue?style=flat-square&logo=python&logoColor=white)
![AstrBot](https://img.shields.io/badge/framework-AstrBot-ff6b6b?style=flat-square)

一款为 [AstrBot](https://github.com/AstrBotDevs/AstrBot) 设计的 QQ 群数据备份插件。支持备份群成员、公告、精华消息、荣誉等数据，并支持导出为 Excel 或压缩包。



## ✨ 功能特性

* **📊 全面备份**: 支持备份群信息、群头像、群成员列表、群公告、群精华消息、群荣誉以及群相册。
* **📥 灵活导出**: 支持将备份数据导出为 Excel 表格，相册数据自动打包为 ZIP 压缩包发送。
* **🔄 数据恢复**: 支持一键恢复群名称、群头像、群成员名片及群相册。
* **⚖️ 权限控制**: 支持 Bot 管理员及插件配置中指定的管理员使用。
* **🛠️ 增量更新**: 自动检测数据变化（如新成员入群、退群、群相册重命名等）并记录日志。



## 📖 使用指南

⚠️ **所有管理指令仅限 Bot 管理员或插件配置的管理员使用**

### 📥 数据备份

**指令**: `/群备份 [群号]`
* 在群聊中直接使用可备份当前群。
* 私聊时需指定群号。

### 📤 数据导出

**指令**: `/群导出 [群号] [选项...]`
* **选项可选**: `群信息`、`群成员`、`群公告`、`群精华`、`群荣誉`、`群相册`。
* 若不指定选项，默认导出除“群相册”外的所有已备份项。
* 若包含“群相册”，插件将打包相册及已删除项目为 ZIP 发送。

### 🔄 数据恢复

**指令**: `/群恢复 [来源群号]`
* 在目标群中使用，将 `来源群号` 的备份数据恢复到当前群。
* **支持恢复**: 群名称、群头像、群成员名片、群相册。


### 🗑️ 删除备份

**指令**: `/删除群备份 [群号]`
* 删除指定群在本地的所有备份数据。



## ⚠️ 注意事项

* **环境限制**: 本插件**仅适用于 NapCat** 环境。
* **API 限制**: 受限于 NapCat API，本插件**无法备份**勾选了“发送给新成员”选项的群公告，以及**无法恢复视频**到群相册。

## ⏰ 定时备份

如果您需要定时自动执行备份任务，可以配合使用 [astrbot_plugin_reminder](https://github.com/Foolllll-J/astrbot_plugin_reminder) 插件。

**配置示例**:
使用 `reminder` 插件的 `/添加任务` 指令：
`/添加任务 每日群备份 0 3 * * * /群备份 123456789`
*(每天凌晨 3 点自动备份群 123456789)*

## 🔗 群文件备份

如果您有备份群文件的需求，可以尝试以下插件：

* 📂 [astrbot_plugin_GroupFS](https://github.com/Foolllll-J/astrbot_plugin_GroupFS): 支持备份群文件到**本地**。
* ☁️ [astrbot_plugin_openlistfile](https://github.com/Foolllll-J/astrbot_plugin_openlistfile): 支持将文件备份到**网盘**。



## 📝 更新日志

* **v0.2**
  * 新增 群恢复 功能
  * 优化 群导出


* **v0.1**
  * 实现基础的备份导出功能



## ❤️ 支持

* [AstrBot 帮助文档](https://astrbot.app)
* 如果您在使用中遇到问题，欢迎提交 [Issue](https://github.com/Foolllll-J/astrbot_plugin_group_backup/issues)。


