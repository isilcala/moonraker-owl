# moonraker-owl

[English](README.md) | 简体中文

Owl Cloud 打印机代理程序，用于连接 Moonraker 驱动的 3D 打印机。此服务将您的 Klipper 打印机连接到 Owl Cloud 平台，实现远程监控、AI 缺陷检测和打印管理。

## 功能特性

- 📡 **实时遥测**: 将打印机状态、温度和打印进度实时推送到 Owl Cloud
- 🎮 **远程控制**: 随时随地暂停、恢复和取消打印
- 📸 **摄像头采集**: 集成摄像头用于 AI 缺陷检测
- 🔄 **自动更新**: 通过 Moonraker Update Manager 无缝更新
- 🔒 **安全可靠**: 基于 JWT 的认证，端到端加密

## 系统要求

- 已安装并运行 **Klipper** 和 **Moonraker**
- **Python 3.10** 或更高版本
- **Linux** 系统 (已在 Raspberry Pi OS、Armbian、MainsailOS 上测试)
- 互联网连接

## 快速开始

### 1. 克隆仓库

```bash
cd ~
git clone git clone https://gitee.com/isilcala/moonraker-owl.git
cd moonraker-owl
```

### 2. 运行安装脚本

```bash
sudo chmod +x ./scripts/install.sh
./scripts/install.sh
```

安装脚本将自动：
- 在 `.venv/` 目录创建 Python 虚拟环境
- 安装 moonraker-owl 及其依赖
- 在 `~/printer_data/config/moonraker-owl.toml` 创建配置文件
- 注册并启用 systemd 服务

### 3. 链接到 Owl Cloud

启动服务前，您必须先链接打印机：

1. 打开 **Owl Web**，进入 **打印机 → 添加打印机**
2. 复制 6 位链接码
3. 运行链接命令：

```bash
~/moonraker-owl/.venv/bin/moonraker-owl --config ~/printer_data/config/moonraker-owl.toml link
```

4. 按提示输入链接码
5. 启动服务：

```bash
sudo systemctl start moonraker-owl
```

### 4. 验证安装

```bash
# 检查服务状态
sudo systemctl status moonraker-owl

# 查看实时日志
journalctl -u moonraker-owl -f

# 查看配置
~/moonraker-owl/.venv/bin/moonraker-owl --config ~/printer_data/config/moonraker-owl.toml show-config
```

## Moonraker Update Manager 配置

将以下内容添加到您的 `moonraker.conf` 以启用自动更新：

```ini
[update_manager moonraker-owl]
type: git_repo
path: ~/moonraker-owl
origin: https://gitee.com/isilcala/moonraker-owl.git
primary_branch: main
virtualenv: ~/moonraker-owl/.venv
requirements: requirements.txt
install_script: scripts/install.sh
is_system_service: True
managed_services: moonraker-owl
```

添加后，重启 Moonraker：

```bash
sudo systemctl restart moonraker
```

## 配置说明

配置文件位于 `~/printer_data/config/moonraker-owl.toml`。

### 主要设置

| 配置节 | 设置项 | 说明 |
|--------|--------|------|
| `[cloud]` | `base_url` | Owl Cloud API 地址 |
| `[cloud]` | `broker_host` | MQTT 服务器地址 |
| `[moonraker]` | `url` | 本地 Moonraker API 地址 |
| `[camera]` | `enabled` | 启用摄像头采集用于 AI 检测 |
| `[camera]` | `snapshot_url` | 摄像头快照 URL |
| `[logging]` | `level` | 日志级别 (DEBUG, INFO, WARNING, ERROR) |

查看 `owl.toml.example` 获取所有可用选项及详细说明。

### 默认环境

Agent 默认配置连接到**测试环境（Staging）**：

| 设置项 | 默认值 |
|--------|--------|
| API 地址 | `https://owl.elencala.com` |
| MQTT 服务器 | `mqtt.owl.elencala.com:8883` (TLS) |
| Web 界面 | `https://owl.elencala.com` |

### 摄像头设置

摄像头捕获默认**零配置**。Agent 会自动从 Moonraker 配置中发现摄像头 - 无需手动设置 URL！

要启用 AI 缺陷检测，只需设置：

```ini
[camera]
enabled = true
```

Agent 将会：
1. 查询 Moonraker 的摄像头 API (`/server/webcams/list`)
2. 自动选择第一个可用的摄像头（或使用 `camera_name` 指定特定摄像头）
3. 将相对 URL（如 `/webcam/snapshot`）解析为绝对 URL
4. 缓存发现的 URL 以提升性能（Moonraker 重连时自动刷新缓存）

**配置优先级：**

| 优先级 | 配置 | 行为 |
|--------|------|------|
| 1 | `snapshot_url = http://...` | 直接使用显式 URL（用于外部摄像头） |
| 2 | `camera_name = bed_cam` | 自动发现，但按名称选择摄像头 |
| 3 | `camera_name = auto`（默认） | 自动发现，使用第一个可用摄像头 |

**配置示例：**

```ini
# 最简单：自动发现第一个可用摄像头
[camera]
enabled = true

# 按名称选择特定摄像头（与 moonraker.conf 中配置的名称一致）
[camera]
enabled = true
camera_name = bed_cam

# 手动 URL，用于不在 Moonraker 配置中的外部/IP 摄像头
[camera]
enabled = true
snapshot_url = http://192.168.1.100:8080/snapshot
```

**故障排查：**

- 如果自动发现失败，请检查摄像头是否已在 `moonraker.conf` 中配置
- 查看已发现的摄像头：`curl http://localhost:7125/server/webcams/list`
- Agent 启动时会在日志中记录发现的 URL（查看 `journalctl -u moonraker-owl`）

## 命令说明

| 命令 | 说明 |
|------|------|
| `moonraker-owl start` | 启动代理程序 (通常通过 systemd 运行) |
| `moonraker-owl link` | 链接打印机到 Owl Cloud |
| `moonraker-owl show-config` | 显示当前配置 |

## 服务管理

```bash
# 启动服务
sudo systemctl start moonraker-owl

# 停止服务
sudo systemctl stop moonraker-owl

# 重启服务
sudo systemctl restart moonraker-owl

# 查看状态
sudo systemctl status moonraker-owl

# 查看日志
journalctl -u moonraker-owl -f

# 开机自启（安装时已配置）
sudo systemctl enable moonraker-owl

# 禁用开机自启
sudo systemctl disable moonraker-owl
```

## 卸载

要卸载 moonraker-owl：

```bash
cd ~/moonraker-owl
./scripts/uninstall.sh
```

选项：
- `--keep-config`: 保留配置和凭证，便于重新安装
- `--all`: 无需确认，删除所有内容

卸载后完全删除源代码：

```bash
rm -rf ~/moonraker-owl
```

## 故障排除

### 服务无法启动

1. 检查是否已链接：
   ```bash
   cat ~/.moonraker-owl/credentials.json
   ```
   如果文件不存在，请先运行链接命令。

2. 查看日志：
   ```bash
   journalctl -u moonraker-owl -n 50 --no-pager
   ```

### 无法连接 Moonraker

1. 验证 Moonraker 是否运行：
   ```bash
   curl http://127.0.0.1:7125/server/info
   ```

2. 检查配置中的 `url` 设置。

### 摄像头不工作

1. 直接测试快照 URL：
   ```bash
   curl -o /tmp/test.jpg http://localhost/webcam/?action=snapshot
   ```

2. 确认配置中 `camera.enabled = true`。

## 文件位置

| 文件 | 路径 |
|------|------|
| 源代码 | `~/moonraker-owl/` |
| 虚拟环境 | `~/moonraker-owl/.venv/` |
| 配置文件 | `~/printer_data/config/moonraker-owl.toml` |
| 日志文件 | `~/printer_data/logs/moonraker-owl.log` |
| 凭证文件 | `~/.moonraker-owl/credentials.json` |
| 云端配置缓存 | `~/.moonraker-owl/cloud-config.json` |
| 服务文件 | `/etc/systemd/system/moonraker-owl.service` |

## 开发

用于开发和测试：

```bash
cd ~/moonraker-owl

# 创建开发环境
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# 运行测试
pytest

# 使用调试日志运行
moonraker-owl --config owl.toml start
```

## 支持

- **文档**: https://docs.owl.dev
- **问题反馈**: https://github.com/project-owl/agent/issues
- **社区**: https://discord.gg/owl-3dprint

## 许可证

MIT License - 详见 LICENSE 文件。
