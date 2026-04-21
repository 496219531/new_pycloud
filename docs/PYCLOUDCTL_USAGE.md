# `pycloudctl` 使用手册

`pycloudctl` 是本项目自带的本地控制脚本，用来管理一套本地 `PyCloud` 运行环境。

它当前负责的对象主要是：

1. `controlplane`
2. `job-orchestrator`
3. `node-1`
4. `node-2`
5. 本地运行目录下的 `logs/`、`pids/`
6. `code_cache/` 下的离线 GC

如果你已经把项目安装成包，入口命令是：

```bash
pycloudctl --help
```

如果你是在仓库里直接开发，也可以用仓库自带包装脚本：

```bash
./scripts/start_services.sh --help
```

这个脚本会自动：

1. 把 `src/` 加进 `PYTHONPATH`
2. 把仓库根目录设为默认 `PYCLOUD_HOME`
3. 转发参数给 `python -m pycloud_parallel.controlplane.ctl`

## 1. 命令总览

当前支持的子命令：

```text
start
start-infocenter
start-gateway
start-controlplane
start-job-orchestrator
start-node
stop
stop-node
restart
status
doctor
gc
```

完整帮助：

```bash
pycloudctl --help
```

容易混淆的一点：

1. `start --help` 看起来像“没参数”
2. 但其实 `start` 能用 `--runtime-root`、端口、worker 容量这些配置
3. 只是它们属于全局参数，不属于 `start` 的局部参数

典型输出要点：

1. 支持全局参数：
   - `--runtime-root`
   - `--controlplane-host`
   - `--controlplane-port`
   - `--node1-host`
   - `--node1-port`
   - `--node1-http-host`
   - `--node1-http`
   - `--node1-http-port`
   - `--node2-host`
   - `--node2-port`
   - `--node2-http-host`
   - `--node2-http`
   - `--node2-http-port`
   - `--node-worker-capacity`
2. 子命令：
   - `start`
   - `start-infocenter`
   - `start-gateway`
   - `start-controlplane`
   - `start-job-orchestrator`
   - `start-node`
   - `stop`
   - `stop-node`
   - `restart`
   - `status`
   - `doctor`
   - `gc`

注意：

1. 这些全局参数要写在子命令前面
2. 例如应写成 `pycloudctl --runtime-root /tmp/pycloud start`
3. 不要写成 `pycloudctl start --runtime-root /tmp/pycloud`
4. 同理，端口和 `--node-worker-capacity` 也要放在 `start` 前面

## 2. 运行目录与默认值

`pycloudctl` 会先决定一个运行根目录 `runtime-root`，优先级如下：

1. `--runtime-root`
2. 环境变量 `PYCLOUD_HOME`
3. 当前工作目录

在这个目录下面，默认会用到：

```text
<runtime-root>/
  logs/
    controlplane.log
    job-orchestrator.log
    infocenter.log
    gateway.log
    node-1.log
    node-2.log
  pids/
    controlplane.pid
    job-orchestrator.pid
    infocenter.pid
    gateway.pid
    node-1.pid
    node-2.pid
  code_cache/
    ...
```

默认端口：

1. `controlplane`: `50051`
2. `job-orchestrator`: `50053`
3. `node-1 gRPC`: `50061`
4. `node-1 service HTTP`: `18081`
5. `node-2 gRPC`: `50062`
6. `node-2 service HTTP`: `18082`

默认 host：

1. `pycloudctl start` / `start-infocenter` / `start-gateway` / `start-controlplane` / `start-job-orchestrator` / `start-node`
2. 如果没有显式传 host，都会自动探测本机可达 IP
3. 不再默认固定成 `127.0.0.1`
4. 如果你只想本机回环监听，可以直接加 `--local`

`node-worker-capacity` 默认会按 `CPU 核数 / 2` 自动计算，最少为 `1`。

## 3. `start`

用途：

1. 启动一套本地 `controlplane + job-orchestrator + node-1 + node-2`
2. 启动前会先尝试停止当前 `runtime-root` 下记录的旧进程

最常用：

```bash
pycloudctl start
```

开发仓库里：

```bash
./scripts/start_services.sh start
```

自定义运行目录：

```bash
pycloudctl --runtime-root /tmp/pycloud-dev start
```

强制整套服务只监听本机回环地址：

```bash
pycloudctl --local start
```

如果你想让主进程直接把报错打到当前终端/窗口，而不是只看日志文件：

```bash
pycloudctl start --debug
```

`--local` 和 `--debug` 可以一起用：

```bash
pycloudctl --local start --debug
```

说明：

1. `--debug` 会把主进程日志级别切到 `DEBUG`
2. 主进程 stdout/stderr 会直连当前控制台/窗口
3. 这主要用于排查启动失败、注册失败、控制面报错
4. 默认行为不变；不加 `--debug` 时仍按原来的后台/log 文件方式运行

自定义端口：

```bash
pycloudctl \
  --controlplane-port 51051 \
  --job-orchestrator-port 51053 \
  --node1-port 51061 \
  --node1-http-port 18181 \
  --node2-port 51062 \
  --node2-http-port 18182 \
  start
```

这里 `--node1-http` 和 `--node1-http-port` 是同一参数的两个别名；`node2` 同理。

兼容说明：

1. CLI 也接受历史手误别名 `--dubug`
2. 但文档和推荐写法统一使用 `--debug`

如果还想指定 host，也可以直接写：

```bash
pycloudctl \
  --controlplane-host 127.0.0.1 \
  --controlplane-port 51051 \
  --job-orchestrator-host 127.0.0.1 \
  --job-orchestrator-port 51053 \
  --node1-host 0.0.0.0 \
  --node1-port 51061 \
  --node1-http-host 127.0.0.1 \
  --node1-http-port 18181 \
  --node2-host 0.0.0.0 \
  --node2-port 51062 \
  --node2-http-host 127.0.0.1 \
  --node2-http-port 18182 \
  start
```

固定每个 node 的 worker 容量：

```bash
pycloudctl --node-worker-capacity 8 start
```

适合 CI / 临时环境的完整示例：

```bash
pycloudctl \
  --runtime-root /tmp/pycloud-ci \
  --controlplane-port 51051 \
  --job-orchestrator-port 51053 \
  --node1-port 51061 \
  --node1-http-port 18181 \
  --node2-port 51062 \
  --node2-http-port 18182 \
  --node-worker-capacity 4 \
  start
```

启动成功后通常会看到：

1. `ControlPlane` 地址
2. `JobQueue / job-orchestrator` 地址
3. `Node-1` 地址
4. `Node-2` 地址
5. `Logs` 路径
6. `PIDs` 路径

## 4. 单独起各角色

现在 `pycloudctl` 也支持按角色单独启动。

推荐顺序：

1. 想要统一的 `logs/`、`pids/`、`stop`、`status` 管理，优先用 `pycloudctl`
2. 想直接控制最底层 server 参数，再用 `pycloud-control`

先看 `pycloudctl` 入口：

```bash
pycloudctl --help
```

底层 server 入口仍然保留：

```bash
pycloud-control --help
```

或者在仓库里直接：

```bash
PYTHONPATH=src python -m pycloud_parallel.controlplane.server --help
```

当前支持的角色：

1. `infocenter`
2. `gateway`
3. `controlplane`
4. `job-orchestrator`
5. `nodecontrol`

先说明一个容易混淆的点：

1. `controlplane`
   - 是一体化入口
   - 内部已经带了 `InfoCenter + Gateway + JobQueue`
2. `infocenter`
   - 是单独的注册/查询面
   - 不带独立 Gateway 路由代理
3. `gateway`
   - 是单独的 HTTP Gateway
   - 需要连到已有 `infocenter`
4. `job-orchestrator`
   - 是独立的大任务排队入口
   - 需要连到已有 `infocenter`
5. `nodecontrol`
   - 是单独节点进程
   - 它自己还会带一个 node 本地的 `service HTTP`

也就是说：

1. 想最省事，起 `controlplane` 就够了
2. 想拆成“注册中心 + HTTP 网关 + 大任务调度”，就起 `infocenter + gateway + job-orchestrator`
3. 想接入执行节点，就再起一个或多个 `nodecontrol`

### 4.1 用 `pycloudctl` 单独起 `infocenter`

```bash
pycloudctl start-infocenter
```

自定义 bind：

```bash
pycloudctl start-infocenter --bind 0.0.0.0:51051
```

### 4.2 用 `pycloudctl` 单独起 HTTP Gateway

如果你说的“单独起 http”，通常指的是这个：

```bash
pycloudctl start-gateway --infocenter-addr 127.0.0.1:50051
```

这里 `--infocenter-addr` 必填；`pycloudctl` 不会再偷偷默认连本地 `127.0.0.1:50051`。

自定义 bind：

```bash
pycloudctl start-gateway --bind 0.0.0.0:50052 --infocenter-addr 127.0.0.1:50051
```

### 4.3 用 `pycloudctl` 单独起 `controlplane`

```bash
pycloudctl start-controlplane
```

自定义 bind：

```bash
pycloudctl start-controlplane --bind 0.0.0.0:51051
```

### 4.3.1 通过 `--env KEY=VALUE` 透传运行时限制

如果你要调整 inline / DataRef / gRPC 大小限制，可以直接把环境变量透传给 `pycloudctl` 启动的子进程：

```bash
pycloudctl start-controlplane \
  --env PYCLOUD_INLINE_PAYLOAD_SOFT_LIMIT_BYTES=1048576 \
  --env PYCLOUD_GRPC_MAX_SEND_MESSAGE_LENGTH_BYTES=16777216 \
  --env PYCLOUD_GRPC_MAX_RECEIVE_MESSAGE_LENGTH_BYTES=16777216
```

这个参数同样适用于：

```bash
pycloudctl start
pycloudctl restart
pycloudctl start-gateway
pycloudctl start-job-orchestrator
pycloudctl start-node
pycloudctl start-infocenter
```

### 4.4 用 `pycloudctl` 单独起 `job-orchestrator`

```bash
pycloudctl start-job-orchestrator --infocenter-addr 127.0.0.1:50051
```

自定义 bind：

```bash
pycloudctl start-job-orchestrator --bind 0.0.0.0:50053 --infocenter-addr 127.0.0.1:50051
```

### 4.5 用 `pycloudctl` 单独起 `nodecontrol`

```bash
pycloudctl start-node --node-id node-1 --infocenter-addr 127.0.0.1:50051
```

更常见的完整写法：

```bash
pycloudctl start-node \
  --node-id node-1 \
  --bind 192.168.1.23:50061 \
  --service-http-bind 192.168.1.23:18081 \
  --infocenter-addr 127.0.0.1:50051 \
  --advertise-addr 192.168.1.23:50061 \
  --worker-capacity 8 \
  --queue-capacity 1000
```

如果你更习惯按“端口参数”来写，也可以直接用：

```bash
pycloudctl start-node \
  --node-id node-1 \
  --node-port 50061 \
  --service-http-port 18081 \
  --infocenter-addr 127.0.0.1:50051
```

可选地再配这些别名：

1. `--node-host`
2. `--service-http-host`
3. `--advertise-host`
4. `--advertise-port`

这些参数会和原有的 `--bind` / `--service-http-bind` / `--advertise-addr` 合并，旧写法仍然可用。

默认情况下，如果你没显式指定 host，`pycloudctl` 现在会自动探测本机 IP 来填充 bind / advertise / service-http 地址，不再默认回退到 `127.0.0.1`。

如果你就是想强制走回环地址，也可以直接写：

```bash
pycloudctl start-node --local --node-id node-1 --infocenter-addr 127.0.0.1:50051
```

如果你就是想单独起一个不注册到控制面的 node，可以显式传空字符串：

```bash
pycloudctl start-node --node-id node-standalone --infocenter-addr ""
```

这里也不会再默认补本地 `127.0.0.1:<controlplane-port>`；你必须显式选择：

1. 接入某个控制面：传 `--infocenter-addr host:port`
2. 完全 standalone：传 `--infocenter-addr ""`

### 4.5 什么时候还要用 `pycloud-control`

如果你不是想要“pycloudctl 管住的一组本地进程”，而是想直接操作底层 server 入口，那么再用 `pycloud-control`。

下面这些示例仍然有效：

`--role` 现在按前缀归一化，常用短写可以直接用：

1. `info`
2. `gate`
3. `job`
4. `node`
5. `cont`

### 4.6 单独起 `infocenter`

```bash
pycloud-control \
  --role info \
  --bind 0.0.0.0:50051
```

仓库开发态：

```bash
PYTHONPATH=src python -m pycloud_parallel.controlplane.server \
  --role info \
  --bind 0.0.0.0:50051
```

用途：

1. 只提供 `/nodes`、`/services/routes`、`/ops` 这一类管理面能力
2. 常用于你想把注册中心和 Gateway 拆开部署的时候

### 4.7 单独起 HTTP Gateway

```bash
pycloud-control \
  --role gate \
  --bind 0.0.0.0:50052 \
  --infocenter-addr 127.0.0.1:50051
```

仓库开发态：

```bash
PYTHONPATH=src python -m pycloud_parallel.controlplane.server \
  --role gate \
  --bind 0.0.0.0:50052 \
  --infocenter-addr 127.0.0.1:50051
```

说明：

1. 这里的 `gateway` 是单独 HTTP 入口
2. 它必须知道 `infocenter` 地址，所以 `--infocenter-addr` 必填
3. 它适合把“注册中心”和“服务调用 HTTP 入口”拆成两个进程

### 4.8 单独起 `controlplane`

```bash
pycloud-control \
  --role cont \
  --bind 0.0.0.0:50051
```

仓库开发态：

```bash
PYTHONPATH=src python -m pycloud_parallel.controlplane.server \
  --role cont \
  --bind 0.0.0.0:50051
```

说明：

1. 这是默认最推荐的轻量部署方式
2. 一个进程里同时提供 `InfoCenter + Gateway + JobQueue`
3. `pycloudctl start` 底层起的就是这个角色，再加两个 `nodecontrol`

### 4.9 单独起 `nodecontrol`

```bash
pycloud-control \
  --role node \
  --bind 192.168.1.23:50061 \
  --node-id node-1 \
  --worker-capacity 8 \
  --queue-capacity 1000 \
  --service-http-bind 192.168.1.23:18081 \
  --infocenter-addr 127.0.0.1:50051 \
  --advertise-addr 192.168.1.23:50061
```

仓库开发态：

```bash
PYTHONPATH=src python -m pycloud_parallel.controlplane.server \
  --role node \
  --bind 192.168.1.23:50061 \
  --node-id node-1 \
  --worker-capacity 8 \
  --queue-capacity 1000 \
  --service-http-bind 192.168.1.23:18081 \
  --infocenter-addr 127.0.0.1:50051 \
  --advertise-addr 192.168.1.23:50061
```

参数含义：

1. `--bind`
   - node gRPC 控制地址
2. `--node-id`
   - 节点逻辑名
3. `--worker-capacity`
   - 节点 worker 容量
4. `--queue-capacity`
   - 节点内部排队上限
5. `--service-http-bind`
   - 这个 node 自己暴露给 service 调用的 HTTP 地址
6. `--infocenter-addr`
   - 要注册到哪个 `infocenter/controlplane`
7. `--advertise-addr`
   - 注册给控制面的“别人应该怎么连我”的地址

注意：

1. `pycloudctl start-node` 现在要求显式声明 `--infocenter-addr`
2. 如果你就是要 standalone，请显式写 `--infocenter-addr ""`
3. 真正要接入集群时，建议总是同时写上 `--infocenter-addr` 和 `--advertise-addr`

### 4.10 两种常见组合

组合 A，一体化最简本地部署：

```bash
pycloud-control --role cont --bind 0.0.0.0:50051
pycloud-control --role node --bind 192.168.1.23:50061 --node-id node-1 --service-http-bind 192.168.1.23:18081 --infocenter-addr 127.0.0.1:50051 --advertise-addr 192.168.1.23:50061
pycloud-control --role node --bind 192.168.1.24:50062 --node-id node-2 --service-http-bind 192.168.1.24:18082 --infocenter-addr 127.0.0.1:50051 --advertise-addr 192.168.1.24:50062
```

组合 B，拆成独立 `infocenter + gateway + nodes`：

```bash
pycloud-control --role info --bind 0.0.0.0:50051
pycloud-control --role gate --bind 0.0.0.0:50052 --infocenter-addr 127.0.0.1:50051
pycloud-control --role node --bind 192.168.1.23:50061 --node-id node-1 --service-http-bind 192.168.1.23:18081 --infocenter-addr 127.0.0.1:50051 --advertise-addr 192.168.1.23:50061
pycloud-control --role node --bind 192.168.1.24:50062 --node-id node-2 --service-http-bind 192.168.1.24:18082 --infocenter-addr 127.0.0.1:50051 --advertise-addr 192.168.1.24:50062
```

如果 caller 走独立 Gateway，就把目标地址指向 `127.0.0.1:50052`。

### 4.11 “单独起 http” 还有另一层含义

有时候你说的 “http” 可能不是 `gateway`，而是 node 自己的 service HTTP。

这个 HTTP 不是独立 role，它跟着 `nodecontrol` 一起起，通过下面参数指定：

```bash
--service-http-bind 127.0.0.1:18081
```

也就是说：

1. `gateway` 是独立进程角色
2. `service-http` 是 `nodecontrol` 的内嵌 HTTP 服务

## 5. `status`

用途：

1. 查看当前 `runtime-root` 下托管的进程是否正在运行
2. 查看每个 node 当前已加载的服务

示例：

```bash
pycloudctl status
```

如果你用了自定义运行目录或 InfoCenter 地址，要保持一致：

```bash
pycloudctl --runtime-root /tmp/pycloud-dev --controlplane-port 51051 status
```

或者直接指定查询目标：

```bash
pycloudctl status --target 127.0.0.1:51051
```

输出主要分两段：

1. `Service Status`
   - 看 PID 和进程是否在运行
2. `Loaded Services By Node`
   - 看每个 node 已经部署了哪些服务、各服务活跃 worker 数

如果你同时打开 `/ops` 页面，需要注意：

1. node 掉线后，节点表会变成 `healthy = no`
2. 服务实例表现在会额外显示 `node_healthy`
3. stale node 上的服务实例会显示成 `LOST`，并高亮出来

补充说明：

1. 只要有任意一个进程没起来，`status` 的退出码就是 `1`
2. 如果全都正常，退出码是 `0`

这对 shell 脚本里做健康检查很有用，例如：

```bash
if pycloudctl status; then
  echo "pycloud is ready"
else
  echo "pycloud is not fully ready"
fi
```

## 6. `doctor`

用途：

1. 查看当前 `runtime-root` 下的 PID 文件和对应进程状态
2. 查看关键端口当前被哪个 PID 监听
3. 辅助判断“旧服务为什么没被 stop 掉”

最常用：

```bash
pycloudctl doctor
```

自定义查询目标与端口：

```bash
pycloudctl doctor --target 127.0.0.1:51051 --ports 51051,51061,51062,18181,18182
```

它会输出：

1. `Runtime Root`
2. `PID Files`
3. `Port Listeners`

适合场景：

1. 升级后想确认旧服务到底是不是同一套 `runtime-root`
2. `stop` 之后端口还被占用
3. 怀疑是手工 `pycloud-control` 启的旧进程没被当前 `pycloudctl` 管到

说明：

1. `doctor` 只诊断，不会杀进程
2. 它会尽量识别监听进程是不是 `pycloud` 相关命令

## 7. `stop`

用途：

1. 停止当前 `runtime-root` 下记录的全部本地服务
2. 包括默认服务和你用 `pycloudctl` 单独启动过的 `infocenter`、`gateway`、自定义 node

示例：

```bash
pycloudctl stop
```

如果你怀疑旧服务的 PID 文件已经失效，但端口还被占着，可以加端口补扫：

```bash
pycloudctl stop --scan-ports
```

也可以自定义扫描端口：

```bash
pycloudctl stop --scan-ports --ports 50051,50061,50062,18081,18082
```

如果服务是从自定义根目录启动的：

```bash
pycloudctl --runtime-root /tmp/pycloud-dev stop
```

适合场景：

1. 结束本地开发环境
2. 重启前先清空当前整套进程
3. 切换端口配置前先整体停掉

补充说明：

1. `stop` 现在会在停 node 前，尽力通知当前 `InfoCenter/ControlPlane` 把该 node 标记为 lost
2. 这样 `/ops` 里不会长期保留“node 已失联但服务仍显示 RUNNING”的旧快照
3. 这是 best-effort 行为；如果控制面本身已经不可达，就只能直接杀进程
4. `--scan-ports` 会在 PID 文件 stop 之后，再按关键端口补扫监听进程
5. 出于安全考虑，它默认只会尝试清理“看起来像 pycloud server”的监听进程

## 8. `stop-node`

用途：

1. 只停止单个 `nodecontrol`
2. 不停止 `controlplane`
3. 不影响另一个 node

支持任意 node 进程名，例如：

```text
node-1
node-2
node-blue
```

示例：

```bash
pycloudctl stop-node node-1
```

```bash
pycloudctl stop-node node-2
```

```bash
pycloudctl stop-node node-blue
```

配合状态检查：

```bash
pycloudctl stop-node node-1
pycloudctl status
```

适合场景：

1. 想模拟单节点故障
2. 只想释放某个 node 的资源
3. 调试 node 侧问题但不想把整套控制面停掉

注意：

1. 它是“停进程”
2. 如果你只是想让 node 暂时不接新任务，优先考虑 `drain` / `cordon`
3. `drain` / `cordon` 是 HTTP 运维接口，不是 `pycloudctl` 子命令
4. `stop-node` 现在也会先尽力把该 node 标记成 lost，再停进程

例如：

```bash
curl -X POST http://127.0.0.1:50051/ops/nodes/node-1/drain
curl -X POST http://127.0.0.1:50051/ops/nodes/node-1/cordon
```

## 9. `restart`

用途：

1. 先执行 `stop`
2. 再执行 `start`

示例：

```bash
pycloudctl restart
```

自定义目录和端口时：

```bash
pycloudctl \
  --runtime-root /tmp/pycloud-dev \
  --controlplane-port 51051 \
  --node1-port 51061 \
  --node1-http 18181 \
  --node2-port 51062 \
  --node2-http 18182 \
  restart
```

适合场景：

1. 更新代码后重启本地服务
2. 日志、状态不确定时做一次干净重启
3. 端口或 worker 数调整后重新拉起环境

## 10. `gc`

用途：

1. 清理 `code_cache/` 里的陈旧代码缓存
2. 清理 `objects/` 里的陈旧对象缓存

最安全的第一步通常是先 `dry-run`：

```bash
pycloudctl gc --dry-run
```

只清理代码缓存：

```bash
pycloudctl gc --scope codes --older-than-hours 168
```

只清理对象缓存：

```bash
pycloudctl gc --scope objects --older-than-hours 168
```

全部清理：

```bash
pycloudctl gc --scope all --older-than-hours 168
```

指定缓存目录：

```bash
pycloudctl \
  --runtime-root /tmp/pycloud-dev \
  gc \
  --artifact-dir /tmp/pycloud-dev/code_cache \
  --scope all \
  --older-than-hours 72 \
  --dry-run
```

参数说明：

1. `--artifact-dir`
   - 默认是 `<runtime-root>/code_cache`
2. `--scope`
   - `codes`
   - `objects`
   - `all`
3. `--older-than-hours`
   - 超过多少小时算陈旧
4. `--dry-run`
   - 只打印将删除什么，不真正删除

当前 GC 规则：

1. `codes`
   - 看 `codes/<code_sha>/meta.json` 中的 `last_at`
   - 过期后删除整个代码目录
2. `objects`
   - 如果对象被“当前 globals 版本”引用，则保留
   - 其余对象按 `last_at` 是否过期来决定是否删除

`gc` 输出是 JSON，适合直接接到 `jq`：

```bash
pycloudctl gc --scope all --older-than-hours 168 --dry-run | jq
```

## 11. 常用工作流

### 9.1 本地首次启动

```bash
pycloudctl start
pycloudctl status
```

### 9.2 使用临时运行目录

```bash
pycloudctl --runtime-root /tmp/pycloud-dev start
pycloudctl --runtime-root /tmp/pycloud-dev status
pycloudctl --runtime-root /tmp/pycloud-dev stop
```

### 9.3 只下掉一个 node 做排查

```bash
pycloudctl stop-node node-1
pycloudctl status
```

### 9.4 每周清一次缓存

先看计划删除内容：

```bash
pycloudctl gc --scope all --older-than-hours 168 --dry-run
```

确认后正式执行：

```bash
pycloudctl gc --scope all --older-than-hours 168
```

### 11.5 新版本装好了，但旧服务没停掉

推荐顺序：

```bash
pycloudctl doctor
pycloudctl stop --scan-ports
pycloudctl doctor
```

如果你知道旧服务跑的是另一组端口：

```bash
pycloudctl doctor --ports 51051,51061,51062,18181,18182
pycloudctl stop --scan-ports --ports 51051,51061,51062,18181,18182
```

## 12. 故障排查

### 12.1 `pycloudctl: command not found`

说明你可能还没把包安装成可执行脚本。

可选做法：

1. 在仓库里直接用 `./scripts/start_services.sh`
2. 或者用 `python -m pycloud_parallel.controlplane.ctl`
3. 或者把项目安装到当前 Python 环境后再用 `pycloudctl`

仓库内直接运行示例：

```bash
PYTHONPATH=src python -m pycloud_parallel.controlplane.ctl status
```

### 12.2 `start` 后端口被占用

先确认是不是已有旧实例：

```bash
pycloudctl status
```

如果不是当前 `runtime-root` 下的实例占用了端口，可以：

1. 换一组端口启动
2. 或手动释放那个端口对应的外部进程

例如：

```bash
pycloudctl \
  --controlplane-port 51051 \
  --node1-port 51061 \
  --node1-http 18181 \
  --node2-port 51062 \
  --node2-http 18182 \
  start
```

### 12.3 `stop` 或 `status` 看不到之前启动的服务

最常见原因是这次命令使用的 `runtime-root` 和之前启动时不一致。

因为 PID 文件放在：

```text
<runtime-root>/pids/
```

只要目录不同，就会像在操作另一套环境。

### 12.4 想看更详细错误

直接看日志：

```bash
tail -f logs/controlplane.log
tail -f logs/node-1.log
tail -f logs/node-2.log
```

如果用了自定义根目录：

```bash
tail -f /tmp/pycloud-dev/logs/controlplane.log
```

## 13. 与脚本入口的关系

仓库里的常用脚本：

```bash
./scripts/start_services.sh start
./scripts/start_services.sh status
./scripts/start_services.sh stop
./scripts/start_services.sh stop-node node-1
./scripts/start_services.sh restart
```

Windows `cmd`：

```bat
scripts\start_services.bat start
scripts\start_services.bat status
scripts\start_services.bat stop
```

这些脚本本质上只是把参数继续转发给 `pycloudctl` 对应模块。

补充：

1. 在 Windows 下，`pycloudctl start*` 现在会为每个服务进程打开独立控制台窗口
2. 不再默认静默跑成不可见后台进程

## 14. 推荐记法

日常最常用的 6 个命令：

```bash
pycloudctl start
pycloudctl status
pycloudctl stop
pycloudctl stop-node node-1
pycloudctl restart
pycloudctl doctor
pycloudctl gc --dry-run
```

如果你只记一条原则，记这个就够了：

1. 想整套启动或关闭，用 `start / stop / restart`
2. 想看状态，用 `status`
3. 想只关一个 node，用 `stop-node`
4. 想清缓存，先 `gc --dry-run`，再正式 `gc`
