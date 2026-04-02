#!/bin/bash
# =============================================================================
# PyCloud 服务启动脚本
# 启动 ControlPlane(InfoCenter + Gateway) + 2 个 NodeControl 节点
# =============================================================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 默认配置
INFOCENTER_PORT=${INFOCENTER_PORT:-50051}
NODE1_PORT=${NODE1_PORT:-50061}
NODE1_HTTP=${NODE1_HTTP:-18081}
NODE2_PORT=${NODE2_PORT:-50062}
NODE2_HTTP=${NODE2_HTTP:-18082}

LOG_DIR="./logs"
PID_DIR="./pids"

# 模块路径
MODULE="pycloud_parallel.controlplane.server"

# =============================================================================
# 辅助函数
# =============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%H:%M:%S') $1"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $(date '+%H:%M:%S') $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%H:%M:%S') $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%H:%M:%S') $1"
}

kill_pid() {
    local pid=$1
    local label=$2
    if kill -0 "$pid" 2>/dev/null; then
        log_info "Stopping $label (PID: $pid)..."
        kill "$pid" 2>/dev/null || true
        sleep 1
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
        fi
    fi
}

stop_process() {
    local name=$1
    local match_pattern=${2:-}
    local pid_file="$PID_DIR/${name}.pid"

    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        kill_pid "$pid" "$name"
        rm -f "$pid_file"
    fi

    if [ -n "$match_pattern" ]; then
        local found=0
        for pid in $(pgrep -f "$match_pattern" 2>/dev/null || true); do
            found=1
            kill_pid "$pid" "$name (matched)"
        done
        if [ "$found" -eq 1 ]; then
            rm -f "$pid_file"
        fi
    fi
}

wait_controlplane_ready() {
    local port=$1
    local timeout_sec=${2:-15}
    python - "$port" "$timeout_sec" <<'PY'
import json
import sys
import time
from urllib.request import urlopen

port = int(sys.argv[1])
timeout_sec = float(sys.argv[2])
deadline = time.time() + timeout_sec
url = f"http://127.0.0.1:{port}/nodes?healthy_only=false&limit=1"

while time.time() < deadline:
    try:
        with urlopen(url, timeout=1.0) as resp:
            data = json.loads(resp.read().decode("utf-8") or "{}")
        if isinstance(data, dict) and data.get("ok") is True:
            raise SystemExit(0)
    except Exception:
        time.sleep(0.2)

raise SystemExit(1)
PY
}

check_http_endpoint() {
    local url=$1
    local timeout_sec=${2:-5}
    python - "$url" "$timeout_sec" <<'PY'
import sys
from urllib.request import urlopen

url = sys.argv[1]
timeout_sec = float(sys.argv[2])

try:
    with urlopen(url, timeout=timeout_sec) as resp:
        status = int(getattr(resp, "status", 0) or 0)
        if 200 <= status < 300:
            raise SystemExit(0)
except Exception:
    pass

raise SystemExit(1)
PY
}

wait_node_registered() {
    local infocenter_target=$1
    local node_id=$2
    local timeout_sec=${3:-15}
    python - "$infocenter_target" "$node_id" "$timeout_sec" <<'PY'
import json
import sys
import time
from urllib.request import urlopen

target = str(sys.argv[1]).strip()
node_id = sys.argv[2]
timeout_sec = float(sys.argv[3])
deadline = time.time() + timeout_sec
if not target.startswith(("http://", "https://")):
    target = f"http://{target}"
url = f"{target.rstrip('/')}/nodes?healthy_only=false&limit=500"

while time.time() < deadline:
    try:
        with urlopen(url, timeout=1.0) as resp:
            data = json.loads(resp.read().decode("utf-8") or "{}")
        if isinstance(data, dict):
            nodes = data.get("nodes") or []
            if any(str(item.get("node_id", "")) == node_id for item in nodes if isinstance(item, dict)):
                raise SystemExit(0)
    except Exception:
        pass
    time.sleep(0.2)

raise SystemExit(1)
PY
}

# =============================================================================
# 启动函数
# =============================================================================

start_controlplane() {
    local port=${1:-$INFOCENTER_PORT}

    log_info "Starting ControlPlane on port $port..."

    python -m $MODULE \
        --role controlplane \
        --bind "0.0.0.0:$port" \
        --log-level INFO \
        >> "$LOG_DIR/controlplane.log" 2>&1 &

    local pid=$!

    if kill -0 "$pid" 2>/dev/null && wait_controlplane_ready "$port" 15; then
        echo $pid > "$PID_DIR/controlplane.pid"
        log_success "ControlPlane started (PID: $pid, Port: $port)"
        return 0
    else
        rm -f "$PID_DIR/controlplane.pid"
        log_error "ControlPlane failed to start"
        return 1
    fi
}

start_node() {
    local name=$1
    local port=$2
    local http_port=$3
    local infocenter=${4:-"127.0.0.1:$INFOCENTER_PORT"}

    log_info "Starting $name on port $port (HTTP: $http_port)..."

    python -m $MODULE \
        --role nodecontrol \
        --bind "0.0.0.0:$port" \
        --node-id "$name" \
        --worker-capacity 4 \
        --queue-capacity 1000 \
        --service-http-bind "127.0.0.1:$http_port" \
        --infocenter-addr "$infocenter" \
        --advertise-addr "127.0.0.1:$port" \
        --node-tags "compute" \
        --log-level INFO \
        >> "$LOG_DIR/${name}.log" 2>&1 &

    local pid=$!

    if kill -0 "$pid" 2>/dev/null && wait_node_registered "$infocenter" "$name" 15; then
        echo $pid > "$PID_DIR/${name}.pid"
        log_success "$name started (PID: $pid, Port: $port, HTTP: $http_port)"
        return 0
    else
        rm -f "$PID_DIR/${name}.pid"
        log_error "$name failed to start"
        return 1
    fi
}

# =============================================================================
# 主逻辑
# =============================================================================

case "${1:-start}" in
    start)
        echo "============================================"
        echo "  PyCloud Services Starter"
        echo "============================================"
        echo ""

        # 创建目录
        mkdir -p "$LOG_DIR" "$PID_DIR"

        # 停止已有进程
        log_info "Stopping existing services..."
        stop_process "controlplane" "pycloud_parallel.controlplane.server --role controlplane" 2>/dev/null || true
        stop_process "infocenter" "pycloud_parallel.controlplane.server --role infocenter" 2>/dev/null || true
        stop_process "gateway" "pycloud_parallel.controlplane.server --role gateway" 2>/dev/null || true
        stop_process "node-1" "pycloud_parallel.controlplane.server --role nodecontrol --bind 0.0.0.0:$NODE1_PORT --node-id node-1" 2>/dev/null || true
        stop_process "node-2" "pycloud_parallel.controlplane.server --role nodecontrol --bind 0.0.0.0:$NODE2_PORT --node-id node-2" 2>/dev/null || true
        sleep 1

        echo ""
        echo "--------------------------------------------"
        echo "  Starting ControlPlane..."
        echo "--------------------------------------------"
        start_controlplane || exit 1

        sleep 2

        echo ""
        echo "--------------------------------------------"
        echo "  Starting NodeControl Nodes..."
        echo "--------------------------------------------"
        start_node "node-1" "$NODE1_PORT" "$NODE1_HTTP" || exit 1
        start_node "node-2" "$NODE2_PORT" "$NODE2_HTTP" || exit 1

        echo ""
        echo "============================================"
        echo "  All Services Started!"
        echo "============================================"
        echo ""
        echo "  ControlPlane: 127.0.0.1:$INFOCENTER_PORT"
        echo "  Node-1:      127.0.0.1:$NODE1_PORT (HTTP: $NODE1_HTTP)"
        echo "  Node-2:      127.0.0.1:$NODE2_PORT (HTTP: $NODE2_HTTP)"
        echo ""
        echo "  Logs:        $LOG_DIR/"
        echo "  PIDs:        $PID_DIR/"
        echo ""
        echo "  Run './start_services.sh status' to check status"
        echo "  Run './start_services.sh stop' to stop all"
        echo ""
        ;;

    stop)
        echo "============================================"
        echo "  Stopping PyCloud Services"
        echo "============================================"
        echo ""

        stop_process "node-1" "pycloud_parallel.controlplane.server --role nodecontrol --bind 0.0.0.0:$NODE1_PORT --node-id node-1"
        stop_process "node-2" "pycloud_parallel.controlplane.server --role nodecontrol --bind 0.0.0.0:$NODE2_PORT --node-id node-2"
        stop_process "controlplane" "pycloud_parallel.controlplane.server --role controlplane"
        stop_process "infocenter" "pycloud_parallel.controlplane.server --role infocenter"
        stop_process "gateway" "pycloud_parallel.controlplane.server --role gateway"

        log_success "All services stopped"
        ;;

    restart)
        $0 stop
        sleep 2
        $0 start
        ;;

    status)
        echo "============================================"
        echo "  Service Status"
        echo "============================================"
        echo ""

        check_service() {
            local name=$1
            local match_pattern=${2:-}
            local pid_file="$PID_DIR/${name}.pid"

            if [ -f "$pid_file" ]; then
                local pid=$(cat "$pid_file")
                if kill -0 "$pid" 2>/dev/null; then
                    echo -e "  ${GREEN}●${NC} $name (PID: $pid) - RUNNING"
                    return 0
                else
                    rm -f "$pid_file"
                    if [ -n "$match_pattern" ]; then
                        local recovered_pid
                        recovered_pid=$(pgrep -f "$match_pattern" 2>/dev/null | head -n 1)
                        if [ -n "$recovered_pid" ]; then
                            echo "$recovered_pid" > "$pid_file"
                            echo -e "  ${GREEN}●${NC} $name (PID: $recovered_pid) - RUNNING"
                            return 0
                        fi
                    fi
                    echo -e "  ${RED}●${NC} $name - DEAD (stale PID file)"
                    return 1
                fi
            else
                if [ -n "$match_pattern" ]; then
                    local pid
                    pid=$(pgrep -f "$match_pattern" 2>/dev/null | head -n 1)
                    if [ -n "$pid" ]; then
                        echo "$pid" > "$pid_file"
                        echo -e "  ${GREEN}●${NC} $name (PID: $pid) - RUNNING"
                        return 0
                    fi
                fi
                echo -e "  ${YELLOW}●${NC} $name - NOT STARTED"
                return 2
            fi
        }

        overall_status=0
        check_service "controlplane" "pycloud_parallel.controlplane.server --role controlplane" || overall_status=$?
        check_service "node-1" "pycloud_parallel.controlplane.server --role nodecontrol --bind 0.0.0.0:$NODE1_PORT --node-id node-1" || overall_status=$?
        check_service "node-2" "pycloud_parallel.controlplane.server --role nodecontrol --bind 0.0.0.0:$NODE2_PORT --node-id node-2" || overall_status=$?

        echo ""
        echo "  Loaded Services By Node"
        echo "  ------------------------------------------"
        python - "$INFOCENTER_PORT" <<'PY'
from collections import defaultdict
from contextlib import redirect_stdout
import io
import sys

target = f"127.0.0.1:{sys.argv[1]}"
try:
    from pycloud_parallel.controlplane.client import InfoCenterClient
    with InfoCenterClient(target, timeout_sec=3) as client:
        with redirect_stdout(io.StringIO()):
            nodes = list(client.list_nodes(healthy_only=False, limit=500))
            routes = list(client.list_service_routes(healthy_only=False, limit=5000))
except Exception as exc:
    print(f"  (query failed: {exc})")
    raise SystemExit(0)

service_names = defaultdict(set)
for route in routes:
    if route.node_id:
        name = (route.service_name or "").strip()
        if name:
            service_names[route.node_id].add(name)

if not nodes:
    print("  (no nodes)")
else:
    for node in sorted(nodes, key=lambda x: x.node_id):
        names = sorted(service_names.get(node.node_id, set()))
        pyver = (getattr(node, "python_version", "") or "").strip() or "unknown"
        if names:
            print(f"  - {node.node_id} [{pyver}]: {', '.join(names)}")
        else:
            print(f"  - {node.node_id} [{pyver}]: (none)")
PY

        echo ""
        exit $overall_status
        ;;

    logs)
        shift
        case "${1:-all}" in
            controlplane|infocenter|info)
                tail -f "$LOG_DIR/controlplane.log" 2>/dev/null || echo "Log not found"
                ;;
            node-1)
                tail -f "$LOG_DIR/node-1.log" 2>/dev/null || echo "Log not found"
                ;;
            node-2)
                tail -f "$LOG_DIR/node-2.log" 2>/dev/null || echo "Log not found"
                ;;
            all)
                tail -f "$LOG_DIR/controlplane.log" "$LOG_DIR/node-1.log" "$LOG_DIR/node-2.log" 2>/dev/null || echo "Logs not found"
                ;;
            *)
                echo "Usage: $0 logs [controlplane|node-1|node-2|all]"
                ;;
        esac
        ;;

    health)
        echo "============================================"
        echo "  HTTP Health Check"
        echo "============================================"
        echo ""

        base_url="http://127.0.0.1:$INFOCENTER_PORT"
        overall_status=0

        if check_http_endpoint "$base_url/nodes?healthy_only=false&limit=1" 5; then
            echo -e "  ${GREEN}●${NC} controlplane /nodes - OK"
        else
            echo -e "  ${RED}●${NC} controlplane /nodes - FAILED"
            overall_status=1
        fi

        if check_http_endpoint "$base_url/ops" 5; then
            echo -e "  ${GREEN}●${NC} controlplane /ops - OK"
        else
            echo -e "  ${RED}●${NC} controlplane /ops - FAILED"
            overall_status=1
        fi

        echo ""
        exit $overall_status
        ;;

    *)
        echo "============================================"
        echo "  PyCloud Services Controller"
        echo "============================================"
        echo ""
        echo "Usage: $0 {start|stop|restart|status|health|logs}"
        echo ""
        echo "Commands:"
        echo "  start   - Start ControlPlane + 2 Nodes (default)"
        echo "  stop    - Stop all services"
        echo "  restart - Restart all services"
        echo "  status  - Check service status"
        echo "  health  - Check ControlPlane HTTP health"
        echo "  logs    - View logs (default: all)"
        echo ""
        echo "  logs controlplane - View ControlPlane (InfoCenter + Gateway) logs"
        echo "  logs node-1      - View node-1 logs"
        echo "  logs node-2      - View node-2 logs"
        echo "  logs all         - View all logs"
        echo ""
        echo "Environment Variables:"
        echo "  INFOCENTER_PORT=$INFOCENTER_PORT"
        echo "  NODE1_PORT=$NODE1_PORT"
        echo "  NODE1_HTTP=$NODE1_HTTP"
        echo "  NODE2_PORT=$NODE2_PORT"
        echo "  NODE2_HTTP=$NODE2_HTTP"
        echo ""
        ;;
esac
