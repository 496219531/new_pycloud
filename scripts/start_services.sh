#!/bin/bash
# =============================================================================
# PyCloud 服务启动脚本
# 启动 InfoCenter + 2 个 NodeControl 节点
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

stop_process() {
    local name=$1
    local pid_file="$PID_DIR/${name}.pid"

    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            log_info "Stopping $name (PID: $pid)..."
            kill "$pid" 2>/dev/null || true
            sleep 1
            # 强制 kill 如果还没停
            if kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null || true
            fi
        fi
        rm -f "$pid_file"
    fi
}

# =============================================================================
# 启动函数
# =============================================================================

start_infocenter() {
    local port=${1:-$INFOCENTER_PORT}

    log_info "Starting InfoCenter on port $port..."

    python -m $MODULE \
        --role infocenter \
        --bind "0.0.0.0:$port" \
        --log-level INFO \
        >> "$LOG_DIR/infocenter.log" 2>&1 &

    local pid=$!
    echo $pid > "$PID_DIR/infocenter.pid"
    sleep 1

    if kill -0 "$pid" 2>/dev/null; then
        log_success "InfoCenter started (PID: $pid, Port: $port)"
        return 0
    else
        log_error "InfoCenter failed to start"
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
    echo $pid > "$PID_DIR/${name}.pid"
    sleep 1

    if kill -0 "$pid" 2>/dev/null; then
        log_success "$name started (PID: $pid, Port: $port, HTTP: $http_port)"
        return 0
    else
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
        stop_infocenter 2>/dev/null || true
        stop_process "node-1" 2>/dev/null || true
        stop_process "node-2" 2>/dev/null || true
        sleep 1

        echo ""
        echo "--------------------------------------------"
        echo "  Starting InfoCenter..."
        echo "--------------------------------------------"
        start_infocenter || exit 1

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
        echo "  InfoCenter:  127.0.0.1:$INFOCENTER_PORT"
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

        stop_process "node-1"
        stop_process "node-2"
        stop_process "infocenter"

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
            local pid_file="$PID_DIR/${name}.pid"

            if [ -f "$pid_file" ]; then
                local pid=$(cat "$pid_file")
                if kill -0 "$pid" 2>/dev/null; then
                    echo -e "  ${GREEN}●${NC} $name (PID: $pid) - RUNNING"
                    return 0
                else
                    echo -e "  ${RED}●${NC} $name - DEAD (stale PID file)"
                    return 1
                fi
            else
                echo -e "  ${YELLOW}●${NC} $name - NOT STARTED"
                return 2
            fi
        }

        check_service "infocenter"
        check_service "node-1"
        check_service "node-2"

        echo ""
        ;;

    logs)
        shift
        case "${1:-all}" in
            infocenter|info)
                tail -f "$LOG_DIR/infocenter.log" 2>/dev/null || echo "Log not found"
                ;;
            node-1)
                tail -f "$LOG_DIR/node-1.log" 2>/dev/null || echo "Log not found"
                ;;
            node-2)
                tail -f "$LOG_DIR/node-2.log" 2>/dev/null || echo "Log not found"
                ;;
            all)
                tail -f "$LOG_DIR/infocenter.log" "$LOG_DIR/node-1.log" "$LOG_DIR/node-2.log" 2>/dev/null || echo "Logs not found"
                ;;
            *)
                echo "Usage: $0 logs [infocenter|node-1|node-2|all]"
                ;;
        esac
        ;;

    *)
        echo "============================================"
        echo "  PyCloud Services Controller"
        echo "============================================"
        echo ""
        echo "Usage: $0 {start|stop|restart|status|logs}"
        echo ""
        echo "Commands:"
        echo "  start   - Start InfoCenter + 2 Nodes (default)"
        echo "  stop    - Stop all services"
        echo "  restart - Restart all services"
        echo "  status  - Check service status"
        echo "  logs    - View logs (default: all)"
        echo ""
        echo "  logs infocenter  - View InfoCenter logs"
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
