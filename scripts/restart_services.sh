#!/bin/bash
# 可靠的服务重启脚本

echo "=== PyCloud 服务重启 ==="
echo

# 1. 强制停止所有服务
echo "[1] 停止所有服务..."
pkill -9 -f "pycloud_parallel.controlplane.server" 2>/dev/null
sleep 2

# 2. 检查端口是否释放
echo "[2] 检查端口..."
for port in 50051 50061 50062; do
    if lsof -i :$port >/dev/null 2>&1; then
        echo "  端口 $port 仍被占用，等待释放..."
        sleep 2
    else
        echo "  ✓ 端口 $port 已释放"
    fi
done
echo

# 3. 清理可能的临时文件
echo "[3] 清理临时文件..."
rm -f /tmp/pycloud_*.log 2>/dev/null
echo "  ✓ 清理完成"
echo

# 4. 启动服务
echo "[4] 启动服务..."
python -m pycloud_parallel.controlplane.server \
    --role controlplane \
    --bind 0.0.0.0:50051 \
    --log-level INFO \
    > /tmp/pycloud_infocenter.log 2>&1 &
INFOCENTER_PID=$!

python -m pycloud_parallel.controlplane.server \
    --role nodecontrol \
    --bind 0.0.0.0:50061 \
    --node-id node-1 \
    --worker-capacity 4 \
    --queue-capacity 1000 \
    --service-http-bind 127.0.0.1:18081 \
    --target 127.0.0.1:50051 \
    --advertise-addr 127.0.0.1:50061 \
    --node-tags compute \
    --log-level INFO \
    > /tmp/pycloud_node1.log 2>&1 &
NODE1_PID=$!

python -m pycloud_parallel.controlplane.server \
    --role nodecontrol \
    --bind 0.0.0.0:50062 \
    --node-id node-2 \
    --worker-capacity 4 \
    --queue-capacity 1000 \
    --service-http-bind 127.0.0.1:18082 \
    --target 127.0.0.1:50051 \
    --advertise-addr 127.0.0.1:50062 \
    --node-tags compute \
    --log-level INFO \
    > /tmp/pycloud_node2.log 2>&1 &
NODE2_PID=$!

echo "  ✓ InfoCenter PID: $INFOCENTER_PID"
echo "  ✓ Node-1 PID: $NODE1_PID"
echo "  ✓ Node-2 PID: $NODE2_PID"
echo

# 5. 等待服务启动
echo "[5] 等待服务启动..."
sleep 5

# 6. 验证服务状态
echo "[6] 验证服务状态..."
if curl -s http://127.0.0.1:50051/nodes >/dev/null 2>&1; then
    NODE_COUNT=$(curl -s http://127.0.0.1:50051/nodes | python3 -c "import sys,json; print(len(json.load(sys.stdin)['nodes']))")
    echo "  ✓ InfoCenter 运行正常"
    echo "  ✓ 已注册节点数: $NODE_COUNT"
else
    echo "  ✗ InfoCenter 未响应"
fi
echo

echo "=== 重启完成 ==="
echo
echo "查看日志:"
echo "  InfoCenter: tail -f /tmp/pycloud_infocenter.log"
echo "  Node-1:      tail -f /tmp/pycloud_node1.log"
echo "  Node-2:      tail -f /tmp/pycloud_node2.log"
