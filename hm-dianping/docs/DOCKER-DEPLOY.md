# Docker 整套部署文档（Ubuntu 虚拟机 + docker compose）

方案 B：MySQL + Redis + RocketMQ(namesrv/broker) + 应用全部容器化，跑在 Ubuntu 虚拟机
里，服务间用 compose 服务名互访，不依赖任何宿主机 IP。Windows 侧只保留浏览器、IDEA
开发环境和 nginx 前端。

## 一、拓扑与端口

| 服务 | 容器名 | 端口 | 说明 |
|------|--------|------|------|
| mysql:8.0 | hmdp-mysql | 3306（仅容器网络内） | 首次启动自动执行 db 三件套初始化 SQL |
| redis:7.2 | hmdp-redis | 6379（仅容器网络内） | 密码 123456，AOF 持久化 |
| apache/rocketmq:5.5.1 | hmdp-namesrv | 9876（仅容器网络内） | 堆 256m |
| apache/rocketmq:5.5.1 | hmdp-broker | 10911（仅容器网络内） | 堆 512m，配置 deploy/broker.conf |
| hm-dianping:1.0 | hmdp-app | **8081 → 宿主机 8081** | 多阶段构建，对外唯一入口 |

内存预算（4G 虚拟机）：Ubuntu+Docker ≈0.6G，MySQL ≈0.4G，Redis ≈0.1G，
namesrv ≈0.35G，broker ≈0.7G，应用 ≈0.9G，合计 ≈3G，留 1G 余量。
**这套环境用于部署验证，不要在上面跑 JMeter 压测**（压测用 Windows 双实例环境）。

## 二、虚拟机一次性准备（root 执行）

```bash
# 1. 加 4G swap（内存余量小，兜底防 OOM）
fallocate -l 4G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab

# 2. Docker 镜像加速（国内拉 mysql/rocketmq/maven 镜像用；镜像站可能失效，按需替换）
mkdir -p /etc/docker
cat > /etc/docker/daemon.json <<'EOF'
{
  "registry-mirrors": ["https://docker.1ms.run", "https://docker.m.daocloud.io"]
}
EOF
systemctl restart docker

# 3. 配置 GitHub SSH key
ssh-keygen -t ed25519 -C "wudongxu-vm"      # 一路回车
cat ~/.ssh/id_ed25519.pub                   # 复制输出内容
# 浏览器打开 GitHub → 右上角头像 → Settings → SSH and GPG keys → New SSH key → 粘贴保存
ssh -T git@github.com                       # 首次输 yes，看到 "Hi wudongxu6149!" 即成功
```

## 三、首次部署

```bash
# Windows 侧（Powershell/Git Bash，git 操作由自己执行）：
#   把 Dockerfile / docker-compose.yml / deploy/ 等新文件 commit + push

# 虚拟机侧：
git clone git@github.com:wudongxu6149/hmdp.git && cd hmdp

# 构建并启动（首次构建含 Maven 下载依赖，约几分钟）
docker compose up -d --build

# 观察五个容器全部 healthy/running
docker compose ps

# 建 Topic（必须在业务测试前做一次）：
#   order_timeout 不带 DELAY 属性的话延迟消息发送会失败
docker exec hmdp-broker sh mqadmin updateTopic -n rocketmq-namesrv:9876 -c DefaultCluster -t seckill_order -r 8 -w 8
docker exec hmdp-broker sh mqadmin updateTopic -n rocketmq-namesrv:9876 -c DefaultCluster -t order_timeout -r 4 -w 4 -a +message.type=DELAY
```

## 四、验证

```bash
docker logs -f hmdp-app          # 看到 Started HmDianPingApplication 即启动成功
curl http://127.0.0.1:8081/shop/list
```

Windows 浏览器访问 `http://192.168.199.128:8081/shop/list` 能返回 JSON 即通。

前端切换：把 Windows nginx 的 upstream 改指虚拟机（只保留一个实例）：

```nginx
upstream backend {
    server 192.168.199.128:8081 max_fails=5 fail_timeout=10s weight=1;
    # server 127.0.0.1:8082 ...  ← 注释掉
}
```

`nginx -s reload` 后，浏览器 `http://localhost:8080` 走的就是虚拟机里的整套环境。

## 五、日常发布流程（后续改代码/改配置）

```bash
# Windows：commit + push
# 虚拟机：
git pull
docker compose up -d --build
```

## 六、常用运维命令

```bash
docker compose logs -f hmdp-app      # 应用日志
docker compose restart hmdp-app      # 重启应用
docker compose down                  # 停止并删除容器（数据卷保留）
docker compose down -v               # ⚠ 连数据卷一起删，MySQL 会重新走初始化 SQL
docker exec -it hmdp-mysql mysql -uroot -p123456 hmdp
docker exec -it hmdp-redis redis-cli -a 123456
```

## 七、注意事项

1. **MySQL 初始化 SQL 只在数据卷为空时执行**。以后改了 db/ 下的 SQL 想重新初始化，
   必须 `docker compose down -v` 再 `up`（会丢数据）；增量变更走正式 SQL 脚本。
2. Topic 建好后不用每次重建（元数据存在 broker-store 卷里）；`down -v` 后需要重建。
3. `deploy/broker.conf` 里 `brokerIP1 = rocketmq-broker` 是容器网络的服务名，别改成
   127.0.0.1，否则容器外的应用（比如 Windows 本地 IDEA 跑的实例）拿到的 broker 地址
   连不上。
4. Windows 本地 IDEA 开发实例照常连 `127.0.0.1` 的 MySQL/RocketMQ 和
   `192.168.199.128` 的 Redis（那是虚拟机里的旧 Redis，与新容器里的 redis:7.2 容器
   互不干扰，数据是两份）。
5. 虚拟机磁盘建议预留 10G（maven 镜像 0.6G + mysql 0.6G + rocketmq 0.5G + 依赖与
   数据卷）。
