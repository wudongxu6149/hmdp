# RocketMQ 5.5.1 Windows 部署指南（黑马点评升级配套）

## 1. 前置条件

- JDK 17（RocketMQ 5.5.1 支持 JDK 8/11/17，与本机一致）
- 安装目录**无空格、无中文**，例如 `D:\rocketmq`

## 2. 环境变量

| 变量 | 值 |
|---|---|
| `ROCKETMQ_HOME` | `D:\rocketmq` |
| `NAMESRV_ADDR` | `127.0.0.1:9876` |
| `Path` | 追加 `%ROCKETMQ_HOME%\bin` |

## 3. 调小 JVM 内存（Windows 默认 8g，个人电脑起不来）

- `bin\runserver.cmd`（NameServer 启动脚本）：JVM 参数改为 `-Xms512m -Xmx512m -Xmn256m`
- `bin\runbroker.cmd`（Broker 启动脚本）：默认 `-Xms8g -Xmx8g -Xmn4g` 改为 `-Xms1g -Xmx1g`

## 4. `conf\broker.conf`

```properties
brokerClusterName = DefaultCluster
brokerName = broker-a
brokerId = 0
namesrvAddr = 127.0.0.1:9876
brokerIP1 = 127.0.0.1
listenPort = 10911
storePathRootDir = D:/rocketmq/store
storePathCommitLog = D:/rocketmq/store/commitlog
timerWheelEnable = true
autoCreateTopicEnable = true
```

关键项说明：

- `brokerIP1 = 127.0.0.1`：必须显式指定。多网卡机器上 Broker 会把自己的地址注册到
  NameServer，客户端拿到的可能是虚拟网卡地址导致连不上。
- `timerWheelEnable = true`：开启**时间轮**，这是 5.x 任意精度延迟消息的前提，
  订单「精确 15 分钟关单」依赖此项（4.x 只有 18 个固定延迟等级）。
- `store` 目录不存在时 Broker 会自动创建；确保磁盘空间充足。

## 5. 启动（两个窗口）

```cmd
:: 窗口1：NameServer
mqnamesrv.cmd

:: 窗口2：Broker（-c 指定上面的配置）
mqbroker.cmd -n 127.0.0.1:9876 -c conf\broker.conf
```

## 6. 创建业务 Topic（新窗口）

```cmd
mqadmin updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t seckill_order_tx
mqadmin updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t seckill_order -r 8 -w 8
mqadmin updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t order_timeout -r 4 -w 4
```

| Topic | 用途 | 队列数 |
|---|---|---|
| `seckill_order_tx` | 秒杀事务消息（半消息 + 回查） | 默认 |
| `seckill_order` | 策略B 普通消息；**8 队列**供「按 userId 哈希选队列」，保证同一用户消息串行消费 | 8 |
| `order_timeout` | 订单延迟关单（15 分钟） | 4 |

## 7. 验证

```cmd
mqadmin clusterList -n 127.0.0.1:9876
```

看到 `broker-a` 状态 `UP` 即部署成功。可选：用 docker 启动
[rocketmq-dashboard](https://github.com/apache/rocketmq-dashboard) 控制台可视化查看消息。

## 8. 与本项目的约定

- `application.yaml`：`rocketmq.name-server=127.0.0.1:9876`
- **引入 starter 后应用启动即连接 NameServer**：先启 NameServer/Broker，再启动应用，
  否则应用启动失败（阶段 2 起真正收发消息）
- 常见问题：双实例部署时两台后端共用同一 NameServer，无需额外配置
