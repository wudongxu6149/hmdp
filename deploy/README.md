# 黑马点评 Docker 整套部署

在项目根目录使用 `compose.yaml`，统一启动 Nginx、Java 应用、MySQL、Redis、RocketMQ NameServer 和 Broker。浏览器通过 Nginx 访问页面与 `/api` 接口，组件之间使用 Compose 服务名通信。

## 1. 环境与版本

- 安装 Docker Engine，或使用 Docker Desktop 并切换到 **Linux containers**；需要 Docker Compose v2（命令为 `docker compose`）。
- 首次构建需要联网拉取镜像和 Maven 依赖，无需在宿主机安装 Java、Maven、MySQL、Redis 或 RocketMQ。
- 当前镜像：MySQL `8.0.45`、Redis `7.2-alpine`、RocketMQ `5.5.0`、Nginx `1.28-alpine`，应用使用 Java 17。
- RocketMQ 选用已确认可获取的官方 `apache/rocketmq:5.5.0`，开启时间轮，并初始化事务消息、延迟消息和可读的死信 Topic。
- 容器、MySQL 会话和应用统一使用 `Asia/Shanghai`（UTC+8）。

根目录 `.env.example` 与 Compose 的默认配置用于本地开发和演示，不代表生产环境配置。部署到共享环境前应替换示例密码。

## 2. 首次启动

先进入含 `compose.yaml` 的项目根目录。可直接使用默认值，也可复制配置后修改：

PowerShell：

```powershell
Copy-Item .env.example .env
```

Bash：

```bash
cp .env.example .env
```

若已有 `.env`，保留并编辑已有文件，避免复制覆盖。可调整 `HTTP_PORT`、`MYSQL_ROOT_PASSWORD`、`MYSQL_PASSWORD` 和 `REDIS_PASSWORD`。

原 Windows Nginx 也使用 `8080`，启动前停止旧 Nginx，或在 `.env` 中设置 `HTTP_PORT=8088`；下面示例使用默认 `8080`。

```bash
docker compose up -d --build
docker compose ps -a
```

首次构建可能较慢。正常状态为六个常驻服务 `mysql`、`redis`、`rocketmq-namesrv`、`rocketmq-broker`、`app`、`nginx` 均运行且健康，两个初始化服务 `rocketmq-volume-init`、`rocketmq-init` 显示 `Exited (0)`。

MQ 数据卷先由 `rocketmq-volume-init` 设置卷根目录的所有者，Broker 再以官方镜像默认的 `rocketmq` 用户启动。应用等待数据库与 Redis 健康、MQ Topic 初始化成功后启动；Nginx 等待应用健康后启动。初始化失败会阻止应用启动。[Compose 启动依赖说明](https://docs.docker.com/compose/how-tos/startup-order/)

访问：

- 页面：[http://localhost:8080](http://localhost:8080)
- 分类接口：[http://localhost:8080/api/shop-type/list](http://localhost:8080/api/shop-type/list)
- 示例店铺：[http://localhost:8080/api/shop/1](http://localhost:8080/api/shop/1)

若修改端口，将上面的 `8080` 替换为 `HTTP_PORT`。从另一台电脑访问时，将 `localhost` 替换为 Docker 宿主机地址。

仅 Nginx 发布宿主机端口。MySQL、Redis、RocketMQ 和应用端口保留在容器网络中；原 IDEA 实例不能直接连接这套中间件。

## 3. 数据初始化与图片

MySQL 仅在 `mysql-data` 为空时执行以下顺序：

1. `hm-dianping/src/main/resources/db/hmdp.sql`：原始表结构及示例数据。
2. `hm-dianping/src/main/resources/db/upgrade.sql`：有效订单标记与一人一单唯一索引。
3. `hm-dianping/src/main/resources/db/close-refund-upgrade.sql`：关单待回补字段与索引。

旧 SQL 中的零日期默认值仅在基础导入会话中放宽限制。该流程创建独立容器数据库，**不会迁移现有宿主机数据库或 Redis 数据**。已有数据需要单独导出、迁移，不能将初始化脚本直接用于现有业务库。

仓库示例数据没有秒杀活动和订单；容器启动成功不等于已完成秒杀业务验证。创建秒杀活动后再验证下单、支付超时和补偿流程。

应用启动时从 MySQL 预热店铺缓存、布隆过滤器和店铺 GEO 索引，新的 Redis 实例可获得附近查询需要的索引；可在应用日志中确认“已完成店铺预热”。

Compose 将 `HMDP_UPLOAD_PATH` 设置为 `/data/imgs`，作为应用图片写入根目录。`image-data` 卷同时挂载给应用与 Nginx，Nginx 以只读方式通过 `/imgs/...` 提供图片。首次空卷从 Nginx 镜像复制仓库示例图片，以后的上传保存在该卷中。[Docker 数据卷说明](https://docs.docker.com/engine/storage/volumes/)

## 4. 更新、停止与排查

更新代码、静态页面或部署配置后，在根目录重新执行：

```bash
docker compose up -d --build
```

该命令同时处理应用和 Nginx 镜像构建。仅执行 `restart` 不会应用源码或镜像修改。

查看全部状态与关键日志：

```bash
docker compose ps -a
docker compose logs --tail=100 mysql redis
docker compose logs --tail=100 rocketmq-volume-init rocketmq-namesrv rocketmq-broker rocketmq-init
docker compose logs --tail=100 app nginx
docker compose logs -f app
```

如果应用未启动，先检查依赖的健康状态和两个初始化服务是否退出为 `0`。如果页面可打开但接口失败，检查应用与 Nginx 日志。端口占用错误则停止旧服务或修改 `HTTP_PORT`。

停止并删除容器，保留命名卷中的数据库、Redis AOF、MQ 存储与日志、上传图片：

```bash
docker compose down
```

**只有确实要删除整套容器数据并重新初始化时，才执行下面命令。它会删除上述命名卷及数据，无法通过再次启动恢复。**

```bash
docker compose down -v
```

更改 `.env` 的 MySQL 密码不会自动修改已有数据卷中的数据库账号；已有数据库的密码变更需同步修改数据库账号与应用配置。

## 5. 验证范围与旧文档

当前已通过选定的 13 项单元测试、Maven 可执行 JAR 打包、官方 Compose 客户端配置校验、初始化脚本语法检查，以及 MQ 初始化成功和“管理命令报错但退出码为 0”的失败分支检查。本机没有 Docker 引擎，尚未验证镜像构建、SQL 实际导入、首次启动、健康检查及上传与持久化等容器运行行为。请在具备 Docker 的环境执行上述启动与接口检查。

`hm-dianping/docs/DOCKER-DEPLOY.md` 保留为历史方案，其中 Nginx 留在 Windows、应用端口对外发布及 RocketMQ `5.5.1` 等描述不适用于本次配置；本次部署以根目录 `compose.yaml` 与本说明为准。RocketMQ 的容器基础用法可参考 [官方 Docker 快速开始](https://rocketmq.apache.org/docs/quickStart/02quickstartWithDocker/)。
