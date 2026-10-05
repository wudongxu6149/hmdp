# 黑马点评统一 Docker 打包设计

用途：单机开发和演示，包含前端和项目依赖的全部组件。

## 选择

采用 Docker Compose 单机部署。相比只打包后端，它能一起初始化和启动所有依赖；相比集群部署，它不需要编排平台，符合本次演示用途。

浏览器访问 Nginx，Nginx 提供现有静态前端并将 `/api/` 转发到一个 Spring Boot 实例。后端通过 Compose 服务名连接 MySQL、Redis 和 RocketMQ NameServer/Broker。只发布 Nginx 的 HTTP 端口。

## 组件与初始化

- 后端：Maven + Java 17 多阶段构建，运行可执行 Spring Boot JAR。
- 前端：沿用仓库的 HTML、JavaScript 和图片，放入 Nginx 镜像。
- MySQL：空数据卷首次依次导入基础 SQL、有效订单约束升级和关单回补字段升级。旧零日期兼容只作用于基础 SQL 导入会话。
- Redis：AOF 持久化；启动预热补齐店铺 GEO 索引，适配新 Redis。
- RocketMQ：使用已核验存在的官方 `5.5.0` 镜像。原文档的 `5.5.1` 镜像标签不存在；用户选择官方可拉取镜像。保留时间轮、事务 Topic、延迟 Topic 和可读死信 Topic。
- MQ 数据卷先由一次性任务将卷根所有者设为 UID/GID 3000，再启动 Broker。Topic 初始化确认管理命令的实际成功输出后，应用才启动。

数据库、Redis、MQ 存储及日志、图片使用命名数据卷。图片卷由 Nginx 镜像在首次空卷挂载时复制示例图片，后端写入、Nginx 只读访问。

## 必要代码适配

上传目录读取 `HMDP_UPLOAD_PATH`，保留已有本地路径默认值。现有店铺预热同时写入 GEO 索引。其余连接参数由环境变量覆盖，沿用现有 Redisson 与 RocketMQ 客户端配置。

各服务等待依赖健康或初始化任务成功；Nginx 通过 Docker DNS 定期解析应用地址。容器和后端 JVM统一使用上海时区。

## 验证

成功标准：后端可打包；Compose 配置可解析；SQL 和 MQ 初始化顺序完整；上传目录和 GEO 初始化有回归覆盖；在安装 Docker 的环境启动后，页面及 `/api/shop-type/list`、`/api/shop/1` 正常。

已完成后端打包、13 项相关单元测试、Compose 客户端配置校验，以及初始化脚本语法和 MQ 成功/失败分支检查。

本机没有 Docker 引擎，镜像构建、SQL 实际导入和整套容器运行尚未验证。现有本地数据库内容不自动迁移，首次启动使用仓库示例 SQL；示例库没有秒杀活动。
