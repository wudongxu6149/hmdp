# 黑马点评完善计划（终版）

## 交付方式（逐步理解）
每阶段开工前先讲解「改哪些文件、每处为什么改」，完成后给变更总结、单独 git commit（一阶段一提交），你确认理解后再进下一阶段。

## 环境定稿
- **JDK 17**：pom.xml `<java.version>8→17`；Caffeine 3.1.8（JDK11+）；Spring Boot 2.7.18 支持 JDK17。若遇 `InaccessibleObjectException` 加 VM 参数 `--add-opens java.base/java.lang=ALL-UNNAMED`。
- nginx 不动、限流全在应用内；RocketMQ 5.5.1（Windows）；Redis（CentOS7 VM）；双实例 8081/8082。
- **已定决策**：一人一单 = 方案A（active_flag，取消后可重抢）。

## 数据一致性总纲
Redis 与 MySQL 库存最终一致（**DB 为准**）；每个 orderId 必达终态可轮询。
保障矩阵：正常→事务消息绑定「Lua扣减」与「消息可消费」｜生产者崩溃→回查 Lua 内原子写入的事务标记｜消费失败→重试16次｜最终失败→死信回补(Redis/DB库存+SREM+标FAILED)｜落库时DB库存不足(漂移)→回补Redis+标FAILED+告警｜重复消息→唯一索引+CAS+幂等ACK｜超时未支付→延迟消息CAS关单+双端回补｜消息丢失→定时对账以DB校正Redis。
九点映射：①写策略B→阶段2｜②事务消息→阶段2｜③一人一单→阶段1｜④对账→阶段4｜⑤延迟关单→阶段3｜⑥CAS支付→阶段3｜⑦限流→阶段6｜⑧多级缓存→阶段5｜⑨读写分离→阶段7。

## 阶段 0：更新依赖和配置文件（本次先执行）
1. **pom.xml**：java.version 8→17；新增 `rocketmq-spring-boot-starter:2.2.3`、`caffeine:3.1.8`。
2. **application.yaml**：rocketmq.name-server=127.0.0.1:9876、rocketmq.producer.group、`seckill.mode=A`（A/B开关）、Tomcat threads.max=500/max-connections=8192/accept-count=1000。
3. **新增 db/upgrade.sql**：
   ```sql
   ALTER TABLE tb_voucher_order
     ADD COLUMN active_flag bigint NOT NULL DEFAULT 0 COMMENT '0=有效；关单时置为本单id',
     ADD UNIQUE KEY uk_user_voucher_active (user_id, voucher_id, active_flag);
   ```
   （执行前先清理历史重复数据，脚本中含清理语句）
4. **RocketMQ 5.5.1 Windows 配置说明**：runbroker/runserver.cmd 内存调小；broker.conf 设 namesrvAddr、brokerIP1=127.0.0.1、Windows 盘符 storePathRootDir、`timerWheelEnable=true`；建 topic：seckill_order_tx、seckill_order（8队列）、order_timeout——写入项目文档供部署参考。

## 阶段 1：一人一单三层防重（方案A）【九点③】
- L1 Lua 资格层（已有保留）；L2 消费者 Redisson 锁+`count(active_flag=0)` 复查；L3 唯一索引捕获 DuplicateKeyException 幂等 ACK。
- 资格生命周期：SADD 获得；解除三条路径（关单/死信回补/对账修复），每条同时回补库存。
- 修改 `VoucherServiceImpl.addSeckillVoucher`：预热改写 Hash `seckill:voucher:{id}`（stock/beginTime/endTime）。

## 阶段 2：写策略 A/B + 事务消息（核心）【九点①②】
1. 重写 `scripts/seckill.lua`：时间窗校验→库存→资格→DECR→SADD→原子写事务标记 `seckill:tx:{orderId}`（TTL 1天）；删 XADD。
2. 新增 `utils/MQConstants`；重写 `seckillVoucher` 按 mode 分支：A=事务消息（PENDING 结果 key+半消息+本地事务跑 Lua）；B=普通消息（QUEUED+按 userId 哈希选队列）立即返回。
3. 新增 `mq/SeckillTxListener`（executeLocalTransaction 跑 Lua；checkLocalTransaction 读事务标记）。
4. 新增 `mq/SeckillOrderConsumer`（公共落库）：claim（B 模式）→锁+复查→DB `where stock>0` 扣减→插订单 status=1→SUCCESS+发延迟关单消息；DB 不足→回补+FAILED+告警；DuplicateKey→幂等。
5. 新增 `mq/SeckillOrderDlqConsumer`（回补+SREM+FAILED）；新增 `GET /voucher-order/result/{orderId}` 轮询。
6. 删除 VoucherOrderHandler（Stream）、blockingQueue、VoucherOrderServiceImplcopy.java、死代码。

## 阶段 3：CAS 支付 + 延迟关单【九点⑤⑥】
1. 新增 `POST /voucher-order/pay/{orderId}`：CAS `status 1→2`+payTime；行数=0→幂等冲突。
2. 新增 `mq/OrderTimeoutConsumer`：TIMER_DELIVER_MS 精确15分钟（兜底 delayLevel 14+未到期重投）；CAS `1→4`+`active_flag=id`→回补 DB/Redis 库存+SREM。

## 阶段 4：定时对账【九点④】
新增 `task/ReconciliationTask`（@Scheduled 每分钟+Redisson 锁+水位）：库存对账（DB 覆盖 Redis）｜扫表关单兜底｜资格 Set 与 DB 差集报告与修复。

## 阶段 5：多级缓存【九点⑧】
新增 `config/CacheConfig`（Caffeine L1）、`utils/MultiLevelCacheClient`（L1→L2→DB）、`listener/CacheInvalidateListener`（Pub/Sub 广播失效）；修改 ShopServiceImpl.queryById/update、ShopTypeServiceImpl、预热仍写 L2。

## 阶段 6：应用内两级限流【九点⑦】
新增 `scripts/rate_limit.lua`（ZSet 滑动窗口）、`@RateLimit`+`RateLimitAspect`（IP/USER/GLOBAL）；应用：秒杀 10次/10s+全局桶、sendCode 1次/60s+IP 10次/h；新增 BizException，改 WebExceptionAdvice。

## 阶段 7：读写链路梳理 + 缺陷修复【九点⑨+清理】
likeBlog 原子化；Feed 收件箱封顶1000；修 FollowServiceImpl 恒真 bug；清 import/死代码；常量集中。

## 阶段 8：验证
mvn package+双实例；100线程抢50券（无超卖/无重复/时间窗/轮询终态/关单重抢）；故障注入（kill/重复投递/库存漂移/停延迟消息）；README-UPGRADE.md。

## 明确不做
nginx/OpenResty 网关层、Canal、Redis 哨兵/集群、Sentinel、布隆过滤器、真实支付、Spring Boot 3 升级。