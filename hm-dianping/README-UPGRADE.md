# 黑马点评升级总览（README-UPGRADE）

> 本文档记录 2026-09 项目完善的全过程：架构演进、一致性保障、接口清单、验证方法。
> 环境基线：JDK 17 · Spring Boot 2.7.18 · RocketMQ 5.5.1（Windows 本机）· Redis（CentOS7 VM）· MySQL（本机）· 双实例 8081/8082

---

## 一、架构演进

```
改造前：Nginx → App（Redis Stream 假MQ + Redis缓存） → MySQL
         一人一单=Set+锁+先查后插（可穿透）· 无支付/关单 · 无限流 · 单级缓存

改造后：Nginx → App×2 [Caffeine L1 + 滑动窗口限流AOP]
                  ↓ RocketMQ 事务消息（削峰/延迟关单/死信补偿）
              Redis [L2缓存 · Pub/Sub失效 · 秒杀预扣 · 幂等退票] 
                  ↓
              MySQL [唯一索引 uk_user_voucher_active · CAS状态机 · 对账账本]
```

## 二、各阶段变更一览

| 阶段 | 内容 | 关键产物 |
|---|---|---|
| 0 | JDK17 + RocketMQ/Caffeine 依赖 + 一人一单唯一索引 + 部署文档 | pom.xml、upgrade.sql、ROCKETMQ-DEPLOY.md |
| 1 | 一人一单三层防重定型 + 秒杀预热 Hash 化 | active_flag 语义、seckill:voucher:{id} |
| 2 | Redis Stream → RocketMQ 事务消息 + 消费者 + DLQ + 结果轮询 | SeckillTxListener 等 8 个新类、seckill.lua 重写 |
| 3 | CAS 支付 + 15 分钟延迟关单 + 关单回补 | payOrder / OrderTimeoutConsumer / tryCloseOrder |
| 4 | 秒杀券启动预热 + 幂等退票 + 定时对账 | RedisPreHeatRunner 扩展 / refundRedisStockOnce / ReconciliationTask |
| 5 | 多级缓存（Caffeine L1 + Pub/Sub 广播失效） | CacheConfig / CacheInvalidateListener / MultiLevelCacheClient |
| 6 | 滑动窗口限流（ZSet+Lua+注解AOP） | @RateLimit / RateLimitAspect / rate_limit.lua / BizException |
| 7 | 缺陷修复（点赞门闩/Feed封顶/恒真bug） + 常量集中 | BlogServiceImpl / FollowServiceImpl |
| 8 | 本文档 + 完整打包验证 | README-UPGRADE.md |

> 注：原"策略A/B 双写策略"设计中的策略B（入口排队+消费者claim）已在阶段4简化移除，事务消息为唯一写策略，需要时从 git 历史找回。

## 三、秒杀全链路（一句话版）

```
限流(AOP) → Lua原子判定[时间窗/库存/一人一单/预扣/事务标记] 
  ├─ 没资格 → 当场报错（ROLLBACK，零残留）
  └─ 有资格 → COMMIT → 返回orderId → 消费者四道防线落库(status=1) 
       → 结果标记SUCCESS → 投递15分钟延迟关单消息
            ├─ 用户支付: CAS status 1→2 ✅
            └─ 超时未支付: CAS 1→4 + active_flag=单id → 回补DB/Redis库存+SREM资格（可重抢）
任何环节失败 → 重试16次 → 死信消费者回补 → 定时对账最终收敛
```

## 四、一致性保障矩阵

| 故障场景 | 保障机制 | 代码位置 |
|---|---|---|
| 扣了 Redis 库存但消息丢失 | 事务消息：Lua+事务标记原子写入，Broker 回查裁决 | SeckillTxListener |
| 消费落库失败 | Broker 自动重试 16 次（递增间隔） | SeckillOrderConsumer |
| 重试耗尽仍失败 | 死信消费者回补库存+资格 | SeckillOrderDlqConsumer |
| Redis/DB 库存漂移 | 落库时 DB 扣不动 → 回补+告警；对账收敛 | landSeckillOrder / ReconciliationTask |
| 幻影扣减（查重命中的消息） | 幂等退票（setIfAbsent 门闩，同单只退一次） | refundRedisStockOnce |
| 同一用户并发重复下单 | Lua资格(L1) → 锁+查重(L2) → 唯一索引(L3) | seckill.lua / landSeckillOrder |
| 消息重复投递 | orderId 幂等（L0） | landSeckillOrder |
| 抢到不付款冻结库存 | 15分钟精确延迟关单（时间轮+早到重投双保险） | OrderTimeoutConsumer / handleOrderTimeout |
| 支付与关单并发打架 | 共用 `WHERE status=1` 的 CAS，一单一终态 | payOrder / tryCloseOrder |
| 关单后重抢冲突 | active_flag=订单id 退出唯一索引 | tryCloseOrder |
| Redis 数据丢失 | 启动预热补写 + 对账修正 | RedisPreHeatRunner / reconcileStock |
| 定时对账双实例重复跑 | Redisson 锁 tryLock 快速失败 | ReconciliationTask |

## 五、新增接口清单

| 接口 | 说明 |
|---|---|
| `GET /voucher-order/result/{orderId}` | 下单结果轮询：PENDING/SUCCESS/FAILED:原因/CLOSED:原因/UNKNOWN |
| `POST /voucher-order/pay/{orderId}` | 模拟支付（CAS 1→2，幂等冲突友好提示） |

原有接口不变（前端零修改兼容）。

## 六、限流配置（注解方式，随时调整）

```java
@RateLimit(key = "seckill", window = 10, maxCount = 10, limitType = USER)  // 抢购防刷
@RateLimit(key = "sendCode", window = 3600, maxCount = 10, limitType = IP) // 防短信轰炸
```
维度支持 USER / IP / GLOBAL；底层 Redis ZSet 滑动窗口，集群共享计数。

## 七、配置项汇总（application.yaml）

```yaml
rocketmq:
  name-server: 127.0.0.1:9876
  producer:
    group: seckill-producer-group
    send-message-timeout: 3000
seckill:
  pay-timeout-minutes: 15          # 支付超时（关单时长）；测试时可改 1
  reconcile-initial-delay-ms: 30000 # 对账首轮延迟
  reconcile-interval-ms: 60000      # 对账间隔
server:
  tomcat:
    threads.max: 500
    max-connections: 8192
    accept-count: 1000
```

## 八、验证清单（手工）

1. **秒杀正常流**：建券（begin/end 覆盖当前）→ 抢购返回 orderId → 轮询 result=SUCCESS → DB 订单 active_flag=0、两侧库存各-1
2. **时间窗**：未开始的券抢购 → "秒杀尚未开始"
3. **一人一单**：同用户重复抢 → "请勿重复下单"；关单后重抢 → 成功（active_flag 语义验证）
4. **支付**：`POST /pay/{orderId}` → status=2；重复支付 → 幂等提示
5. **关单**：`pay-timeout-minutes: 1` + 抢购不支付 → 1 分钟后 status=4、两侧库存+1、资格释放可重抢
6. **限流**：10 秒内连点抢购 >10 次 → "请求过于频繁"
7. **多级缓存**：更新店铺 → 两实例日志均出现"收到失效广播"；`GET /shop/{id}` 数据刷新
8. **对账**：手工 `HSET seckill:voucher:{id} stock 999` → 下一轮日志"虚高已修正"

## 九、已知取舍（诚实清单）

- Pub/Sub 广播尽力而为：实例宕机期间错过的失效由 L1 60s TTL 兜底（脏读窗口有上界）
- 关单消息统一通过 `syncSendDeliverTimeMills` 写入 RocketMQ 5.x 时间轮；消费者早到校验仍按原始绝对时间重投，最终由定时对账兜底
- Redis-DB 双写仍是最终一致（非强一致）：对账任务负责收敛
- 进行中秒杀券的"Redis<DB"可能是健康在途，对账只修虚高不修缺口（缺口待券结束后双向校正）

## 十、后续迭代方向

Canal 订阅 binlog 缓存同步 · Redis 哨兵/集群 · Sentinel 熔断 · 布隆过滤器 · 真实支付 · MP 升级 3.5.x（根治 lambda/JDK17 反射问题）
