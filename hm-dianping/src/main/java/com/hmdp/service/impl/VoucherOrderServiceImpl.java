package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillMessage;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mq.SeckillPendingOrderStore;
import com.hmdp.mq.SeckillResultStore;
import com.hmdp.mq.SeckillTxContext;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.MQConstants;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;

import static com.hmdp.utils.MQConstants.TOPIC_SECKILL_ORDER;
import static com.hmdp.utils.RedisConstants.*;

/**
 * 服务实现类
 * <p>
 * 【阶段2 重构总览】秒杀写链路：Redis Stream（假 MQ）→ RocketMQ 事务消息。
 * 半消息 → 本地事务内执行秒杀 Lua（时间窗/库存/一人一单/预扣库存/记资格/写事务标记）
 * → 成功 COMMIT；失败 ROLLBACK。Lua 同时留下待恢复订单，兜底 Broker 最终放弃半消息的情况。
 * <p>
 * 消费者落库（SeckillOrderConsumer → landSeckillOrder）失败自动重试 16 次，
 * 仍失败由死信消费者（SeckillOrderDlqConsumer）回补库存与资格。
 * （原策略B"入口排队+消费者claim"双策略设计已简化移除，见 git 历史）
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    private static final DefaultRedisScript<Long> REFUND_SCRIPT = new DefaultRedisScript<>();

    static {
        REFUND_SCRIPT.setLocation(new ClassPathResource("scripts/refund_seckill.lua"));
        REFUND_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    /* 【阶段2新增】RocketMQ 发送端与结果标记存储（Lua 执行在 SeckillTxListener 内） */
    @Resource
    private RocketMQTemplate rocketMQTemplate;
    @Resource
    private SeckillResultStore seckillResultStore;
    @Resource
    private SeckillPendingOrderStore pendingOrderStore;
    @Resource
    private CloseRefundService closeRefundService;
    @Resource
    private TransactionTemplate transactionTemplate;

    /**
     * 【阶段3新增】未支付订单超时关单时长（分钟）。
     * 【缺陷修复】原为 @Value("15") 字面量未读配置：改 yaml 只影响对账路径（ReconciliationTask），
     * 延迟消息主链路仍固定 15 分钟，两条关单路径口径分裂；
     * 现与 ReconciliationTask 读同一个配置项 seckill.pay-timeout-minutes，改 yaml 一处即同步
     */
    @Value("${seckill.pay-timeout-minutes:15}")
    private long payTimeoutMinutes;

    /* ============================== 下单入口 ============================== */

    /**
     * 购买秒杀券：【阶段2 重构】事务消息写策略——资格判定（Lua）在本地事务内同步执行，
     * 没资格当场答复；有资格则 COMMIT 后由消费者异步落库，客户端凭 orderId 轮询终态
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        long startNs = System.nanoTime();
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        // 订单号仍由 RedisIdWorker 生成（时间戳+Redis序列，跨实例严格递增），
        // 必须在发送消息前生成：它要随消息传递，是幂等/事务标记/结果轮询的关联键
        long orderId = redisIdWorker.nextId("order");
        SeckillMessage payload = new SeckillMessage(voucherId, userId, orderId);

        // ctx 是生产者与事务监听器之间的结果载体：executeLocalTransaction 在半消息发送后
        // 于【本线程同步】执行，Lua 判定结果直接写回 ctx，send 返回后即可答复用户
        SeckillTxContext ctx = new SeckillTxContext(voucherId, userId, orderId);

        Message<SeckillMessage> message = MessageBuilder.withPayload(payload)
                // ORDER_ID 放消息头：Broker 回查(checkLocalTransaction)时据此定位 Redis 事务标记
                .setHeader("ORDER_ID", String.valueOf(orderId))
                .build();
        long mqStartNs = System.nanoTime();
        try {
            rocketMQTemplate.sendMessageInTransaction(TOPIC_SECKILL_ORDER, message, ctx);
        } catch (Exception e) {
            // 半消息发送失败时 Lua 尚未执行（本地事务在发送成功后才运行），Redis 无扣减，无需回补；
            // 若本地事务已执行而后续异常，Broker 回查标记；超出回查窗口则由恢复任务处理预扣记录
            log.error("[事务消息] 发送失败, orderId={}", orderId, e);
            return Result.fail("系统繁忙，请稍后重试");
        }
        long endNs = System.nanoTime();
        if (endNs - startNs >= 200_000_000L) {
            log.warn("[秒杀入口慢请求] orderId={}, totalMs={}, beforeMqMs={}, mqMs={}",
                    orderId, (endNs - startNs) / 1_000_000,
                    (mqStartNs - startNs) / 1_000_000,
                    (endNs - mqStartNs) / 1_000_000);
        }

        // Lua 结果：0=有资格(已预扣) 1=库存不足 2=重复下单 3=未开始 4=已结束 -1=执行异常(状态未知)
        switch (ctx.getLuaResult()) {
            case 0:
                return Result.ok(orderId);
            case 1:
                return Result.fail("库存不足!请稍后重试");
            case 2:
                return Result.fail("请勿重复下单!");
            case 3:
                return Result.fail("秒杀尚未开始!");
            case 4:
                return Result.fail("秒杀已经结束!");
            default:
                // 状态未知（Lua 异常）：不能答复成功也不能答复失败——事务消息会经回查最终 COMMIT 或丢弃，
                // 让客户端轮询结果接口拿真实终态
                return Result.fail("系统繁忙，请稍后查询下单结果");
        }
    }

    /* ==================== 消费者落库 ==================== */

    /**
     * 【阶段2 重构】秒杀订单落库，取代旧 createVoucherOrder（Stream 消费+Redisson锁+先查后插）。
     * 四道防线：Redisson 锁 → orderId 幂等 → 业务查重(active_flag=0) → DB 唯一索引。
     * 整个方法在一个事务里：Redis 成功标记和超时消息只在数据库事务提交后写入。
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void landSeckillOrder(SeckillMessage msg) {
        Long orderId = msg.getOrderId();
        Long userId = msg.getUserId();
        Long voucherId = msg.getVoucherId();

        // 正常消费、恢复扫描和 DLQ 共用用户锁。锁须持有到 DB 事务完成，不能在方法返回时提前释放。
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        lock.lock();

        //事务释放后解锁（放前面来使得锁尽可能的早注册）
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCompletion(int status) {
                lock.unlock();
            }
        });


        // 在锁内首次查询，避免先读建立旧事务快照，再拿锁后仍读不到别的实例刚提交的订单。
        VoucherOrder existingOrder = getById(orderId);

        //防止消息重复投递
        if (existingOrder != null) {

            if (existingOrder.getStatus() == 1) {
                log.info("[幂等命中] 订单已落库, orderId={}", orderId);
                markSuccessAndScheduleTimeoutAfterCommit(msg,
                        existingOrder.getCreateTime().plusMinutes(payTimeoutMinutes));
            } else {
                //如果当前订单不是待支付状态，说明已经被处理过了，移除ZSet中的恢复记录
                pendingOrderStore.complete(msg);
            }
            return;
        }

        // 检查已由 DLQ死信队列补偿 或失败分支退票的消息，不允许迟到的事务消息/恢复扫描重新落库。
        if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(SECKILL_REFUND_KEY + orderId))) {
            seckillResultStore.mark(orderId, "FAILED:已回补");
            pendingOrderStore.complete(msg);
            return;
        }

        // 已有其他有效订单时，只退本次预扣库存，不移除该用户已有订单的购买资格。
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).eq("active_flag", 0).count();
        if (count > 0) {
            log.info("[查重命中] userId={}, voucherId={} 已有有效订单, 退回幻影预扣库存", userId, voucherId);
            refundRedisStockOnce(orderId, voucherId, userId, false);
            seckillResultStore.mark(orderId, "FAILED:请勿重复下单");
            pendingOrderStore.complete(msg);
            return;
        }

        // 乐观扣减库存：UPDATE ... SET stock=stock-1 WHERE voucher_id=? AND stock>0
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock", 0)
                .update();
        if (!success) {
            log.error("[库存漂移] Redis/DB 库存不一致, orderId={}, voucherId={}", orderId, voucherId);
            refundRedisStockOnce(orderId, voucherId, userId, true);
            seckillResultStore.mark(orderId, "FAILED:库存不足");
            pendingOrderStore.complete(msg);
            return;
        }

        // 插入订单（未支付）。activeFlag 不设置 → MP 跳过该列 → DB 默认值 0（有效单）
        VoucherOrder order = new VoucherOrder();
        order.setId(orderId).setUserId(userId).setVoucherId(voucherId).setPayType(1).setStatus(1);
        try {
            save(order);
        } catch (DuplicateKeyException e) {
            // DB 唯一索引兜底；抛异常回滚刚才的数据库扣库，重投再按 orderId 幂等或查重处理。
            log.info("[唯一键冲突] 等待消息重投收敛, orderId={}", orderId);
            throw e;
        }

        // 事务提交成功后才对外暴露 SUCCESS，并启动超时关单计时。
        markSuccessAndScheduleTimeoutAfterCommit(msg, LocalDateTime.now().plusMinutes(payTimeoutMinutes));


    }

    /* ==================== 死信补偿 ==================== */

    /**
     * 【阶段2新增】死信队列补偿：消息重试 16 次仍未成功落库（如 DB 长时间不可用）后的最终兜底。
     * 订单未落库 → 回补 Redis 库存 + 解除一人一单资格 + 结果终态 FAILED，用户可重新抢购；
     * 订单已落库 → 说明落库成功但 ACK 丢失，无需补偿，告警人工核对。
     */
    @Override
    public void compensateDlqSeckillOrder(SeckillMessage msg) {
        Long orderId = msg.getOrderId();
        Long userId = msg.getUserId();
        Long voucherId = msg.getVoucherId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        lock.lock();
        try {
            if (getById(orderId) != null) {
                log.warn("[DLQ] 订单已存在，无需补偿, orderId={}", orderId);
                return;
            }
            refundRedisStockOnce(orderId, voucherId, userId, true);
            seckillResultStore.mark(orderId, "FAILED:落库失败已回补");
            pendingOrderStore.complete(msg);
            log.error("[DLQ补偿] 订单落库最终失败，已回补库存与资格, orderId={}, userId={}, voucherId={}",
                    orderId, userId, voucherId);
        } finally {
            lock.unlock();
        }
    }

    /* ==================== 下单结果轮询 ==================== */

    /**
     * 【阶段2新增】客户端轮询下单结果：优先读结果标记（QUEUED/PENDING/SUCCESS/FAILED:原因），
     * 标记过期或丢失时兜底查订单表，保证 orderId 必达可解释的终态
     */
    @Override
    public Result querySeckillResult(Long orderId) {
        String status = seckillResultStore.get(orderId);
        if (status != null) {
            return Result.ok(status);
        }
        VoucherOrder order = getById(orderId);
        return Result.ok(order != null ? "SUCCESS" : "UNKNOWN");
    }

    /* ==================== 支付与超时关单【阶段3新增】 ==================== */

    /**
     * 模拟支付：CAS 条件更新 status 1→2。
     * 「先读状态再更新」在 支付回调/关单/重试 并发时会把已关订单改成已支付——
     * 条件更新 WHERE status=1 让 DB 自己裁决并发：支付与关单竞争，只有一个 UPDATE 能命中，一单一终态。
     * 影响行数=0 时订单必已是其他终态，按幂等冲突答复（重新查询给出具体原因）
     */
    @Override
    public Result payOrder(Long orderId) {
        UserDTO user = UserHolder.getUser();
        VoucherOrder order = getById(orderId);
        if (order == null) {
            return Result.fail("订单不存在!");
        }
        // 只允许支付本人的订单
        if (!order.getUserId().equals(user.getId())) {
            return Result.fail("只能支付自己的订单!");
        }
        // CAS：SET status=2, pay_time=NOW() WHERE id=? AND status=1
        boolean success = update()
                .set("status", 2)
                .set("pay_time", LocalDateTime.now())
                .eq("id", orderId)
                .eq("status", 1)
                .update();
        if (!success) {
            // CAS 败者：查询真实终态给出幂等答复
            int status = getById(orderId).getStatus();
            if (status == 2) {
                return Result.fail("订单已支付，请勿重复支付");
            }
            if (status == 4) {
                return Result.fail("订单已超时关闭，无法支付");
            }
            return Result.fail("当前订单状态无法支付");
        }
        return Result.ok();
    }

    /**
     * 关单延迟消息消费入口。
     * 正常情况下消息由 RocketMQ 5.x 时间轮在 due 时刻投递；如果因时钟偏差、历史消息重放等原因早到，
     * 仍使用同一个绝对时间重新写入时间轮。这样既保留消费端的防御性校验，也不会退化为经典延迟等级。
     */
    @Override
    public void handleOrderTimeout(SeckillMessage msg) {
        Long orderId = msg.getOrderId();
        VoucherOrder order = getById(orderId);
        if (order == null) {
            // 订单不存在：说明订单从未成功落库（落库事务回滚，或落库失败已由 DLQ 补偿）。
            // 该分支是幂等的；若此消息被重放，只会再次命中这里，直接 ACK 即可。
            log.warn("[关单] 订单不存在，忽略, orderId={}", orderId);
            return;
        }

        LocalDateTime due = order.getCreateTime().plusMinutes(payTimeoutMinutes);
        LocalDateTime now = LocalDateTime.now();

        if (now.isBefore(due)) {
            // 【时间轮修复】早到消息仍按原始 due 绝对时间重新投递。
            // 旧代码会把剩余时间映射到 1~18 级经典延迟等级，不仅丢失毫秒级精度，
            // 还会让系统表面上一直走经典延迟；现在统一进入 5.x 时间轮，行为和首次发送完全一致。
            long remainMs = Duration.between(now, due).toMillis();
            sendTimeoutMessage(msg, due);
            log.info("[关单] 未到期，重投剩余 {}ms, orderId={}", remainMs, orderId);
            return;
        }
        // 已到期：关单方法仅在数据库更新期间持有事务
        this.tryCloseOrder(orderId);
    }

    /**
     * CAS 关单并回补资源（阶段4扫表任务会复用此方法）。
     * status 1→4 且 active_flag=本单id：active_flag 置值后该行退出唯一索引 uk_user_voucher_active，
     * 一人一单的"有效订单"空位释放 → 用户可重新抢购（方案A语义）。
     * DB 关单、库存+1及待回补标记同事务提交；Redis 回补仅在提交后执行，失败由定时任务重试。
     */
    @Override
    public boolean tryCloseOrder(Long orderId) {
        Boolean closed = transactionTemplate.execute(status -> closeOrderInDb(orderId));
        if (!Boolean.TRUE.equals(closed)) {
            return false;
        }
        try {
            closeRefundService.apply(orderId);
        } catch (Exception e) {
            // DB 已提交，异常不能回滚；待处理标记保留，交给定时任务重试。
            log.error("[关单] Redis 回补失败，等待定时重试, orderId={}", orderId, e);
        }
        return true;
    }

    private boolean closeOrderInDb(Long orderId) {
        VoucherOrder order = getById(orderId);
        if (order == null) {
            log.info("[关单] 订单不存在, orderId={}", orderId);
            return false;
        }
        if (order.getStatus() != 1) {
            // 已支付/已关闭：CAS 败者，幂等返回
            log.info("[关单] 订单已关闭/支付, orderId={}, voucherId={}", orderId, order.getVoucherId());
            return false;
        }

        //更新数据库中对于订单的信息
        boolean closed = update()
                .set("status", 4)                    // 已取消
                .set("active_flag", order.getId())   // 退出唯一约束 → 释放一人一单位
                .set("close_refund_pending", 1)     //标记是否退款
                .eq("id", orderId)
                .eq("status", 1)         // CAS：只关未支付订单
                .update();
        if (!closed) {
            // 并发竞争败者（恰好此刻支付成功 / 已被其他线程关闭）
            log.info("[关单] 订单不满足唯一索引, orderId={}, voucherId={}", orderId, order.getVoucherId());
            return false;
        }
        // 回补 DB 库存
        boolean stockRefunded = seckillVoucherService.update()
                .setSql("stock = stock + 1")
                .eq("voucher_id", order.getVoucherId())
                .update();
        if (!stockRefunded) {
            throw new IllegalStateException("关单回补数据库库存失败, orderId=" + orderId);
        }

        log.info("[关单] 数据库已标记关闭，等待事务提交后回补 Redis, orderId={}", orderId);
        return true;
    }

    /**
     * 数据库事务提交后再更新 Redis 轮询结果并发送超时消息，
     * 避免回滚订单被客户端观测为 SUCCESS。
     */
    private void markSuccessAndScheduleTimeoutAfterCommit(SeckillMessage msg, LocalDateTime due) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                //先发消息
                sendTimeoutMessage(msg, due);
                seckillResultStore.mark(msg.getOrderId(), "SUCCESS");
                pendingOrderStore.complete(msg);
            }
        });
    }

    /**
     * 【时间轮修复】发送 RocketMQ 5.x 任意精度延迟消息。
     * <p>
     * TIMER_DELIVER_MS 是 RocketMQ 的保留系统属性，不能通过 Spring Message#setHeader 当成普通属性传递；
     * rocketmq-spring 2.2.3 会过滤该 Header。必须调用 syncSendDeliverTimeMills，由模板在原生消息上执行
     * Message#setDeliverTimeMs，Broker 才会把消息写入时间轮。此处只保留业务 KEYS Header 用于消息追踪。
     *
     * @param payload 关单消息体
     * @param due     订单应触发超时检查的绝对时间
     */
    private void sendTimeoutMessage(SeckillMessage payload, LocalDateTime due) {

        Message<SeckillMessage> message = MessageBuilder.withPayload(payload)
                // KEYS 是模板明确支持映射到原生消息的索引字段，保留后可在控制台按订单号追踪。
                .setHeader("KEYS", String.valueOf(payload.getOrderId()))
                .build();

        // 专用 API 会把 due 转成原生 TIMER_DELIVER_MS 系统属性；普通 syncSend + 自定义 Header 无法做到这一点。
        rocketMQTemplate.syncSendDeliverTimeMills(
                MQConstants.TOPIC_ORDER_TIMEOUT, message, toEpochMilli(due));

    }

    /**
     * 【阶段3新增】LocalDateTime → epoch 毫秒（与秒杀券预热写入的口径一致）
     */
    private long toEpochMilli(LocalDateTime time) {
        return time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    /**
     * 【阶段4补漏】幂等退票：把本消息在 Lua 中预扣的 Redis 库存 +1 回去。
     * Lua 将退票标记、库存 +1 和可选的资格释放原子执行，保证同一 orderId 只退一次——
     * 消息重放/并发重投时不会重复加库存。三个调用方：查重命中（幻影扣减退回）、
     * 库存漂移（回补+释放资格）、死信补偿（最终失败回补）。
     * 使用场景：数据库中没有这个orderId的订单，只需要恢复Redis中扣减的库存和资格即可
     */
    private void refundRedisStockOnce(Long orderId, Long voucherId, Long userId, boolean releaseQualification) {
        stringRedisTemplate.execute(REFUND_SCRIPT,
                                    //KEYS
                                    Arrays.asList(SECKILL_REFUND_KEY + orderId,
                                            SECKILL_VOUCHER_KEY + voucherId,
                                            SECKILL_ORDER_KEY + voucherId),
                                    //ARG
                                    userId.toString(),
                                    releaseQualification ? "1" : "0");
    }
}
