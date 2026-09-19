package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillMessage;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
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
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.hmdp.utils.MQConstants.TOPIC_SECKILL_ORDER;
import static com.hmdp.utils.RedisConstants.*;

/**
 * 服务实现类
 *
 * 【阶段2 重构总览】秒杀写链路：Redis Stream（假 MQ）→ RocketMQ 事务消息。
 * 半消息 → 本地事务内执行秒杀 Lua（时间窗/库存/一人一单/预扣库存/记资格/写事务标记）
 * → 成功 COMMIT（消息必然可消费）；失败 ROLLBACK（Redis 原子，无残留）。
 * 「Redis 扣减」与「消息可消费」被绑成原子结果，修复旧链路"扣了库存消息丢了"的缺陷。
 *
 * 消费者落库（SeckillOrderConsumer → landSeckillOrder）失败自动重试 16 次，
 * 仍失败由死信消费者（SeckillOrderDlqConsumer）回补库存与资格。
 * （原策略B"入口排队+消费者claim"双策略设计已简化移除，见 git 历史）
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

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

    /**
     * 【阶段3新增】未支付订单超时关单时长（分钟）
     */
    @Value("15")
    private long payTimeoutMinutes;

    /* ============================== 下单入口 ============================== */

    /**
     * 购买秒杀券：【阶段2 重构】事务消息写策略——资格判定（Lua）在本地事务内同步执行，
     * 没资格当场答复；有资格则 COMMIT 后由消费者异步落库，客户端凭 orderId 轮询终态
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
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
        try {
            rocketMQTemplate.sendMessageInTransaction(TOPIC_SECKILL_ORDER, message, ctx);
        } catch (Exception e) {
            // 半消息发送失败时 Lua 尚未执行（本地事务在发送成功后才运行），Redis 无扣减，无需回补；
            // 若本地事务已执行而后续异常，状态由 UNKNOWN → Broker 回查事务标记收敛，不会丢库存
            log.error("[事务消息] 发送失败, orderId={}", orderId, e);
            return Result.fail("系统繁忙，请稍后重试");
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
     * 四道防线：L0 orderId 幂等 → L2 Redisson 锁 → L2 业务查重(active_flag=0) → L3 DB 唯一索引。
     * 整个方法在一个事务里：Redis 结果标记放在订单落库之后写，"标记写成功"即代表"订单已提交"。
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void landSeckillOrder(SeckillMessage msg) {
        Long orderId = msg.getOrderId();
        Long userId = msg.getUserId();
        Long voucherId = msg.getVoucherId();

        // L0 幂等：同一 orderId 的消息重投（消费者重试/rebalance）直接确认，结果兜底置成功
        if (getById(orderId) != null) {
            log.info("[幂等命中] 订单已落库, orderId={}", orderId);
            seckillResultStore.mark(orderId, "SUCCESS");
            return;
        }

        // L2 并发防线：按用户加分布式锁。入口 Lua 的 SISMEMBER 原子判重保证同一用户对同一券
        // 只会产生一条消息，此锁专防异常场景（rebalance 重分配）下同一消息被并发重复投递
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        lock.lock();
        try {
            // L2 业务查重：已有【有效订单】（active_flag=0）→ 本消息按重复下单终态化。
            // 已取消的历史单不占位——这是方案A"取消后可重抢"的语义。
            // 【阶段4补漏】Lua 已为本消息预扣了 Redis 库存，但本消息注定不会产生订单（幻影扣减），
            // 必须退票：只退库存、不 SREM 资格——用户的资格由其已有有效订单合法持有
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).eq("active_flag", 0).count();
            if (count > 0) {
                log.info("[查重命中] userId={}, voucherId={} 已有有效订单, 退回幻影预扣库存", userId, voucherId);
                refundRedisStockOnce(orderId, voucherId);
                seckillResultStore.mark(orderId, "FAILED:请勿重复下单");
                return;
            }

            // 乐观扣减库存：UPDATE ... SET stock=stock-1 WHERE voucher_id=? AND stock>0
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId).gt("stock", 0)
                    .update();
            if (!success) {
                // 【数据漂移】Redis 判定有资格但 DB 库存不足：回补 Redis 库存 + 解除资格 + 终态化 + 告警，
                // 不重试（重试无法修复漂移），漂移根因由阶段4的对账任务定位。
                // 【阶段4补漏】回补走幂等退票（setIfAbsent 门闩），防同一消息并发重放导致库存重复+1；
                // SREM 幂等故无需门闩——此处释放资格是因为资格背后没有库存支撑，用户应可重试
                log.error("[库存漂移] Redis/DB 库存不一致, orderId={}, voucherId={}", orderId, voucherId);
                refundRedisStockOnce(orderId, voucherId);
                stringRedisTemplate.opsForSet().remove(SECKILL_ORDER_KEY + voucherId, userId.toString());
                seckillResultStore.mark(orderId, "FAILED:库存不足");
                return;
            }

            // 插入订单（未支付）。activeFlag 不设置 → MP 跳过该列 → DB 默认值 0（有效单）
            VoucherOrder order = new VoucherOrder();
            order.setId(orderId).setUserId(userId).setVoucherId(voucherId).setPayType(1).setStatus(1);
            try {
                save(order);
            } catch (DuplicateKeyException e) {
                // L3 兜底：唯一索引 uk_user_voucher_active 拦截。以 orderId 是否已落库区分两种情况：
                // 本单已存在 → 并发重投，按幂等成功终态化；他单占位 → 重复下单。
                // 无论如何都必须抛出：回滚本次事务（含上面 DB 扣掉的库存，防止双扣），
                // 消息重投后走 L0 幂等 / 查重分支正常确认
                if (getById(orderId) != null) {
                    seckillResultStore.mark(orderId, "SUCCESS");
                } else {
                    seckillResultStore.mark(orderId, "FAILED:请勿重复下单");
                }
                throw e;
            }

            // 终态 SUCCESS：标记写在落库之后、事务提交之前——客户端看到 SUCCESS 时订单必然已可查
            seckillResultStore.mark(orderId, "SUCCESS");

            // 【阶段3新增】发送延迟关单消息：精确投递时间 = 下单时间 + 支付超时（5.x 时间轮 TIMER_DELIVER_MS）。
            // 若属性不被 Broker 支持（消息立即到达），消费者会按剩余时长重投兜底；
            // 订单超时未支付将被 CAS 关单并回补库存/资格；若落库事务最终回滚，消费者按"订单不存在"丢弃，无害
            sendTimeoutMessage(msg, LocalDateTime.now().plusMinutes(payTimeoutMinutes), null);
        } finally {
            lock.unlock();
        }
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
        if (getById(orderId) != null) {
            log.warn("[DLQ] 订单已存在，无需补偿, orderId={}", orderId);
            return;
        }
        // 【阶段4补漏】回补走幂等退票（防 DLQ 消息重放导致库存重复+1）；SREM 幂等无需门闩
        refundRedisStockOnce(orderId, voucherId);
        stringRedisTemplate.opsForSet().remove(SECKILL_ORDER_KEY + voucherId, userId.toString());
        seckillResultStore.mark(orderId, "FAILED:落库失败已回补");
        log.error("[DLQ补偿] 订单落库最终失败，已回补库存与资格, orderId={}, userId={}, voucherId={}",
                orderId, userId, voucherId);
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
     * 消息可能早到（TIMER_DELIVER_MS 未生效、消息重放等）：未到期按剩余时长重投，到期才关单——
     * 这是"精确15分钟"在 4.x 客户端 + 5.x Broker 组合下的可靠实现路径
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void handleOrderTimeout(SeckillMessage msg) {
        Long orderId = msg.getOrderId();
        VoucherOrder order = getById(orderId);
        if (order == null) {
            // 订单不存在：落库事务曾回滚（死信已补偿）或消息重放——无需关单，直接 ACK
            log.warn("[关单] 订单不存在，忽略, orderId={}", orderId);
            return;
        }
        LocalDateTime due = order.getCreateTime().plusMinutes(payTimeoutMinutes);
        LocalDateTime now = LocalDateTime.now();
        if (now.isBefore(due)) {
            // 未到期：按剩余时长重投。delayLevel 取 ≥ 剩余时间的最小等级（保证下次到达必已到期），
            // 同时保留 TIMER_DELIVER_MS（被支持时精确到达）
            long remainMs = Duration.between(now, due).toMillis();
            sendTimeoutMessage(msg, due, delayLevelForRemaining(remainMs));
            log.info("[关单] 未到期，重投剩余 {}ms, orderId={}", remainMs, orderId);
            return;
        }
        // 已到期：执行关单（内部调用与本方法同事务）
        tryCloseOrder(orderId);
    }

    /**
     * CAS 关单并回补资源（阶段4扫表任务会复用此方法）。
     * status 1→4 且 active_flag=本单id：active_flag 置值后该行退出唯一索引 uk_user_voucher_active，
     * 一人一单的"有效订单"空位释放 → 用户可重新抢购（方案A语义）。
     * 关单成功后回补三件套：DB 库存+1 → Redis 库存+1 → 解除资格 SREM。
     * 顺序刻意 DB 在前：若中途崩溃，宁可 Redis 库存短暂偏高（多放行的请求会被 Lua/查重拦住），不可偏低（少卖）
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean tryCloseOrder(Long orderId) {
        VoucherOrder order = getById(orderId);
        if (order == null || order.getStatus() != 1) {
            // 已支付/已关闭/不存在：CAS 败者，幂等返回
            return false;
        }
        boolean closed = update()
                .set("status", 4)                    // 已取消
                .set("active_flag", order.getId())   // 退出唯一约束 → 释放一人一单位
                .eq("id", orderId)
                .eq("status", 1)                     // CAS：只关未支付订单
                .update();
        if (!closed) {
            // 并发竞争败者（恰好此刻支付成功 / 已被其他线程关闭）
            return false;
        }
        // 回补 DB 库存
        seckillVoucherService.update()
                .setSql("stock = stock + 1")
                .eq("voucher_id", order.getVoucherId())
                .update();
        // 回补 Redis 库存
        stringRedisTemplate.opsForHash().increment(SECKILL_VOUCHER_KEY + order.getVoucherId(), "stock", 1);
        // 解除一人一单资格（用户可重新抢购）
        stringRedisTemplate.opsForSet().remove(SECKILL_ORDER_KEY + order.getVoucherId(), order.getUserId().toString());
        // 轮询侧同步终态
        seckillResultStore.mark(orderId, "CLOSED:超时未支付");
        log.info("[关单] 已关闭并回补, orderId={}, voucherId={}", orderId, order.getVoucherId());
        return true;
    }

    /**
     * 【阶段3新增】发送关单延迟消息。
     * 优先设置 TIMER_DELIVER_MS（5.x 时间轮，毫秒级精确投递，broker 需 timerWheelEnable=true）；
     * delayLevel 参数非空时（重投场景）同时设置经典延迟等级兜底——两者都指向"不早于到期时刻"，
     * 无论 Broker 支持哪种，到达即视为到期
     */
    private void sendTimeoutMessage(SeckillMessage payload, LocalDateTime due, Integer delayLevel) {
        Message<SeckillMessage> message = MessageBuilder.withPayload(payload)
                .setHeader("TIMER_DELIVER_MS", String.valueOf(toEpochMilli(due)))
                .setHeader("KEYS", String.valueOf(payload.getOrderId())) // 消息Key，便于控制台按订单号追踪
                .build();
        if (delayLevel == null) {
            rocketMQTemplate.syncSend(MQConstants.TOPIC_ORDER_TIMEOUT, message);
        } else {
            rocketMQTemplate.syncSend(MQConstants.TOPIC_ORDER_TIMEOUT, message, 3000, delayLevel);
        }
    }

    /**
     * 【阶段3新增】RocketMQ 经典延迟等级表（1~18 级，毫秒）：
     * 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
     */
    private static final long[] DELAY_LEVEL_MS = {
            1_000L, 5_000L, 10_000L, 30_000L,
            60_000L, 120_000L, 180_000L, 240_000L, 300_000L,
            360_000L, 420_000L, 480_000L, 540_000L, 600_000L,
            1_200_000L, 1_800_000L, 3_600_000L, 7_200_000L};

    /**
     * 【阶段3新增】为剩余时长选取 ≥ 它的最小延迟等级（保证重投后到达时订单必已到期）；超出2h兜底取18级
     */
    private int delayLevelForRemaining(long remainMs) {
        for (int i = 0; i < DELAY_LEVEL_MS.length; i++) {
            if (DELAY_LEVEL_MS[i] >= remainMs) {
                return i + 1;
            }
        }
        return DELAY_LEVEL_MS.length;
    }

    /**
     * 【阶段3新增】LocalDateTime → epoch 毫秒（与秒杀券预热写入的口径一致）
     */
    private long toEpochMilli(LocalDateTime time) {
        return time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    /**
     * 【阶段4补漏】幂等退票：把本消息在 Lua 中预扣的 Redis 库存 +1 回去。
     * 用 setIfAbsent 门闩（seckill:refund:{orderId}，TTL 1天）保证同一 orderId 只退一次——
     * 消息重放/并发重投时不会重复加库存。三个调用方：查重命中（幻影扣减退回）、
     * 库存漂移（回补+释放资格）、死信补偿（最终失败回补）。
     * 注意：只退库存不释放资格——资格是否释放由各调用方按业务语义自行决定（SREM 幂等，无门闩风险）
     */
    private void refundRedisStockOnce(Long orderId, Long voucherId) {
        Boolean first = stringRedisTemplate.opsForValue().setIfAbsent(
                SECKILL_REFUND_KEY + orderId, "1", Duration.ofDays(1));
        if (Boolean.TRUE.equals(first)) {
            stringRedisTemplate.opsForHash().increment(SECKILL_VOUCHER_KEY + voucherId, "stock", 1);
        }
    }
}
