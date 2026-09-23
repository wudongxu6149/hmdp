package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SeckillScript;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import static com.hmdp.utils.RedisConstants.SECKILL_TX_KEY;

/**
 * 【阶段2新增】秒杀事务消息监听器（写链路核心）。
 * 绑定默认 rocketMQTemplate：所有 sendMessageInTransaction 的半消息都由本类裁决。
 *
 * 解决的问题：旧链路「先扣 Redis 库存、再发消息」两步不原子——发送失败/进程崩溃会出现
 * 库存已扣但订单消息丢失。现在 Lua（扣库存+写事务标记）在本地事务内执行：
 *   成功 → COMMIT；资格不通过 → ROLLBACK，此时 Lua 未执行任何写操作；
 * 生产者宕机 → Broker 回查事务标记，标记与扣减在同一个 Lua 里原子写入，不存在"已扣无标记"。
 * Broker 达到回查上限仍放弃半消息时，恢复任务按 Lua 留下的订单记录重试落库。
 */
@Slf4j
@Component
@RocketMQTransactionListener
public class SeckillTxListener implements RocketMQLocalTransactionListener {

    @Resource
    private SeckillScript seckillScript;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private SeckillResultStore seckillResultStore;

    /**
     * 本地事务：半消息发送成功后【在调用线程同步】执行。
     * 秒杀 Lua 在这里运行，判定结果写回 ctx 供生产者立即答复用户。
     */
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        SeckillTxContext ctx = (SeckillTxContext) arg;
        try {
            // 事务标记与扣库存/记资格在同一 Lua 内原子完成
            int r = seckillScript.execute(ctx.getVoucherId(), ctx.getUserId(), ctx.getOrderId());
            ctx.setLuaResult(r);
            if (r == 0) {
                // 有资格且已预扣：结果标记 PENDING（等待消费者落库），提交消息
                seckillResultStore.mark(ctx.getOrderId(), "PENDING");
                log.info("[事务消息] 本地事务通过, orderId={}", ctx.getOrderId());
                return RocketMQLocalTransactionState.COMMIT;
            }
            // 无资格：Lua 原子性保证 Redis 未扣（未到扣减步骤），终态化并回滚半消息
            seckillResultStore.mark(ctx.getOrderId(), "FAILED:" + reason(r));
            log.info("[事务消息] 资格不通过 r={}, orderId={}", r, ctx.getOrderId());
            return RocketMQLocalTransactionState.ROLLBACK;
        } catch (Exception e) {
            // Lua 执行异常时 Redis 状态未知（脚本可能已生效）→ 必须返回 UNKNOWN 交由回查裁决。
            // 盲目 ROLLBACK 会出现"库存已扣但消息被丢弃"；UNKNOWN 下 Broker 反复回查，
            // Broker 最终丢弃半消息时，恢复任务仍能找到 Lua 同时写入的预扣记录。
            log.error("[事务消息] 本地事务异常, orderId={}", ctx.getOrderId(), e);
            ctx.setLuaResult(-1);
            return RocketMQLocalTransactionState.UNKNOWN;
        }
    }

    /**
     * Broker 回查（生产者发送 COMMIT 前宕机等场景）：以 Redis 事务标记为唯一事实。
     * 标记由 seckill.lua 与扣减原子写入，TTL 1 天远大于回查窗口（分钟级），不存在歧义。
     *
     * SECKILL_TX_KEY 是 Redis 秒杀资格事务的成功凭证。Broker 回查发现标记存在，
     * 就 COMMIT 原来的 seckill_order 事务半消息；订单消费者收到该消息并成功落库后，再由应用发送 order_timeout 时间轮消息。
     */
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
        Long orderId = resolveOrderId(msg);
        if (orderId == null) {
            // 消息体异常无法定位订单：放弃，等效回滚
            return RocketMQLocalTransactionState.ROLLBACK;
        }
        Boolean exists = stringRedisTemplate.hasKey(SECKILL_TX_KEY + orderId);
        boolean committed = Boolean.TRUE.equals(exists);
        log.info("[事务消息] 回查 orderId={}, 事务标记存在={}", orderId, committed);
        return committed ? RocketMQLocalTransactionState.COMMIT : RocketMQLocalTransactionState.UNKNOWN;
    }

    /**
     * 从回查消息中解析 orderId：优先取生产者放入的 ORDER_ID 头（随消息属性往返），
     * 取不到再兜底解析消息体 JSON
     */
    private Long resolveOrderId(Message msg) {
        Object header = msg.getHeaders().get("ORDER_ID");
        if (header != null) {
            return Long.valueOf(header.toString());
        }
        Object payload = msg.getPayload();
        String json = payload instanceof byte[] ? new String((byte[]) payload) : String.valueOf(payload);
        return JSONUtil.parseObj(json).getLong("orderId");
    }

    private String reason(int r) {
        switch (r) {
            case 1:  return "库存不足";
            case 2:  return "请勿重复下单";
            case 3:  return "秒杀尚未开始";
            case 4:  return "秒杀已经结束";
            default: return "系统异常";
        }
    }
}
