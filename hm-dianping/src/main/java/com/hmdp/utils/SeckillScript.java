package com.hmdp.utils;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Arrays;

/**
 * 【阶段2新增】秒杀资格判定脚本的统一执行入口。
 * 在事务消息的本地事务内（SeckillTxListener）执行，脚本能力：
 * 时间窗校验 → 库存校验 → 一人一单 → 扣库存 → 记资格 → 写事务标记和恢复记录，全部原子执行。
 * （原策略B"消费者 claim"复用此脚本的分支已随策略B移除）
 */
@Component
public class SeckillScript {

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("scripts/seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Value("${seckill.pending-recovery-delay-ms:60000}")
    //订单什么时候进入最早的异常恢复检查，默认为60s
    private long recoveryDelayMillis;

    /**
     * 执行秒杀资格脚本
     *
     * @return 0=成功 1=库存不足 2=重复下单 3=秒杀未开始 4=已结束 -1=执行异常
     */
    public int execute(Long voucherId, Long userId, Long orderId) {
        long now = System.currentTimeMillis();

        Long r = stringRedisTemplate.execute(
                SECKILL_SCRIPT,

                Arrays.asList(
                        RedisConstants.SECKILL_VOUCHER_KEY + voucherId,  // KEYS[1] 秒杀元数据 Hash
                        RedisConstants.SECKILL_ORDER_KEY + voucherId,    // KEYS[2] 一人一单资格 Set
                        RedisConstants.SECKILL_TX_KEY + orderId,         // KEYS[3] 事务标记
                        RedisConstants.SECKILL_PENDING_ORDER_KEY),      // KEYS[4] 恢复日志

                String.valueOf(now), //ARG[1]
                String.valueOf(userId), //ARG[2]
                String.valueOf(voucherId), //ARG[3]
                String.valueOf(orderId), //ARG[4]
                String.valueOf(now + recoveryDelayMillis)); //ARG[5]

        return r == null ? -1 : r.intValue();
    }
}
