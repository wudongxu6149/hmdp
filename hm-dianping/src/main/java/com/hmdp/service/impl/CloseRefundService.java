package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.utils.MQConstants;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.Arrays;

import static com.hmdp.utils.RedisConstants.SECKILL_CLOSE_REFUND_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_RESULT_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_VOUCHER_KEY;

/** DB 关单提交后的 Redis 回补；失败时保留订单上的待处理标记供定时任务重试。 */
@Service
public class CloseRefundService {

    private final static DefaultRedisScript<Long> CLOSE_REFUND_SCRIPT=new DefaultRedisScript<>();

    static {
        CLOSE_REFUND_SCRIPT.setLocation(new ClassPathResource("scripts/close_refund.lua"));
        CLOSE_REFUND_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private VoucherOrderMapper voucherOrderMapper;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /** 开启独立事务：原关单事务已经提交，不能把“清除待办”写进尚未结束的旧事务。 */
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void apply(Long orderId) {

        VoucherOrder order = voucherOrderMapper.selectById(orderId);

        //做订单回补前的检查，只有订单确实已关闭、而且 Redis 回补仍待处理，才继续执行后面的 Lua 脚本
        if (order == null || !Integer.valueOf(4).equals(order.getStatus())
                || !Integer.valueOf(1).equals(order.getCloseRefundPending())) {
            return;
        }

        Long applied = stringRedisTemplate.execute(CLOSE_REFUND_SCRIPT,
                Arrays.asList(SECKILL_CLOSE_REFUND_KEY + orderId,   //退款标记
                        SECKILL_VOUCHER_KEY + order.getVoucherId(), //优惠券标记->恢复库存
                        SECKILL_ORDER_KEY + order.getVoucherId(), //用户购买资格标记
                        SECKILL_RESULT_KEY + orderId),
                order.getUserId().toString(),
                String.valueOf(MQConstants.RESULT_TTL_SECONDS));

        if (applied == null) {
            throw new IllegalStateException("关单 Redis 回补没有返回结果, orderId=" + orderId);
        }

        voucherOrderMapper.update(null, new UpdateWrapper<VoucherOrder>()
                .set("close_refund_pending", 0)
                .eq("id", orderId)
                .eq("status", 4)
                .eq("close_refund_pending", 1));
    }
}
