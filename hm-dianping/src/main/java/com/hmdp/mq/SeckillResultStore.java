package com.hmdp.mq;

import com.hmdp.utils.MQConstants;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

/**
 * 【阶段2新增】下单结果标记的读写封装：seckill:result:{orderId}
 * 取值：PENDING(资格已定待落库) / SUCCESS(已落库) / FAILED:原因 / CLOSED:超时未支付
 * 客户端通过 GET /voucher-order/result/{orderId} 轮询该标记获取终态。
 * 每个请求生成独立 orderId → 各自的结果标记互不覆盖（双击场景：第一单 SUCCESS、第二单 FAILED:重复）。
 */
@Component
public class SeckillResultStore {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public void mark(Long orderId, String status) {
        stringRedisTemplate.opsForValue().set(
                RedisConstants.SECKILL_RESULT_KEY + orderId, status,
                MQConstants.RESULT_TTL_SECONDS, TimeUnit.SECONDS);
    }

    public String get(Long orderId) {
        return stringRedisTemplate.opsForValue().get(RedisConstants.SECKILL_RESULT_KEY + orderId);
    }
}
