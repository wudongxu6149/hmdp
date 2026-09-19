package com.hmdp.service.impl;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static com.hmdp.utils.RedisConstants.SECKILL_VOUCHER_KEY;

/**
 * <p>
 * 秒杀优惠券表，与优惠券是一对一关系 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2022-01-04
 */
@Slf4j
@Service
public class SeckillVoucherServiceImpl extends ServiceImpl<SeckillVoucherMapper, SeckillVoucher> implements ISeckillVoucherService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 【阶段4新增】秒杀券元数据写入 Redis Hash（唯一写入点）。
     * 字段名 stock/beginTime/endTime 与 seckill.lua 的 HGET 严格对应；
     * putAll 幂等（覆盖同名字段），可被启动预热反复调用
     */
    @Override
    public void preHeatRedisMeta(SeckillVoucher seckillVoucher) {
        Map<String, String> meta = new HashMap<>();
        meta.put("stock", seckillVoucher.getStock().toString());
        meta.put("beginTime", toEpochMilli(seckillVoucher.getBeginTime()) + "");
        meta.put("endTime", toEpochMilli(seckillVoucher.getEndTime()) + "");
        stringRedisTemplate.opsForHash().putAll(SECKILL_VOUCHER_KEY + seckillVoucher.getVoucherId(), meta);
        log.info("秒杀券元数据写入 Redis, voucherId={}, stock={}", seckillVoucher.getVoucherId(), seckillVoucher.getStock());
    }

    /** LocalDateTime → epoch 毫秒（与 seckill.lua 时间窗比较的口径一致） */
    private long toEpochMilli(LocalDateTime time) {
        return time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}
