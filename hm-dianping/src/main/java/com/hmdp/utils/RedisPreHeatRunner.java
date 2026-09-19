package com.hmdp.utils;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Shop;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IShopService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;
import static com.hmdp.utils.RedisConstants.SECKILL_VOUCHER_KEY;

@Component
@Slf4j
public class RedisPreHeatRunner {

    @Resource
    private IShopService shopService;
    @Resource
    private CacheClient cacheClient;
    /* 【阶段4新增】秒杀券预热依赖 */
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 启动后预热店铺信息
     */
    @EventListener(ApplicationReadyEvent.class)
    public void preHeatData() {
        //获取全部的店铺信息
        List<Shop> list = shopService.list();
        list.forEach(shop -> {
            String key = CACHE_SHOP_KEY + shop.getId();
            cacheClient.setWithExpireTime(key, shop, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        });
        log.info("已完成店铺预热");
    }

    /**
     * 【阶段4新增】秒杀券元数据预热。
     * 扫描所有未结束的秒杀券（含未开始的），若 Redis 缺少对应 Hash——
     * 升级前创建的旧券、Redis 数据丢失/被清——则以 DB 为准补写，让两类问题在每次启动时自愈。
     *
     * 关键约束【只补缺失、不覆盖已存在】：
     * 活动进行中 Redis 库存可能低于 DB（请求已被 Lua 预扣、订单尚未落库），
     * 启动时若用 DB 无脑覆盖会造成 Redis 库存虚高；已存在 Hash 的一致性修正交给对账任务
     */
    @EventListener(ApplicationReadyEvent.class)
    public void preHeatSeckillVouchers() {

        // 未结束 = endTime > now，包含"未开始"的券（提前预热，Lua 时间窗到点自动放行）

        List<SeckillVoucher> list = seckillVoucherService.query()
                .gt("end_time", LocalDateTime.now())
                .list();
        int written = 0;
        int skipped = 0;
        for (SeckillVoucher sv : list) {
            String key = SECKILL_VOUCHER_KEY + sv.getVoucherId();
            if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(key))) {
                skipped++;
                continue;
            }
            seckillVoucherService.preHeatRedisMeta(sv);
            written++;
        }
        log.info("秒杀券预热完成: 未结束券 {} 张, 补写 {}, 已存在跳过 {}", list.size(), written, skipped);
    }
}
