package com.hmdp.utils;

import com.hmdp.cache.ShopBloomFilter;
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
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_VOUCHER_KEY;
import static com.hmdp.utils.RedisConstants.randomCacheTtlMinutes;

@Component
@Slf4j
public class RedisPreHeatRunner {

    @Resource
    private IShopService shopService;
    @Resource
    private CacheClient cacheClient;
    @Resource
    private ShopBloomFilter shopBloomFilter;
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

        // 【缓存穿透优化】先把数据库中的完整 ID 集合装入布隆过滤器，再标记过滤器可用。
        // 初始化期间查询会降级放行，初始化完成后不存在的随机 ID 才会在数据库之前被拦截。
        shopBloomFilter.initialize(
                list.stream().map(Shop::getId).collect(Collectors.toList()));

        list.forEach(shop -> {
            String key = CACHE_SHOP_KEY + shop.getId();
            // 【缓存雪崩优化】每家店铺独立生成 20~30 分钟逻辑 TTL。
            // 预热通常在短时间内批量写入；随机化可以避免所有店铺在固定 30 分钟后同时进入重建流程。
            cacheClient.setWithExpireTime(
                    key, shop, randomCacheTtlMinutes(), TimeUnit.MINUTES);
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
     *
     * 预热只查询优惠券的存在性，而不会去查询券的信息是否正确，校对信息的任务交给定时任务完成
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
