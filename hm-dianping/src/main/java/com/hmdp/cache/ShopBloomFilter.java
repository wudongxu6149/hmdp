package com.hmdp.cache;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 店铺 ID 布隆过滤器：只回答“这个 ID 是否可能存在”，不存储店铺业务数据。
 *
 * 布隆过滤器返回 false 时可以确定 ID 不存在；返回 true 仍可能是假阳性，必须继续查缓存或数据库。
 * ready 标记非常重要：全量店铺 ID 尚未装载完成前不能相信否定结果，否则真实店铺会被误判为不存在。
 */
@Slf4j
@Component
public class ShopBloomFilter {

    private final RBloomFilter<Long> bloomFilter;
    private final long expectedInsertions;
    private final double falseProbability;
    private final AtomicBoolean ready = new AtomicBoolean(false);

    public ShopBloomFilter(RedissonClient redissonClient,
                           @Value("${cache.bloom.shop.key:bloom:shop:id}") String key,
                           @Value("${cache.bloom.shop.expected-insertions:100000}") long expectedInsertions,
                           @Value("${cache.bloom.shop.false-probability:0.01}") double falseProbability) {
        this.bloomFilter = redissonClient.getBloomFilter(key);
        this.expectedInsertions = expectedInsertions;
        this.falseProbability = falseProbability;
    }

    /**
     * 启动时初始化参数并把数据库中的全部店铺 ID 加入过滤器。
     * tryInit 只在过滤器首次创建时写入容量参数；过滤器已存在时再次 add 是幂等的，
     * 因此多实例启动或应用重启不会清空在线过滤器，也不会制造初始化期间的假阴性窗口。
     */
    public void initialize(Collection<Long> shopIds) {
        ensureInitialized();
        for (Long shopId : shopIds) {
            if (shopId != null) {
                bloomFilter.add(shopId);
            }
        }
        // 必须在全量 ID 装载完成后再发布 ready；之前的查询采用放行策略回源，不能直接判不存在。
        ready.set(true);
        log.info("店铺布隆过滤器初始化完成: 加载 {} 个ID, expectedInsertions={}, falseProbability={}",
                shopIds.size(), expectedInsertions, falseProbability);
    }

    /**
     * 查询 ID 是否可能存在。
     * 未初始化完成或 Redis 查询异常时返回 true（放行），宁可让少量请求回源数据库，
     * 也不能返回 false 把真实店铺误杀；数据库回源仍受缓存重建互斥锁保护。
     */
    public boolean mightContain(Long shopId) {
        if (shopId == null || shopId < 0) {
            return false;
        }

        //初始化完成前，统一放行请求，由数据库兜底
        if (!ready.get()) {
            return true;
        }
        try {
            return bloomFilter.contains(shopId);
        } catch (RuntimeException e) {
            log.error("店铺布隆过滤器查询异常，降级放行数据库校验: shopId={}", shopId, e);
            return true;
        }
    }

    /**
     * 新增店铺后同步 ID。该方法故意不吞异常：调用方会让数据库事务回滚，
     * 避免出现“数据库已有真实店铺、布隆过滤器却没有”的假阴性。
     */
    public void add(Long shopId) {
        // Web 容器可能在 ApplicationReadyEvent 全量初始化完成前收到请求；新增时先确保底层位图参数存在。
        // tryInit 对已存在过滤器是幂等操作，不会清空其他实例已经写入的 Bit。
        ensureInitialized();
        bloomFilter.add(shopId);
    }

    /** 首次创建过滤器参数；已存在时不会重建或清空在线数据。 */
    private void ensureInitialized() {
        bloomFilter.tryInit(expectedInsertions, falseProbability);
    }
}
