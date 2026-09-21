package com.hmdp.utils;

import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * 【阶段5新增】多级缓存客户端：L1 Caffeine（本机内存）→ L2 Redis → DB。
 *
 * 设计原则：【组合】而非重写 CacheClient——L2 的逻辑过期/互斥锁重建语义原封不动保留在
 * CacheClient.queryWithLogicalExpire 里，本类只在它前面加一层 L1：
 *   L1 命中      → 纳秒级返回，不打 Redis
 *   L1 未命中    → 走原有 L2 链路（未过期返回 / 过期则异步互斥重建）→ 回填 L1
 * L1 的短 TTL（CacheConfig 里 60s）保证重建期间最多容忍 60s 旧数据，与逻辑过期语义自洽
 */
@Slf4j
@Component
public class MultiLevelCacheClient {

    private final Cache<String, String> localCache;
    private final CacheClient cacheClient;
    private final StringRedisTemplate stringRedisTemplate;

    //构造器注入bean
    public MultiLevelCacheClient(Cache<String, String> localCache,
                                 CacheClient cacheClient,
                                 StringRedisTemplate stringRedisTemplate) {
        this.localCache = localCache;
        this.cacheClient = cacheClient;
        this.stringRedisTemplate = stringRedisTemplate;
    }



    /**
     * 【缓存穿透优化】L1 → L2 → Bloom → DB 的多级查询。
     * L1 命中时不访问 Redis；L1 未命中后由 CacheClient 区分 L2 正常值、空值和真正缺失，
     * 只有缓存真正缺失且布隆判断“可能存在”时才允许加锁回源数据库。
     */
    public <R, ID> R queryWithBloomAndLogicalExpire(String keyPrefix,
                                                     ID id,
                                                     Class<R> type,
                                                     Predicate<ID> bloomPredicate,
                                                     Function<ID, R> dbLoader,
                                                     Long ttl,
                                                     TimeUnit unit) {
        String key = keyPrefix + id;
        String l1Json = localCache.getIfPresent(key);
        if (l1Json != null) {
            return JSONUtil.toBean(l1Json, type);
        }

        //Caffeine中未命中店铺缓存
        R result = cacheClient.queryWithBloomAndLogicalExpire(
                keyPrefix, id, type, bloomPredicate, dbLoader, ttl, unit);
        if (result != null) {
            // DB 恢复或 L2 命中后统一回填 L1，后续热点请求不再访问 Redis。
            localCache.put(key, JSONUtil.toJsonStr(result));
        }
        return result;
    }


    /**
     * 更新数据后的失效组合拳：删本机 L1 + 广播频道让所有实例（含自己）删各自的 L1。
     * 注意：L2 Redis 缓存的删除由调用方在【更新 DB 之后】先行完成——先更库、再逐级失效
     */
    public void invalidateAndBroadcast(String key) {
        //失效自己的L1缓存
        localCache.invalidate(key);

        //向所有订阅了这个频道的实例广播失效
        stringRedisTemplate.convertAndSend(RedisConstants.CACHE_INVALIDATE_TOPIC, key);
        log.info("[多级缓存] 已失效并广播: {}", key);
    }
}
