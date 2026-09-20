package com.hmdp.utils;

import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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
     * 带逻辑过期语义的多级查询：L1 → L2（逻辑过期）→ DB，命中后逐级回填
     *
     * @param keyPrefix 缓存 key 前缀（L1 与 L2 使用同一套 key，失效广播才能按 key 对齐）
     * @param dbLoader  DB 加载函数（由调用方传入，如 this::getById）
     */
    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type,
                                            Function<ID, R> dbLoader, Long ttl, TimeUnit unit) {
        String key = keyPrefix + id;
        // L1：本机内存命中直接返回（此时不打 Redis）
        String l1Json = localCache.getIfPresent(key);
        if (l1Json != null) {
            return JSONUtil.toBean(l1Json, type);
        }

        // L2：走原有逻辑过期链路（未过期返回 / 过期异步互斥重建 / 未预热返回 null）
        R result = cacheClient.queryWithLogicalExpire(keyPrefix, id, type, dbLoader, ttl, unit);
        if (result != null) {

            // 回填 L1：缓存最终解析结果（不含 RedisData 逻辑过期包装——L2 层管过期，L1 只管短 TTL）
            localCache.put(key, JSONUtil.toJsonStr(result));
        }
        return result;
    }


    /**
     * 更新数据后的失效组合拳：删本机 L1 + 广播频道让所有实例（含自己）删各自的 L1。
     * 注意：L2 Redis 缓存的删除由调用方在【更新 DB 之后】先行完成——先更库、再逐级失效
     */
    public void invalidateAndBroadcast(String key) {
        localCache.invalidate(key);

        //向所有订阅了这个频道的实例广播失效
        stringRedisTemplate.convertAndSend(RedisConstants.CACHE_INVALIDATE_TOPIC, key);
        log.info("[多级缓存] 已失效并广播: {}", key);
    }
}
