package com.hmdp.utils;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(30);

    /**
     * 逻辑过期的异步重建任务去重表，只在当前 JVM 内生效。
     * <p>
     * 同一个实例的高并发请求同时发现某个 Key 过期时，只有第一个请求能通过 add(lockKey)
     * 提交线程池任务，其他请求直接返回旧值，避免大量无效任务堆积在线程池中。
     * 两个应用实例各自维护一份该集合，因此它不能代替分布式锁；真正保证跨实例只有一个
     * 数据库重建者的是后续的 Redisson RLock。
     */
    private static final Set<String> REBUILDING_KEYS = ConcurrentHashMap.newKeySet();

    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;

    public CacheClient(StringRedisTemplate stringRedisTemplate, RedissonClient redissonClient) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.redissonClient = redissonClient;
    }

    //方法1：将任意java对象序列化为json对象并存储再string类型的key中，并且可以设置ttl过期时间
    public void set(String key, Object value, Long expireTime, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), expireTime, unit);
    }

    //方法2：将任意java对象序列化为json对象并存储再string类型的key中，并且可以设置逻辑过期时间，用于处理缓存击穿问题
    public void setWithExpireTime(String key, Object value, Long expireTime, TimeUnit unit) {
        //设置过期逻辑
        RedisData redisData = new RedisData();
        redisData.setData(value);
        //转换时间单位，统一为秒，设置过期时间
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(expireTime)));

        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    //方法3：根据指定的key查询缓存，并且反序列化为指定类型，利用缓存空值的方式解决穿透问题
    /*缓存穿透
     * */
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> classType,
                                          Function<ID, R> function,
                                          Long expireTime, TimeUnit unit) {
        //1.根据id从redis中查询
        String cacheKey = keyPrefix + id.toString();
        String json = stringRedisTemplate.opsForValue().get(cacheKey);
        //如果不为空,把 json 转换为 bean 对象返回
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, classType);
        }

        /*2.判断命中的是否是空值*/
        if (json != null) {
            return null;
        }

        R r = function.apply(id);
        //如果数据库中也不存在，将空值写进redis中，并设置过期时间
        if (r == null) {
            stringRedisTemplate.opsForValue().set(cacheKey, "", CACHE_NULL_TTL, TimeUnit.SECONDS);
            return null;
        }
        //把查询到的结果添加到redis中，并返回
        this.set(cacheKey, r, expireTime, unit);
        return r;
    }


    //方法4：根据指定的key查询缓存，并且反序列化为指定类型，需要利用逻辑过期解决缓存击穿问题
    /*缓存击穿:逻辑过期*/
    public <R, ID> R queryWithLogicalExpire(String keyPrefix,
                                            ID id,
                                            Class<R> classType,
                                            Function<ID, R> function,
                                            Long expireTime,
                                            TimeUnit unit) {

        String cacheKey = keyPrefix + id.toString();
        String lockKey = LOCK_SHOP_KEY + id;

        // 1. 先查缓存，判断缓存是否命中，如果未命中直接返回空
        String json = stringRedisTemplate.opsForValue().get(cacheKey);
        if (StrUtil.isBlank(json)) {
            return null;
        }

        return handleLogicalCacheHit(cacheKey, lockKey, id, json, classType, function, expireTime, unit);
    }


    /**
     * 【布隆过滤器接入】逻辑过期缓存的完整查询：Redis 命中沿用旧值返回+异步重建；
     * Redis 完全缺失时先询问布隆过滤器，只有“可能存在”的 ID 才允许在互斥锁保护下查询数据库。
     * DB 仍不存在说明命中布隆假阳性，此时写空字符串短 TTL，避免同一假阳性持续回源。
     */
    public <R, ID> R queryWithBloomAndLogicalExpire(String keyPrefix,
                                                    ID id,
                                                    Class<R> classType,
                                                    Predicate<ID> bloomPredicate,
                                                    Function<ID, R> function,
                                                    Long expireTime,
                                                    TimeUnit unit) {
        String cacheKey = keyPrefix + id;
        String lockKey = LOCK_SHOP_KEY + id;
        String json = stringRedisTemplate.opsForValue().get(cacheKey);

        //查询商铺的信息如果直接命中，就判断是否逻辑过期
        if (StrUtil.isNotBlank(json)) {
            return handleLogicalCacheHit(
                    cacheKey, lockKey, id, json, classType, function, expireTime, unit);
        }
        if (json != null) {
            // 非 null 的空字符串是“数据库不存在”的空值缓存，直接返回，不能再次查询布隆或数据库。
            return null;
        }

        //先判断需要查询的id是否可能存在，不存在直接退出，存在则重建缓存
        if (!bloomPredicate.test(id)) {
            // 布隆返回 false 代表一定不存在，直接拦截随机非法 ID，数据库不会承受穿透流量。
            log.debug("店铺ID不合法!被布隆过滤器拦截: id={}", id);
            return null;
        }

        return rebuildMissingCacheWithMutex(
                cacheKey, lockKey, id, classType, function, expireTime, unit);
    }

    /**
     * 解析逻辑过期缓存。未过期直接返回；已过期仍返回旧值，并只让一个线程异步刷新。
     * 抽成公共方法是为了让原有逻辑过期查询和新增的 Bloom 查询保持完全相同的击穿保护语义。
     */
    private <R, ID> R handleLogicalCacheHit(String cacheKey,
                                            String lockKey,
                                            ID id,
                                            String json,
                                            Class<R> classType,
                                            Function<ID, R> function,
                                            Long expireTime,
                                            TimeUnit unit) {

        //如果命中：获取逻辑过期时间
        RedisData shopData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean(JSONUtil.toJsonStr(shopData.getData()), classType);
        LocalDateTime time = shopData.getExpireTime();

        //未过期：直接返回店铺信息
        if (time.isAfter(LocalDateTime.now())) {
            return r;
        }
        // 此路径有旧值可返回，因此可以异步重建：本地集合只合并线程池任务，Redisson 负责跨实例互斥。
        if (REBUILDING_KEYS.add(lockKey)) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                RLock lock = redissonClient.getLock(lockKey);
                try {
                    // Redisson 锁必须由加锁线程解锁，因此在异步任务内完成整个重建流程。
                    if (!lock.tryLock()) {
                        return;
                    }

                    //二次检查，防止重复查数据库
                    String json2 = stringRedisTemplate.opsForValue().get(cacheKey);
                    if (StrUtil.isNotBlank(json2)) {
                        RedisData data = JSONUtil.toBean(json2, RedisData.class);
                        if (data.getExpireTime().isAfter(LocalDateTime.now())) {
                            log.info("缓存已被其他线程刷新，id: {}", id);
                            return;
                        }
                    }

                    // 查询数据库并刷新缓存；若记录已被删除则写入空值，防止旧逻辑数据永久返回。
                    R r1 = function.apply(id);
                    if (r1 == null) {
                        stringRedisTemplate.opsForValue().set(
                                cacheKey, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                    } else {
                        this.setWithExpireTime(cacheKey, r1, expireTime, unit);
                    }
                } catch (Exception e) {
                    log.error("异步重建逻辑过期缓存失败: key={}", cacheKey, e);
                } finally {
                    if (lock.isHeldByCurrentThread()) {
                        lock.unlock();
                    }
                    REBUILDING_KEYS.remove(lockKey);
                }
            });
        }
        // 逻辑过期仍直接返回旧值。
        return r;
    }

    /**
     * Redis 缓存被删除或丢失时的同步恢复路径。此时没有旧值可返回，请求必须同步等待重建结果；
     * Redisson 保证同一店铺只有一个请求访问数据库，其余请求短暂等待并轮询 Redis。
     * <p>
     * 这里不使用 REBUILDING_KEYS：该集合用于避免重复提交异步线程池任务，而本方法由请求线程
     * 自己重建或等待，并不存在异步任务队列堆积问题；即使加入本地集合，其他请求仍然必须轮询
     * Redis，不能直接返回 null，因此无法替代 Redisson 锁。
     */
    private <R, ID> R rebuildMissingCacheWithMutex(String cacheKey,
                                                   String lockKey,
                                                   ID id,
                                                   Class<R> classType,
                                                   Function<ID, R> function,
                                                   Long expireTime,
                                                   TimeUnit unit) {
        for (int retry = 0; retry < 20; retry++) {

            //如果抢到锁，则由当前请求同步查询数据库并重建缓存
            RLock lock = redissonClient.getLock(lockKey);
            if (lock.tryLock()) {
                try {
                    // Double Check：等待锁期间可能已有线程完成重建，拿锁后必须重新检查 Redis。
                    String latestJson = stringRedisTemplate.opsForValue().get(cacheKey);
                    if (StrUtil.isNotBlank(latestJson)) {
                        return handleLogicalCacheHit(
                                cacheKey, lockKey, id, latestJson, classType, function, expireTime, unit);
                    }
                    if (latestJson != null) {
                        return null;
                    }

                    R loaded = function.apply(id);

                    // Bloom 允许假阳性，因此 DB 查空时仍需缓存空值，吸收相同 ID 的后续请求。
                    if (loaded == null) {
                        stringRedisTemplate.opsForValue().set(cacheKey, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                        return null;
                    }

                    this.setWithExpireTime(cacheKey, loaded, expireTime, unit);
                    return loaded;

                } finally {
                    if (lock.isHeldByCurrentThread()) {
                        lock.unlock();
                    }
                }
            }

            // 未抢到锁时等待正在重建的线程；总等待上限约 1 秒，避免请求无限阻塞。
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("等待缓存重建时线程被中断", e);
            }

            String rebuiltJson = stringRedisTemplate.opsForValue().get(cacheKey);

            //如果当前店铺的缓存已经被其它线程重建好了，直接返回结果即可
            if (StrUtil.isNotBlank(rebuiltJson)) {
                return handleLogicalCacheHit(
                        cacheKey, lockKey, id, rebuiltJson, classType, function, expireTime, unit);
            } else if (rebuiltJson != null) {
                return null;
            }

        }

        // 锁持有时间异常时不允许绕过互斥锁直接打 DB，宁可本次快速失败，保护数据库优先。
        log.warn("等待店铺缓存重建超时: key={}", cacheKey);
        return null;
    }
}
