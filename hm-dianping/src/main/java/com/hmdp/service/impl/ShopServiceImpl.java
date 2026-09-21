package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.cache.ShopBloomFilter;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.exception.BizException;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.MultiLevelCacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

@Service
@Slf4j
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    private final StringRedisTemplate stringRedisTemplate;
    private final CacheClient cacheClient;
    /* 【阶段5新增】多级缓存客户端：L1 Caffeine 在前，L2 逻辑过期链路在后 */
    private final MultiLevelCacheClient multiLevelCacheClient;
    /* 【缓存穿透优化】Redis 分布式布隆过滤器，保存所有可能存在的店铺 ID。 */
    private final ShopBloomFilter shopBloomFilter;

    public ShopServiceImpl(StringRedisTemplate stringRedisTemplate, CacheClient cacheClient,
                           MultiLevelCacheClient multiLevelCacheClient,
                           ShopBloomFilter shopBloomFilter) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.cacheClient = cacheClient;
        this.multiLevelCacheClient = multiLevelCacheClient;
        this.shopBloomFilter = shopBloomFilter;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    @Override
    public Result queryById(Long id) {
        //Shop shop = queryWithPassThrough(id);

        //使用互斥锁实现缓存击穿
        //Shop shop = queryWithMutex(id);

        //使用逻辑过期实现缓存击穿
        //Shop shop=queryWithLogicalExpire(id);

        //使用工具类中的方法
        /*Shop shop=cacheClient.queryWithPassThrough(CACHE_SHOP_KEY,id,Shop.class,
                                      this::getById,randomCacheTtlMinutes(),TimeUnit.MINUTES);*/
        // 【阶段5重构】多级缓存：L1 Caffeine 命中直接返回（不打 Redis）；
        // 未命中走 L2 逻辑过期链路（互斥重建语义不变）并回填 L1
        // 【缓存雪崩优化】每次发生真实缓存写入时使用 20~30 分钟随机逻辑 TTL，
        // 避免启动预热的一批店铺在同一时刻逻辑过期并集中提交数据库重建任务。
        Shop shop = multiLevelCacheClient.queryWithBloomAndLogicalExpire(
                CACHE_SHOP_KEY,
                id,
                Shop.class,
                this.shopBloomFilter::mightContain,
                this::getById,
                randomCacheTtlMinutes(), TimeUnit.MINUTES);

        if (shop == null) { //说明查到了空值
            return Result.fail("店铺信息不存在!");
        }
        return Result.ok(shop);
    }


    /**
     * 新增店铺与布隆过滤器的一致性写入。
     * DB 插入后、事务提交前写 Bloom：Bloom 写失败会抛异常并回滚数据库，避免真实 ID 未进入过滤器；
     * 若 DB 最终回滚但 Bloom 已写，只会产生可接受的假阳性，不会误杀真实数据。
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public Result createShop(Shop shop) {
        if (shop == null) {
            return Result.fail("店铺信息不能为空");
        }
        //把数据插入数据库中
        if (!save(shop) || shop.getId() == null) {
            return Result.fail("新增店铺失败");
        }

        try {
            //同步新增到布隆过滤器中
            shopBloomFilter.add(shop.getId());
        } catch (RuntimeException e) {
            // 这里必须抛出异常触发数据库回滚，不能吞掉异常后留下 Bloom 假阴性。
            log.error("新增店铺同步布隆过滤器失败，回滚数据库: shopId={}", shop.getId(), e);
            throw new BizException("新增店铺失败，请稍后重试");
        }

        // 缓存必须在数据库提交后写入；若事务最终回滚，提前缓存会产生并不存在的幽灵店铺。
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    cacheClient.setWithExpireTime(
                            CACHE_SHOP_KEY + shop.getId(), shop,
                            randomCacheTtlMinutes(), TimeUnit.MINUTES);
                } catch (RuntimeException e) {
                    // DB 与 Bloom 已经成功，缓存失败可以由下一次查询安全回源恢复，不能反向影响已提交事务。
                    log.error("新增店铺提交后预热缓存失败，等待查询链路自愈: shopId={}", shop.getId(), e);
                }
            }
        });

        return Result.ok(shop.getId());
    }



    //更新店铺信息
    @Override
    @Transactional(rollbackFor = Exception.class)
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺信息不完整!");
        }
        //1.修改数据库中的信息，只更新非null字段
        boolean success = updateById(shop);
        if (!success) {
            return Result.fail("更新店铺信息失败");
        }

        //2.缓存一致性组合拳【阶段5修复：删除改覆盖写】：
        // 逻辑过期读路径(queryWithLogicalExpire)在 L2 miss 时【不查 DB】，若此处用 delete 删 L2，
        // 会造成"更新后到下次预热前该商铺查询一律 miss"的真空窗口。
        // 正统逻辑过期姿势是【覆盖写】：读回 DB 最新全量 → 以逻辑过期包装写入 L2（数据即时最新，
        // 逻辑过期时间顺延），查询链路零真空；随后广播删各实例 L1（L1 无逻辑过期概念，删除+懒加载回填即可）
        Shop latest = getById(id);
        if (latest != null) {
            // 【缓存雪崩优化】更新店铺后的覆盖写也重新生成随机逻辑 TTL，
            // 防止批量更新操作把大量 Key 的下一次逻辑过期时间重新对齐。
            cacheClient.setWithExpireTime(
                    CACHE_SHOP_KEY + id, latest, randomCacheTtlMinutes(), TimeUnit.MINUTES);
        }
        multiLevelCacheClient.invalidateAndBroadcast(CACHE_SHOP_KEY + id);
        log.info("更新店铺信息成功!");

        return Result.ok();
    }


    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;

        // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs
                                .newGeoSearchArgs()
                                .includeDistance() //返回距离当前请求的距离
                                .sortAscending() //按照距离进行升序排列
                                .limit(end)
                );
        // 4.解析出id

        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();

        //防止ids空拼接
        if (list.size() <= from) {
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());

        Map<Long, Distance> distanceMap = new HashMap<>();
        list.stream().skip(from).forEach(result -> {
            //获取店铺id
            Long shopId = Long.valueOf(result.getContent().getName());
            ids.add(shopId);
            //获取距离
            Distance distance = result.getDistance();

            distanceMap.put(shopId, distance);
        });

        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }


    //加锁
    private boolean tryLock(String key) {
        Boolean b = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(b);
    }

    //解锁
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
















    /*=========================================================================================================================*/

    /*缓存击穿:逻辑过期*/
    public Shop queryWithLogicalExpire(Long id) {
        String cacheKey = CACHE_SHOP_KEY + id;
        String lockKey = LOCK_SHOP_KEY + id;
        // 1. 先查缓存，判断缓存是否命中，如果未命中直接返回空
        String shopJson = stringRedisTemplate.opsForValue().get(cacheKey);
        //如果未命中，直接返回空值即可
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }
        //如果命中：获取逻辑过期时间
        RedisData shopData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) shopData.getData(), Shop.class);
        LocalDateTime expireTime = shopData.getExpireTime();
        //未过期：直接返回店铺信息
        if (expireTime.isAfter(LocalDateTime.now())) {
            return shop;
        }
        //过期：获取互斥锁
        boolean flag = tryLock(lockKey);
        //判断锁是否获取成功
        //获取锁成功：开启独立线程，读取数据库信息并写入redis
        if (flag) {
            CACHE_REBUILD_EXECUTOR.submit(() ->
            {
                try {
                    //二次检查，防止重复查数据库
                    String json = stringRedisTemplate.opsForValue().get(cacheKey);
                    RedisData data = JSONUtil.toBean(json, RedisData.class);
                    if (data.getExpireTime().isAfter(LocalDateTime.now())) {
                        log.info("缓存已被其他线程刷新，id: {}", id);
                        return;  // 已被刷新，直接返回
                    }
                    // 【缓存雪崩优化】备用逻辑过期实现同样使用分钟级随机 TTL；旧代码不仅固定为 30，
                    // 还误用了 SECONDS，可能让缓存约 30 秒就过期，现在统一为 20~30 分钟。
                    cacheClient.setWithExpireTime(
                            cacheKey, shop, randomCacheTtlMinutes(), TimeUnit.MINUTES);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        //获取锁失败，直接返回旧值
        return shop;
    }


    /*缓存击穿:互斥锁
     * */
    public Shop queryWithMutex(Long id) {
        String cacheKey = CACHE_SHOP_KEY + id;
        String lockKey = LOCK_SHOP_KEY + id;
        // 1. 先查缓存，如果命中直接返回
        String shopJson = stringRedisTemplate.opsForValue().get(cacheKey);
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        if (shopJson != null) {
            return null; // 命中空值（防穿透）
        }
        // 2. 尝试获取锁重建缓存（使用循环 + 限次重试，替代递归）
        int retryCount = 0, maxRetry = 10; // 最多重试 10 次，防止死循环

        while (retryCount < maxRetry) {
            boolean isLock = tryLock(lockKey);
            if (isLock) {
                // 获取锁成功
                try {
                    // Double Check：拿到锁后再次检查缓存，防止前一个抢锁线程已经重建完毕
                    shopJson = stringRedisTemplate.opsForValue().get(cacheKey);
                    if (StrUtil.isNotBlank(shopJson)) {
                        return JSONUtil.toBean(shopJson, Shop.class);
                    }
                    //防止缓存穿透，当前查到的是空字符串
                    if (shopJson != null) {
                        return null;
                    }
                    // 查数据库
                    Shop shop = getById(id);
                    if (shop == null) {
                        // 数据库不存在，写入空值解决缓存穿透
                        stringRedisTemplate.opsForValue().set(cacheKey, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                        return null;
                    }
                    // 数据库存在，写入缓存
                    // 【缓存雪崩优化】物理 TTL 也按 Key 独立随机，避免备用互斥锁方案启用时批量失效。
                    stringRedisTemplate.opsForValue().set(
                            cacheKey, JSONUtil.toJsonStr(shop), randomCacheTtlMinutes(), TimeUnit.MINUTES);
                    return shop;
                } finally {
                    // 释放锁
                    unlock(lockKey);
                }
            }
            // 获取锁失败，休眠 20 毫秒后循环重试
            try {
                Thread.sleep(LOCK_SHOP_TTL);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            retryCount++;
        }

        // 超过最大重试次数仍未拿到锁，降级处理（抛异常或返回系统繁忙）
        throw new RuntimeException("系统繁忙，请稍后再试！");
    }

    /*缓存穿透
     * */
    public Shop queryWithPassThrough(Long id) {
        //1.根据id从redis中查询
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //如果不为空,把 json 转换为 bean 对象返回
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        /*2.如果redis中查不到，从数据库中查询

        判断命中的是否是空值*/
        if (shopJson != null) {
            return null;
        }
        Shop shop = getById(id);
        //如果数据库中也不存在，将空值写进redis中，并设置过期时间
        if (shop == null) {
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //把查询到的结果添加到redis中，并返回
        // 【缓存雪崩优化】穿透查询回填正常数据时设置 20~30 分钟随机物理 TTL，
        // 空值仍保持独立的短 TTL，避免不存在的数据长期占用缓存。
        stringRedisTemplate.opsForValue().set(
                CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop),
                randomCacheTtlMinutes(), TimeUnit.MINUTES);
        return shop;
    }


}
