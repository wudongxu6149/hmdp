package com.hmdp.utils;

import java.util.concurrent.ThreadLocalRandom;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 30L;

    public static final Long CACHE_NULL_TTL = 2L;

    /**
     * 【缓存雪崩优化】店铺相关缓存不再使用统一的固定 30 分钟过期时间，而是在 20~30 分钟间随机。
     * 最小值和最大值统一放在常量类中，确保启动预热、异步重建和普通缓存回填使用同一套边界。
     */
    public static final long CACHE_SHOP_TTL_MINUTES = 20L;
    public static final long CACHE_SHOP_TTL_MAX_MINUTES = 30L;

    public static final String CACHE_SHOP_KEY = "cache:shop:";
    public static final String CACHE_SHOP_TYPE = "cache:shop:type";

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 20L;

    public static final String SECKILL_VOUCHER_KEY = "seckill:voucher:";
    /* 【阶段2新增】秒杀链路三个 key：资格 Set / 事务标记 / 下单结果标记 */
    public static final String SECKILL_ORDER_KEY = "seckill:order:";

    //lua脚本执行结果凭证，用来给发生异常时候，broker根据UNKNOWN回查这个标记
    public static final String SECKILL_TX_KEY = "seckill:tx:";

    public static final String SECKILL_RESULT_KEY = "seckill:result:";
    /* 【阶段4新增】Redis 库存退票幂等标记：seckill:refund:{orderId}，防同一消息重放导致重复回补 */
    public static final String SECKILL_REFUND_KEY = "seckill:refund:";

    /* 【阶段5新增】多级缓存失效广播频道（Pub/Sub）：更新方发布，所有实例订阅并删除各自 L1 */
    public static final String CACHE_INVALIDATE_TOPIC = "cache:invalidate";

    /* 【阶段7新增】点赞并发互斥标记：blog:like:guard:{blogId}:{userId}，防双击连点导致计数重复加减 */
    public static final String BLOG_LIKE_GUARD_KEY = "blog:like:guard:";
    /* 【阶段7新增】Feed 收件箱封顶：每个用户的收件箱只保留最近 N 条（防止无限膨胀） */
    public static final long FEED_INBOX_MAX = 1000L;

    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";

    /**
     * 【缓存雪崩优化】为每一次缓存写入生成独立 TTL，返回范围为 [20, 30] 分钟（包含两端）。
     * 使用 ThreadLocalRandom 避免并发请求共享 Random 带来的竞争；max + 1 是因为 nextLong 的上界不包含。
     * 不同 Key 的过期/逻辑过期时间被打散后，就不会在统一的第 30 分钟同时触发回源或缓存重建。
     */
    public static long randomCacheTtlMinutes() {
        return ThreadLocalRandom.current().nextLong(
                CACHE_SHOP_TTL_MINUTES, CACHE_SHOP_TTL_MAX_MINUTES + 1);
    }
}
