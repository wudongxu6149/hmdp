package com.hmdp.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.hmdp.listener.CacheInvalidateListener;
import com.hmdp.utils.RedisConstants;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.time.Duration;

/**
 * 【阶段5新增】多级缓存的 L1 装配 + 失效广播订阅容器。
 *
 * 多级缓存整体设计：
 *   L1 = Caffeine（本机内存，纳秒级访问，短 TTL）——挡住热点商品的绝大部分读
 *   L2 = Redis（逻辑过期/互斥锁语义全部保留在 CacheClient）——跨实例共享的真数据源缓存
 *   DB = 兜底
 *
 * 多实例一致性：写库后删除 Redis + 发布失效广播 → 每个实例订阅同一频道删除各自的 L1。
 * Pub/Sub 是尽力而为（实例宕机期间的消息收不到），L1 的 60s 短 TTL 兜住这种边界——脏读窗口有上界
 */
@Configuration
public class CacheConfig {

    /**
     * L1 本地缓存：值统一存 JSON 字符串（与 L2 的存储形态一致，反序列化口径统一）。
     * maximumSize 防内存膨胀（超容量按访问频率淘汰）；expireAfterWrite 60s 是
     * "广播丢失时的最大脏读窗口"，二者共同保证 L1 不会长期偏离 L2
     */
    @Bean
    public Cache<String, String> localCache() {
        return Caffeine.newBuilder()
                .maximumSize(10000L)
                .expireAfterWrite(Duration.ofSeconds(60))
                .build();
    }

    /**
     * Pub/Sub 订阅容器：每个实例启动时都订阅 cache:invalidate 频道，
     * 收到消息后由 CacheInvalidateListener 删除本实例对应的 L1 条目
     */
    @Bean
    public RedisMessageListenerContainer cacheInvalidateContainer(
            RedisConnectionFactory connectionFactory,
            CacheInvalidateListener listener) {

        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(listener, new ChannelTopic(RedisConstants.CACHE_INVALIDATE_TOPIC));

        return container;
    }
}
