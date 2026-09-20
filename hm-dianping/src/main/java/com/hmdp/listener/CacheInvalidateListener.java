package com.hmdp.listener;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * 【阶段5新增】缓存失效广播订阅者：收到 cache:invalidate 频道消息后删除【本实例】的 L1 缓存。
 * 消息体约定为完整的缓存 key（如 cache:shop:1），与 L1 的 key 命名一致，收到即失效。
 *
 * 注意 Pub/Sub 语义：发布时不在场的实例收不到这条消息——其 L1 会靠 60s 短 TTL 自然过期，
 * 脏读窗口有上界，这是设计内的取舍而非缺陷
 */
@Slf4j
@Component
public class CacheInvalidateListener implements MessageListener {

    private final Cache<String, String> localCache;

    public CacheInvalidateListener(Cache<String, String> localCache) {
        this.localCache = localCache;
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String key = new String(message.getBody(), StandardCharsets.UTF_8);
        localCache.invalidate(key);
        log.info("[多级缓存] 收到失效广播, 已删除本实例 L1: {}", key);
    }
}
