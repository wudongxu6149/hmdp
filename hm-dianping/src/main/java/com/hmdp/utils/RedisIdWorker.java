package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class RedisIdWorker {

    private static final Long BEGIN_TIMESTAMP = 1767225600L;
    private static final int COUNT_BITS = 32;

    private final StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long nextId(String keyPreFix) {
        //1.生成时间戳
        LocalDateTime now = LocalDateTime.now();

        long endTime = now.toEpochSecond(ZoneOffset.UTC);
        long timeStamp = endTime - BEGIN_TIMESTAMP;
        //2.生成序列号
        String date=now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        //2.1 记录每天生成的订单次数（自增长）
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPreFix + ":"+date);

        //3.拼接并返回
        return timeStamp << COUNT_BITS | count;

    }

    //生成一个初始时间
    public static void main(String[] args) {
        LocalDateTime basicTime = LocalDateTime.of(2026, 1, 1, 0, 0, 0);
        long epochSecond = basicTime.toEpochSecond(ZoneOffset.UTC);
        log.info(String.valueOf(epochSecond));
    }
}
