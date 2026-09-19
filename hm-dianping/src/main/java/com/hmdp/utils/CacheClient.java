package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {

    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(30);

    private final StringRedisTemplate stringRedisTemplate;
    public CacheClient(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate=stringRedisTemplate;
    }

    //方法1：将任意java对象序列化为json对象并存储再string类型的key中，并且可以设置ttl过期时间
    public void set(String key, Object value, Long expireTime, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),expireTime,unit);
    }

    //方法2：将任意java对象序列化为json对象并存储再string类型的key中，并且可以设置逻辑过期时间，用于处理缓存击穿问题
    public void setWithExpireTime(String key, Object value, Long expireTime, TimeUnit unit){
        //设置过期逻辑
        RedisData redisData=new RedisData();
        redisData.setData(value);
        //转换时间单位，统一为秒，设置过期时间
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(expireTime)));

        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    //方法3：根据指定的key查询缓存，并且反序列化为指定类型，利用缓存空值的方式解决穿透问题
    /*缓存穿透
     * */
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> classType,
                                         Function<ID,R> function,
                                         Long expireTime, TimeUnit unit){
        //1.根据id从redis中查询
        String cacheKey=keyPrefix+id.toString();
        String json = stringRedisTemplate.opsForValue().get(cacheKey);
        //如果不为空,把 json 转换为 bean 对象返回
        if(StrUtil.isNotBlank(json)){
            return JSONUtil.toBean(json,classType);
        }

        /*2.判断命中的是否是空值*/
        if(json!=null){
            return null;
        }

        R r = function.apply(id);
        //如果数据库中也不存在，将空值写进redis中，并设置过期时间
        if(r==null){
            stringRedisTemplate.opsForValue().set(cacheKey, "",CACHE_NULL_TTL, TimeUnit.SECONDS);
            return null;
        }
        //把查询到的结果添加到redis中，并返回
        this.set(cacheKey,r,expireTime,unit);
        return r;
    }


    //方法4：根据指定的key查询缓存，并且反序列化为指定类型，需要利用逻辑过期解决缓存击穿问题
    /*缓存击穿:逻辑过期*/
    public <R,ID> R queryWithLogicalExpire(String keyPrefix,
                                           ID id,
                                           Class<R> classType,
                                           Function<ID,R> function,
                                           Long expireTime, TimeUnit unit) {

        String cacheKey = keyPrefix + id.toString();
        String lockKey = LOCK_SHOP_KEY + id;

        // 1. 先查缓存，判断缓存是否命中，如果未命中直接返回空
        String json = stringRedisTemplate.opsForValue().get(cacheKey);
        if (StrUtil.isBlank(json)) {
            return null;
        }

        //如果命中：获取逻辑过期时间
        RedisData shopData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean(JSONUtil.toJsonStr(shopData.getData()), classType);
        LocalDateTime time = shopData.getExpireTime();

        //未过期：直接返回店铺信息
        if(time.isAfter(LocalDateTime.now())){
            return r;
        }
        //过期：获取互斥锁
        boolean flag = tryLock(lockKey);
        //判断锁是否获取成功
        //获取锁成功：开启独立线程，读取数据库信息并写入redis
        if(flag){
            CACHE_REBUILD_EXECUTOR.submit(() ->
            {
                try {
                    //二次检查，防止重复查数据库
                    String json2=stringRedisTemplate.opsForValue().get(cacheKey);
                    RedisData data = JSONUtil.toBean(json2, RedisData.class);

                    if (data.getExpireTime().isAfter(LocalDateTime.now())) {
                        log.info("缓存已被其他线程刷新，id: {}", id);
                        return;  // 已被刷新，直接返回
                    }

                    //查询数据库
                    R r1 = function.apply(id);
                    this.setWithExpireTime(cacheKey,r1,expireTime,unit);

                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }
            });
        }
        //获取锁失败，直接返回旧值
        return r;
    }

    //加锁
    private boolean tryLock(String key){
        Boolean b = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(b);
    }

    //解锁
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
}
