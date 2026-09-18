package com.hmdp.utils.lock.lockImp;

import cn.hutool.core.lang.UUID;
import com.hmdp.utils.lock.ILock;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleLock implements ILock {

    private StringRedisTemplate stringRedisTemplate;
    private String name;

    public SimpleLock(String name,StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true)+"-";

    //初始化lua脚本
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static{
        UNLOCK_SCRIPT=new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("scripts/unLock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        //1.获取当前线程的唯一ID
        String id = ID_PREFIX + Thread.currentThread().getId();
        //2.尝试获取锁
        Boolean success = stringRedisTemplate.opsForValue().
                setIfAbsent(KEY_PREFIX + name, id, timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unLock() {
        //调用lua脚本
        stringRedisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + name),
                ID_PREFIX + Thread.currentThread().getId()
        );

    }

    /*@Override
    //问题出在获取和删除不是一个原子操作，可能会出现误删别人的锁的情况
    public void unLock() {
        //获取当前线程的标识
        String currentID = ID_PREFIX + Thread.currentThread().getId();
        //判断当前线程标识和缓存中的是否一致
        String value = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);

        if (StrUtil.isNotBlank(value)) { //防止锁已过期或被删除
            if (value.equals(currentID)) {
                stringRedisTemplate.delete(KEY_PREFIX + name);
            }
        }

    }*/
}
