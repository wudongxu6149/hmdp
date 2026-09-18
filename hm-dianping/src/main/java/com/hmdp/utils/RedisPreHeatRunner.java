package com.hmdp.utils;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

@Component
@Slf4j
public class RedisPreHeatRunner  {

    /*
    * 在项目启动后预热店铺信息
    * */

    @Resource
    private IShopService shopService;
    @Resource
    private CacheClient cacheClient;

    //监听当所有的连接池、代理对象加载完毕后初始化商铺信息
    @EventListener(ApplicationReadyEvent.class)
    public void preHeatData() {
        //获取全部的店铺信息
        List<Shop> list = shopService.list();
        list.forEach(shop-> {
            String key=CACHE_SHOP_KEY+shop.getId();
            cacheClient.setWithExpireTime(key,shop,CACHE_SHOP_TTL, TimeUnit.MINUTES);
        });
        log.info("已完成店铺预热");
    }
}
