package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.benmanes.caffeine.cache.Cache;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * 【阶段5重构】商铺类型列表接入多级缓存：L1 Caffeine → L2 Redis → DB。
 * 类型是变更极少的字典数据，最适合本地缓存——热点读几乎不再出应用进程
 */
@Service
@RequiredArgsConstructor
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    private final StringRedisTemplate stringRedisTemplate;
    /* 【阶段5新增】L1 本地缓存（CacheConfig 装配，key 与 L2 一致便于失效对齐） */
    private final Cache<String, String> localCache;

    @Override
    public List<ShopType> queryTypeList() {
        // 1.【阶段5新增】L1 本地缓存命中直接返回（不打 Redis）
        String l1Json = localCache.getIfPresent(CACHE_SHOP_TYPE);
        if (StrUtil.isNotBlank(l1Json)) {
            return JSONUtil.toList(l1Json, ShopType.class);
        }
        // 2. L2 Redis：命中后回填 L1（存原始 JSON，与 L2 同源同形态）
        String shopTypeJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE);
        if (StrUtil.isNotBlank(shopTypeJson)) {
            List<ShopType> shopTypes = JSONUtil.toList(shopTypeJson, ShopType.class);
            localCache.put(CACHE_SHOP_TYPE, shopTypeJson);
            return shopTypes;
        }
        // 3. DB 兜底，写回两级
        List<ShopType> shopTypes = query().orderByAsc("sort").select().list();
        if (shopTypes == null || shopTypes.isEmpty()) {
            return null;
        }
        shopTypeJson = JSONUtil.toJsonStr(shopTypes);
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_TYPE, shopTypeJson, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        localCache.put(CACHE_SHOP_TYPE, shopTypeJson);
        return shopTypes;
    }
}
