package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
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
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@RequiredArgsConstructor
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    private final StringRedisTemplate stringRedisTemplate;

    @Override
    public List<ShopType> queryTypeList() {
        //1.先从redis中查找
        String shopTypeJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE);
        //2.判断是否为空
        if(StrUtil.isNotBlank(shopTypeJson)){
            //如果不为空就转换成集合返回
            return JSONUtil.toList(shopTypeJson, ShopType.class);
        }

        //3.如果没查到，就需要从数据库中查询然后添加到redis中,并设置过期时间
        List<ShopType> shopTypes = query().orderByAsc("sort").select().list();


        if(shopTypes==null || shopTypes.isEmpty()){
            return null;
        }
        stringRedisTemplate.opsForValue().set(
                CACHE_SHOP_TYPE,
                JSONUtil.toJsonStr(shopTypes),
                CACHE_SHOP_TTL,
                TimeUnit.MINUTES);
        return shopTypes;

    }
}
