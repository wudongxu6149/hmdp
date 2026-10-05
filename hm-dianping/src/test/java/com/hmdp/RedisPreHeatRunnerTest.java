package com.hmdp;

import com.hmdp.cache.ShopBloomFilter;
import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisPreHeatRunner;
import org.junit.jupiter.api.Test;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.GeoOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class RedisPreHeatRunnerTest {

    @Test
    void shopPreheatPopulatesGeoIndexForEachType() {
        RedisPreHeatRunner runner = new RedisPreHeatRunner();
        IShopService shopService = mock(IShopService.class);
        CacheClient cacheClient = mock(CacheClient.class);
        ShopBloomFilter bloomFilter = mock(ShopBloomFilter.class);
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        GeoOperations<String, String> geo = mock(GeoOperations.class);
        ReflectionTestUtils.setField(runner, "shopService", shopService);
        ReflectionTestUtils.setField(runner, "cacheClient", cacheClient);
        ReflectionTestUtils.setField(runner, "shopBloomFilter", bloomFilter);
        ReflectionTestUtils.setField(runner, "stringRedisTemplate", redis);
        Shop first = new Shop().setId(1L).setTypeId(1L).setX(120.149192).setY(30.316078);
        Shop second = new Shop().setId(2L).setTypeId(1L).setX(120.151505).setY(30.333422);
        Shop third = new Shop().setId(10L).setTypeId(2L).setX(120.149093).setY(30.324666);
        when(shopService.list()).thenReturn(List.of(first, second, third));
        when(redis.opsForGeo()).thenReturn(geo);

        runner.preHeatData();

        verify(geo).add(SHOP_GEO_KEY + "1", new Point(120.149192, 30.316078), "1");
        verify(geo).add(SHOP_GEO_KEY + "1", new Point(120.151505, 30.333422), "2");
        verify(geo).add(SHOP_GEO_KEY + "2", new Point(120.149093, 30.324666), "10");
        verifyNoMoreInteractions(geo);
        verify(bloomFilter).initialize(List.of(1L, 2L, 10L));
        for (Shop shop : List.of(first, second, third)) {
            verify(cacheClient).setWithExpireTime(
                    eq(CACHE_SHOP_KEY + shop.getId()), eq(shop), anyLong(), eq(TimeUnit.MINUTES));
        }
    }
}
