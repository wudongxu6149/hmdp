package com.hmdp;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Shop;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.*;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private CacheClient cacheClient;
    @Resource
    private ShopServiceImpl shopService;
    @Autowired
    private RedisIdWorker redisIdWorker;
    @Autowired
    private UserMapper userMapper;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Test
    public void testSaveShop(){
        int id=2;
        Shop shop = shopService.getById(id);
        cacheClient.setWithExpireTime(CACHE_SHOP_KEY+id,shop,10L, TimeUnit.SECONDS);
    }

    @Test
    public void testRedisIdWorker(){
        for(int i=0;i<500;i++){
            long order = redisIdWorker.nextId("order");
            System.out.println(order);
        }
    }

    @Test
    public void testUserInfo()  throws IOException {
        BufferedWriter bufferedWriter=new BufferedWriter(new FileWriter(new File("D:\\Desktop\\tokens.txt")));

        //查询数据库中全部的用户信息
        QueryWrapper<User> queryWrapper=new QueryWrapper<>();
        queryWrapper.select("id","nick_name","icon");
        List<User> users = userMapper.selectList(queryWrapper);


        //转换类型
        List<UserDTO> userList = users.stream().map((User user)->  BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());

        for (UserDTO u : userList) {
            String token = UUID.randomUUID().toString(true);
            Map<String, Object> userMap = BeanUtil.beanToMap(
                    u,
                    new HashMap<>(),
                    CopyOptions.create().setFieldValueEditor((filedName, fieldValue) ->  fieldValue==null?null:fieldValue.toString())
            );

            //写出token本地文件中
            stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY+token,userMap);
            bufferedWriter.write(token);
            bufferedWriter.newLine();
        }

        bufferedWriter.close();


    }

    @Test
    void loadShopData() {
        // 1.查询店铺信息
        List<Shop> list = shopService.list();
        // 2.把店铺分组，按照typeId分组，typeId一致的放到一个集合
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        // 3.分批完成写入Redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            // 3.1.获取类型id
            Long typeId = entry.getKey();
            String key = SHOP_GEO_KEY + typeId;
            // 3.2.获取同类型的店铺的集合
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations=new ArrayList<>();
            // 3.3.写入redis GEOADD key 经度 纬度 member
            for (Shop shop : value) {
                // stringRedisTemplate.opsForGeo().add(key, new Point(shop.getX(), shop.getY()), shop.getId().toString());
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(), shop.getY())
                ));
            }
            stringRedisTemplate.opsForGeo().add(key, locations);
        }
    }

}
