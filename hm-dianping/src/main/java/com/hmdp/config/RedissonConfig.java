package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {

    @Bean
    public RedissonClient redissonClient(){
        //配置redisson
        Config config=new Config();
        //添加redis单机的地址，如果是redis集群的话使用useClusterServers()添加集群地址
        config.useSingleServer().setAddress("redis://192.168.199.128:6379").setPassword("123456");

        return Redisson.create(config);
    }
}
