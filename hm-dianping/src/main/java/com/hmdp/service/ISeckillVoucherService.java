package com.hmdp.service;

import com.hmdp.entity.SeckillVoucher;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 * 秒杀优惠券表，与优惠券是一对一关系 服务类
 * </p>
 *
 * @author 虎哥
 * @since 2022-01-04
 */
public interface ISeckillVoucherService extends IService<SeckillVoucher> {

    /**
     * 【阶段4新增】秒杀券元数据写入 Redis Hash（seckill:voucher:{id}：stock/beginTime/endTime）。
     * 唯一写入点：新增秒杀券（VoucherServiceImpl）与启动预热（RedisPreHeatRunner）共用，
     * 字段名与 seckill.lua 的 HGET 读取严格对应，防止两处各写一套造成漂移
     */
    void preHeatRedisMeta(SeckillVoucher seckillVoucher);
}
