package com.hmdp.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 【阶段2新增】秒杀下单消息体：生产者（事务消息发送端）与消费者之间的契约。
 * orderId 必须随消息携带——它是幂等判定、事务标记、结果轮询三者的关联键。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeckillMessage {

    /** 秒杀券 id */
    private Long voucherId;

    /** 下单用户 id */
    private Long userId;

    /** 全局订单号（RedisIdWorker 生成，落库为订单主键） */
    private Long orderId;
}
