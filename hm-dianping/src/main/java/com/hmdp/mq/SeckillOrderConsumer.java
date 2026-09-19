package com.hmdp.mq;

import com.hmdp.dto.SeckillMessage;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.MQConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 【阶段2新增】秒杀订单消费者：事务消息 COMMIT 后的唯一落库入口。
 * 正常返回 = ACK；抛出异常 = RECONSUME_LATER，Broker 自动重试 16 次后进入死信队列
 * %DLQ%seckill-order-consumer（由 SeckillOrderDlqConsumer 做最终补偿）。
 * 消息体由 rocketmq-spring 的消息转换器自动完成 JSON → SeckillMessage 反序列化。
 */

@Slf4j
@Component
@RocketMQMessageListener(
        topic = MQConstants.TOPIC_SECKILL_ORDER,
        consumerGroup = MQConstants.GROUP_SECKILL_CONSUMER)
public class SeckillOrderConsumer implements RocketMQListener<SeckillMessage> {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Override
    public void onMessage(SeckillMessage msg) {
        log.info("[秒杀落库] 收到订单消息: {}", msg);
        // 落库四道防线（幂等/锁/查重/唯一索引）在 landSeckillOrder 内实现
        voucherOrderService.landSeckillOrder(msg);
    }
}
