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
 * 【阶段2新增】秒杀订单死信消费者：业务消费者重试 16 次仍失败的消息最终汇入 %DLQ% 组，
 * 由本消费者执行兜底补偿——解决旧链路"Redis 库存扣了但订单永远没落库"的不一致。
 * 注意：若控制台/权限导致 %DLQ% topic 不可读，需 mqadmin updateTopicPerm 放开读权限。
 */
@Slf4j
@Component
@RocketMQMessageListener(
        topic = MQConstants.TOPIC_SECKILL_DLQ,
        consumerGroup = MQConstants.GROUP_SECKILL_DLQ_CONSUMER)
public class SeckillOrderDlqConsumer implements RocketMQListener<SeckillMessage> {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Override
    public void onMessage(SeckillMessage msg) {
        log.error("[死信] 秒杀订单消息最终消费失败, {}", msg);
        voucherOrderService.compensateDlqSeckillOrder(msg);
    }
}
