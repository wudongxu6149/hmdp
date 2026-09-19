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
 * 【阶段3新增】订单超时关单消费者。
 * 秒杀订单落库成功时由 landSeckillOrder 发出延迟消息（TIMER_DELIVER_MS = 下单+15分钟），
 * 到期投递到本消费者：status=1 则 CAS 关单并回补 DB/Redis 库存与一人一单资格；
 * status≠1（已支付/已关闭）幂等 ACK；消息早到按剩余时长重投。
 * 消费异常自动重试，极端失败进入 %DLQ%order-timeout-consumer——
 * 该场景的最终兜底是阶段4的扫表关单定时任务（ReconciliationTask）
 */
@Slf4j
@Component
@RocketMQMessageListener(
        topic = MQConstants.TOPIC_ORDER_TIMEOUT,
        consumerGroup = MQConstants.GROUP_ORDER_TIMEOUT_CONSUMER)
public class OrderTimeoutConsumer implements RocketMQListener<SeckillMessage> {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Override
    public void onMessage(SeckillMessage msg) {
        log.info("[关单] 收到超时检查消息: orderId={}", msg.getOrderId());
        voucherOrderService.handleOrderTimeout(msg);
    }
}
