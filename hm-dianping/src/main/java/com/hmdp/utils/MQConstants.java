package com.hmdp.utils;

/**
 * 【阶段2新增】RocketMQ 常量：topic / 消费者组 / 死信
 * 与 docs/ROCKETMQ-DEPLOY.md 中 mqadmin 创建的 topic 一一对应
 */
public final class MQConstants {

    private MQConstants() {
    }

    /**
     * 秒杀订单 topic：事务消息 COMMIT 后在此被消费者消费。
     * 消费者组只有一个 → 死信链路也只有一条（%DLQ% + 消费者组），
     * 避免为每个 topic 重复写一套消费者 + 死信补偿。
     * 事务消息的半消息对消费者不可见，COMMIT 后才成为普通消息被消费。
     */
    public static final String TOPIC_SECKILL_ORDER = "seckill_order";

    /**
     * 订单超时关单延迟消息 topic（阶段3 启用）
     */
    public static final String TOPIC_ORDER_TIMEOUT = "order_timeout";

    /**
     * 秒杀订单消费者组：重试 16 次仍失败的消息进入死信队列
     */
    public static final String GROUP_SECKILL_CONSUMER = "seckill-order-consumer";

    /**
     * 死信 topic：RocketMQ 规则为 %DLQ% + 消费者组名
     */
    public static final String TOPIC_SECKILL_DLQ = "%DLQ%" + GROUP_SECKILL_CONSUMER;

    /**
     * 死信补偿消费者组（与业务消费者组必须不同）
     */
    public static final String GROUP_SECKILL_DLQ_CONSUMER = "seckill-dlq-consumer";

    /**
     * 【阶段3新增】订单超时关单消费者组：消费失败自动重试，极端失败由阶段4扫表任务兜底
     */
    public static final String GROUP_ORDER_TIMEOUT_CONSUMER = "order-timeout-consumer";

    /**
     * 下单结果标记 TTL：1 天，覆盖客户端轮询窗口
     */
    public static final long RESULT_TTL_SECONDS = 24 * 3600L;
}
