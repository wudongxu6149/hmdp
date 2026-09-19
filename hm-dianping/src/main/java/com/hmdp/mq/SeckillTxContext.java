package com.hmdp.mq;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

/**
 * 【阶段2新增】事务消息的上下文：生产者创建，随 sendMessageInTransaction 的 arg 传入事务监听器。
 * executeLocalTransaction 与发送线程【同步】执行，Lua 判定结果写回 luaResult 后，
 * 生产者 send 返回即可据此给用户明确答复（资格失败立即报错，成功则异步落库）。
 */
@Getter
@RequiredArgsConstructor
public class SeckillTxContext {

    private final Long voucherId;
    private final Long userId;
    private final Long orderId;

    /**
     * Lua 执行结果：0=有资格(已预扣) 1=库存不足 2=重复下单 3=秒杀未开始 4=已结束 -1=执行异常(状态未知)
     */
    @Setter
    private volatile int luaResult = -1;
}
