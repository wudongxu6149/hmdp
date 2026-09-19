package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillMessage;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    /* ========== 【阶段2重构】以下方法取代原 createVoucherOrder（Redis Stream 落库） ========== */

    /**
     * 秒杀订单落库：由 SeckillOrderConsumer 调用。
     * 含幂等、Redisson 锁、业务查重、乐观扣减、唯一索引兜底（方法内注释详述）
     */
    void landSeckillOrder(SeckillMessage msg);

    /**
     * 死信补偿：重试 16 次仍落库失败的消息，回补 Redis 库存与一人一单资格
     */
    void compensateDlqSeckillOrder(SeckillMessage msg);

    /**
     * 下单结果轮询：读结果标记，兜底查订单表
     */
    Result querySeckillResult(Long orderId);

    /* ========== 【阶段3新增】支付与超时关单 ========== */

    /**
     * 模拟支付：CAS 条件更新 status 1→2（WHERE status=1），
     * 影响行数=0 说明已被关单/已支付，返回幂等冲突提示——与关单共用同一状态约束，一单一终态
     */
    Result payOrder(Long orderId);

    /**
     * 关单延迟消息消费入口（OrderTimeoutConsumer 调用）：
     * 到期则关单；未到期（消息早到）按剩余时长重投
     */
    void handleOrderTimeout(SeckillMessage msg);

    /**
     * CAS 关单并回补资源：status 1→4 且 active_flag=orderId（释放一人一单位），
     * 成功后回补 DB/Redis 库存并解除资格。返回是否真正由本次调用关闭（供阶段4扫表任务复用）
     */
    boolean tryCloseOrder(Long orderId);
}
