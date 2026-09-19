package com.hmdp.task;

import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_VOUCHER_KEY;

/**
 * 【阶段4新增】定时对账任务——数据一致性保障的"最后一环"：
 * 事务消息/延迟消息/回补链路负责主路径尽量不出错，本任务负责【错了之后收敛】。
 *
 * 三个子任务（每轮依次执行，任一失败不影响其余，下一轮重试）：
 *   ① 库存对账：以订单表+DB库存为账本，校正 Redis 库存（含"只修虚高"的进行中保护，见方法注释）
 *   ② 扫表关单：status=1 且超时的订单走 tryCloseOrder（复用阶段3的 CAS 关单+回补），
 *      兜底"延迟消息丢失/关单消费者不可用"导致的订单永久冻结库存
 *   ③ 资格核对：已结束秒杀券的资格 Set 与 DB 有效订单差集核对，清理幽灵资格
 *
 * 双实例部署下的互斥：Redisson 锁 tryLock 快速失败——同一时刻只有一个实例执行本轮对账，
 * 抢不到锁的实例直接跳过（对账下一轮还会跑，无需等待）。
 * 本任务所有查询均用字符串列名（MP 3.4.3 + JDK17 下 lambdaQuery 有反射兼容问题，见预热任务注释）。
 */
@Slf4j
@Component
public class ReconciliationTask {

    /** 对账互斥锁：双实例同一时刻只允许一个执行 */
    private static final String RECONCILE_LOCK_KEY = "lock:task:reconcile";
    /** 已结束券的对账回溯窗口：只处理最近 7 天结束的券，避免任务无限膨胀 */
    private static final int ENDED_SCAN_DAYS = 7;
    /** 扫表关单单轮批次上限：防单轮锁持有时间过长 */
    private static final int CLOSE_BATCH_LIMIT = 100;

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    /**
     * 【阶段4新增】关单超时分钟数：与关单消费者（OrderTimeoutConsumer 链路）读同一个配置项，
     * 两条关单路径的超时口径天然一致，改 yaml 一处即同步
     */
    @Value("${seckill.pay-timeout-minutes:15}")
    private long payTimeoutMinutes;

    /**
     * 对账主入口：默认每分钟一轮（首轮延迟 30s，避开启动预热高峰）
     */
    @Scheduled(initialDelayString = "${seckill.reconcile-initial-delay-ms:30000}",
            fixedDelayString = "${seckill.reconcile-interval-ms:60000}")
    public void reconcile() {
        RLock lock = redissonClient.getLock(RECONCILE_LOCK_KEY);
        // 快速失败：另一实例正在对账，本轮直接跳过
        if (!lock.tryLock()) {
            return;
        }
        try {
            try {
                reconcileStock();
            } catch (Exception e) {
                log.error("[对账] 库存对账异常, 下轮重试", e);
            }
            try {
                closeTimeoutOrders();
            } catch (Exception e) {
                log.error("[对账] 扫表关单异常, 下轮重试", e);
            }
            try {
                checkQualification();
            } catch (Exception e) {
                log.error("[对账] 资格核对异常, 下轮重试", e);
            }
        } finally {
            lock.unlock();
        }
    }

    /* ==================== ① 库存对账 ==================== */

    /**
     * 库存对账。核心公式：账本口径下，静默时刻（无在途消息）Redis 库存应当 == DB 库存。
     * 在途消息（Lua 已预扣、订单未落库）会让 Redis 短暂低于 DB——这是健康瞬时态，不是漂移。
     * 因此对进行中的券只修"虚高"（Redis > DB 数学上必为错：每个有效订单对两侧各扣一次，
     * 回补路径两侧各加一次，Redis 任何高于 DB 的值都对应一次无订单的净加库存）；
     * 低于 DB 只告警观察。已结束的券在途已排空，双向校正。
     */
    private void reconcileStock() {
        LocalDateTime now = LocalDateTime.now();
        // 进行中的券（含未开始）：只修虚高
        List<SeckillVoucher> ongoing = seckillVoucherService.query()
                .gt("end_time", now).list();
        for (SeckillVoucher sv : ongoing) {
            compareAndFixStock(sv, false);
        }
        // 最近 7 天结束的券：在途已排空，双向校正
        List<SeckillVoucher> ended = seckillVoucherService.query()
                .gt("end_time", now.minusDays(ENDED_SCAN_DAYS))
                .le("end_time", now).list();
        for (SeckillVoucher sv : ended) {
            compareAndFixStock(sv, true);
        }
    }

    /**
     * 单券库存比对：redisStock vs dbStock（DB 为账本）
     *
     * @param allowFixLow true=允许把低于 DB 的 Redis 向上修正（仅用于已结束、在途已排空的券）
     */
    private void compareAndFixStock(SeckillVoucher sv, boolean allowFixLow) {
        String key = SECKILL_VOUCHER_KEY + sv.getVoucherId();
        Object redisStockObj = stringRedisTemplate.opsForHash().get(key, "stock");
        if (redisStockObj == null) {
            // Redis 库存丢失（数据丢失/误删）：以 DB 补写，让秒杀恢复可用
            log.warn("[对账] Redis 库存缺失, 以 DB 补写, voucherId={}, dbStock={}",
                    sv.getVoucherId(), sv.getStock());
            seckillVoucherService.preHeatRedisMeta(sv);
            return;
        }
        int redisStock = Integer.parseInt(redisStockObj.toString());
        int dbStock = sv.getStock() == null ? 0 : sv.getStock();
        if (redisStock > dbStock) {
            // 虚高：重复回补/人工误操作等造成——以账本为准向下修正
            log.error("[对账] Redis 库存虚高已修正! voucherId={}, redis={}, db={}",
                    sv.getVoucherId(), redisStock, dbStock);
            stringRedisTemplate.opsForHash().put(key, "stock", String.valueOf(dbStock));
        } else if (redisStock < dbStock) {
            if (allowFixLow) {
                // 已结束仍有缺口：在途不可能存在，说明发生了未回补的净扣减——向上修正
                log.error("[对账] 已结束券 Redis 库存缺口已修正! voucherId={}, redis={}, db={}",
                        sv.getVoucherId(), redisStock, dbStock);
                stringRedisTemplate.opsForHash().put(key, "stock", String.valueOf(dbStock));
            } else {
                // 进行中的正常瞬时态（在途消息）或未回补的漏损——无法区分，仅告警观察
                log.warn("[对账] Redis 库存低于 DB（在途消息或漏损）, voucherId={}, redis={}, db={}",
                        sv.getVoucherId(), redisStock, dbStock);
            }
        }
    }

    /* ==================== ② 扫表关单兜底 ==================== */

    /**
     * 扫表关单：查"未支付且已超时"的订单，逐个走阶段3的 tryCloseOrder（CAS 关单+回补，幂等）。
     * 兜底两类故障：关单延迟消息丢失、order-timeout 消费者长时间不可用。
     * 每轮限量 CLOSE_BATCH_LIMIT 条，防单轮持锁过久；本轮没扫完下一轮继续。
     */
    private void closeTimeoutOrders() {
        LocalDateTime deadline = LocalDateTime.now().minusMinutes(payTimeoutMinutes);
        List<VoucherOrder> timeoutOrders = voucherOrderService.query()
                .eq("status", 1)
                .lt("create_time", deadline)
                .last("LIMIT " + CLOSE_BATCH_LIMIT)
                .list();
        if (timeoutOrders.isEmpty()) {
            return;
        }
        log.warn("[对账] 扫表发现超时未支付订单 {} 笔, 开始兜底关单", timeoutOrders.size());
        for (VoucherOrder order : timeoutOrders) {
            // tryCloseOrder 内部 CAS（WHERE status=1）+ 回补三件套，幂等可重入
            boolean closed = voucherOrderService.tryCloseOrder(order.getId());
            log.info("[对账] 兜底关单 orderId={}, result={}", order.getId(), closed ? "已关闭" : "已被并发处理");
        }
    }

    /* ==================== ③ 资格差集核对 ==================== */

    /**
     * 资格核对（只对已结束的券做，活动进行中的资格属正常业务态不动）：
     * 账本 = DB 有效订单的用户集合（active_flag=0）。
     *   差集1：Set 有、账本无 → 幽灵资格（订单未落/漏SREM）→ 活动已结束，清理对齐账本
     *   差集2：账本有、Set 无 → 资格丢失 → 活动已结束无业务影响，仅报告不修复
     */
    private void checkQualification() {
        LocalDateTime now = LocalDateTime.now();
        List<SeckillVoucher> ended = seckillVoucherService.query()
                .gt("end_time", now.minusDays(ENDED_SCAN_DAYS))
                .le("end_time", now).list();
        for (SeckillVoucher sv : ended) {
            String orderKey = SECKILL_ORDER_KEY + sv.getVoucherId();
            Set<String> members = stringRedisTemplate.opsForSet().members(orderKey);
            if (members == null || members.isEmpty()) {
                continue;
            }
            // DB 账本：该券全部有效订单的去重用户集合
            Set<String> validUsers = voucherOrderService.query()
                    .select("DISTINCT user_id")
                    .eq("voucher_id", sv.getVoucherId())
                    .eq("active_flag", 0).list().stream()
                    .map(o -> String.valueOf(o.getUserId()))
                    .collect(Collectors.toSet());

            int ghostRemoved = 0;
            for (String member : members) {
                if (!validUsers.contains(member)) {
                    stringRedisTemplate.opsForSet().remove(orderKey, member);
                    ghostRemoved++;
                }
            }
            long lostCount = validUsers.stream().filter(u -> !members.contains(u)).count();
            if (ghostRemoved > 0 || lostCount > 0) {
                log.warn("[对账] 资格差集核对完成: voucherId={}, 幽灵资格清理 {}, 账本缺资格 {}(仅报告)",
                        sv.getVoucherId(), ghostRemoved, lostCount);
            }
        }
    }
}
