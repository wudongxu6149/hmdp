package com.hmdp;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.extension.conditions.update.UpdateChainWrapper;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.impl.CloseRefundService;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.task.ReconciliationTask;
import com.hmdp.utils.RedisPreHeatRunner;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CloseRefundFlowTest {

    @Test
    void dbRollbackDoesNotTriggerRedisRefund() {
        CloseRefundService refundService = mock(CloseRefundService.class);
        VoucherOrderServiceImpl service = serviceWithDbUpdates(true, refundService);

        TransactionSynchronizationManager.initSynchronization();
        try {
            assertTrue(service.tryCloseOrder(100L));
            verifyNoInteractions(refundService);
            for (TransactionSynchronization callback : TransactionSynchronizationManager.getSynchronizations()) {
                callback.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK);
            }
            verifyNoInteractions(refundService);
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void refundStartsOnlyAfterDbCommit() {
        CloseRefundService refundService = mock(CloseRefundService.class);
        VoucherOrderServiceImpl service = serviceWithDbUpdates(true, refundService);

        TransactionSynchronizationManager.initSynchronization();
        try {
            assertTrue(service.tryCloseOrder(100L));
            verifyNoInteractions(refundService);
            for (TransactionSynchronization callback : TransactionSynchronizationManager.getSynchronizations()) {
                callback.afterCommit();
            }
            verify(refundService).apply(100L);
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void failedAfterCommitAttemptIsLeftForScheduledRetry() {
        CloseRefundService refundService = mock(CloseRefundService.class);
        doThrow(new IllegalStateException("Redis 不可用")).when(refundService).apply(100L);
        VoucherOrderServiceImpl service = serviceWithDbUpdates(true, refundService);

        TransactionSynchronizationManager.initSynchronization();
        try {
            assertTrue(service.tryCloseOrder(100L));
            for (TransactionSynchronization callback : TransactionSynchronizationManager.getSynchronizations()) {
                assertDoesNotThrow(callback::afterCommit);
            }
            verify(refundService).apply(100L);
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void failedDbStockRefundAbortsClose() {
        CloseRefundService refundService = mock(CloseRefundService.class);
        VoucherOrderServiceImpl service = serviceWithDbUpdates(false, refundService);

        TransactionSynchronizationManager.initSynchronization();
        try {
            assertThrows(IllegalStateException.class, () -> service.tryCloseOrder(100L));
            assertTrue(TransactionSynchronizationManager.getSynchronizations().isEmpty());
            verifyNoInteractions(refundService);
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void redisFailureLeavesDbPendingFlagUntouched() {
        CloseRefundService service = new CloseRefundService();
        VoucherOrderMapper mapper = mock(VoucherOrderMapper.class);
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ReflectionTestUtils.setField(service, "voucherOrderMapper", mapper);
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redis);
        when(mapper.selectById(100L)).thenReturn(closedOrder());

        assertThrows(IllegalStateException.class, () -> service.apply(100L));
        verify(mapper, never()).update(isNull(), any(UpdateWrapper.class));
    }

    @Test
    void repeatedRedisRefundCanClearDbPendingFlagWithoutIncrementingAgain() {
        CloseRefundService service = new CloseRefundService();
        VoucherOrderMapper mapper = mock(VoucherOrderMapper.class);
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ReflectionTestUtils.setField(service, "voucherOrderMapper", mapper);
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redis);
        when(mapper.selectById(100L)).thenReturn(closedOrder());
        // Lua 返回 0 表示此 orderId 的回补已执行；这里仍需清除数据库待办。
        when(redis.execute(any(RedisScript.class), any(List.class), anyString(), anyString())).thenReturn(0L);

        service.apply(100L);

        verify(mapper).update(isNull(), any(UpdateWrapper.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void scheduledTaskRetriesClosedOrdersWithPendingRefund() {
        ReconciliationTask task = new ReconciliationTask();
        IVoucherOrderService orderService = mock(IVoucherOrderService.class);
        CloseRefundService refundService = mock(CloseRefundService.class);
        QueryChainWrapper<VoucherOrder> query = mock(QueryChainWrapper.class, RETURNS_SELF);
        when(orderService.query()).thenReturn(query);
        when(query.list()).thenReturn(List.of(closedOrder()));
        ReflectionTestUtils.setField(task, "voucherOrderService", orderService);
        ReflectionTestUtils.setField(task, "closeRefundService", refundService);

        ReflectionTestUtils.invokeMethod(task, "repairCloseRefunds");

        verify(refundService).apply(100L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void startupDoesNotPreheatDbStockBeforePendingRefund() {
        RedisPreHeatRunner runner = new RedisPreHeatRunner();
        ISeckillVoucherService voucherService = mock(ISeckillVoucherService.class);
        IVoucherOrderService orderService = mock(IVoucherOrderService.class);
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        QueryChainWrapper<SeckillVoucher> voucherQuery = mock(QueryChainWrapper.class, RETURNS_SELF);
        QueryChainWrapper<VoucherOrder> orderQuery = mock(QueryChainWrapper.class, RETURNS_SELF);
        when(voucherService.query()).thenReturn(voucherQuery);
        when(voucherQuery.list()).thenReturn(List.of(new SeckillVoucher().setVoucherId(14L)));
        when(orderService.query()).thenReturn(orderQuery);
        when(orderQuery.count()).thenReturn(1);
        ReflectionTestUtils.setField(runner, "seckillVoucherService", voucherService);
        ReflectionTestUtils.setField(runner, "voucherOrderService", orderService);
        ReflectionTestUtils.setField(runner, "stringRedisTemplate", redis);

        runner.preHeatSeckillVouchers();

        verify(voucherService, never()).preHeatRedisMeta(any());
    }

    @SuppressWarnings("unchecked")
    private VoucherOrderServiceImpl serviceWithDbUpdates(boolean stockUpdated, CloseRefundService refundService) {
        VoucherOrderServiceImpl service = spy(new VoucherOrderServiceImpl());
        VoucherOrder order = new VoucherOrder().setId(100L).setUserId(21L).setVoucherId(14L).setStatus(1);
        doReturn(order).when(service).getById(100L);

        UpdateChainWrapper<VoucherOrder> orderUpdate = mock(UpdateChainWrapper.class, RETURNS_SELF);
        when(orderUpdate.update()).thenReturn(true);
        doReturn(orderUpdate).when(service).update();

        ISeckillVoucherService voucherService = mock(ISeckillVoucherService.class);
        UpdateChainWrapper<SeckillVoucher> stockUpdate = mock(UpdateChainWrapper.class, RETURNS_SELF);
        when(stockUpdate.update()).thenReturn(stockUpdated);
        when(voucherService.update()).thenReturn(stockUpdate);
        ReflectionTestUtils.setField(service, "seckillVoucherService", voucherService);
        ReflectionTestUtils.setField(service, "closeRefundService", refundService);
        return service;
    }

    private VoucherOrder closedOrder() {
        return new VoucherOrder().setId(100L).setUserId(21L).setVoucherId(14L)
                .setStatus(4).setCloseRefundPending(1);
    }
}
