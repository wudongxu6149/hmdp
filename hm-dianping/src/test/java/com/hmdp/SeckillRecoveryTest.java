package com.hmdp;

import com.hmdp.dto.SeckillMessage;
import com.hmdp.mq.SeckillPendingOrderStore;
import com.hmdp.mq.SeckillResultStore;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.task.ReconciliationTask;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Set;

import static com.hmdp.utils.RedisConstants.SECKILL_REFUND_KEY;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class SeckillRecoveryTest {

    @Test
    void recoveryReplaysTheOriginalOrder() {
        ReconciliationTask task = new ReconciliationTask();
        SeckillPendingOrderStore store = mock(SeckillPendingOrderStore.class);
        IVoucherOrderService service = mock(IVoucherOrderService.class);
        SeckillMessage message = new SeckillMessage(14L, 21L, 100L);
        ReflectionTestUtils.setField(task, "pendingOrderStore", store);
        ReflectionTestUtils.setField(task, "voucherOrderService", service);
        when(store.due(anyLong(), eq(100))).thenReturn(Set.of("100:21:14"));
        when(store.parse("100:21:14")).thenReturn(message);

        ReflectionTestUtils.invokeMethod(task, "recoverPendingOrders");

        verify(service).landSeckillOrder(message);
        verify(store, never()).complete(message); // 只能由落库成功/退票完成的路径清理
    }

    @Test
    void recoveryFailureKeepsTheRecordForRetry() {
        ReconciliationTask task = new ReconciliationTask();
        SeckillPendingOrderStore store = mock(SeckillPendingOrderStore.class);
        IVoucherOrderService service = mock(IVoucherOrderService.class);
        SeckillMessage message = new SeckillMessage(14L, 21L, 100L);
        ReflectionTestUtils.setField(task, "pendingOrderStore", store);
        ReflectionTestUtils.setField(task, "voucherOrderService", service);
        when(store.due(anyLong(), eq(100))).thenReturn(Set.of("100:21:14"));
        when(store.parse("100:21:14")).thenReturn(message);
        doThrow(new IllegalStateException("DB 不可用")).when(service).landSeckillOrder(message);

        ReflectionTestUtils.invokeMethod(task, "recoverPendingOrders");

        verify(store).retryLater(eq("100:21:14"), anyLong());
    }

    @Test
    void refundedOrderDoesNotHitTheOldNullPointerOrGetInserted() {
        VoucherOrderServiceImpl service = spy(new VoucherOrderServiceImpl());
        RedissonClient redisson = mock(RedissonClient.class);
        RLock lock = mock(RLock.class);
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        SeckillPendingOrderStore store = mock(SeckillPendingOrderStore.class);
        SeckillResultStore resultStore = mock(SeckillResultStore.class);
        SeckillMessage message = new SeckillMessage(14L, 21L, 100L);
        ReflectionTestUtils.setField(service, "redissonClient", redisson);
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redis);
        ReflectionTestUtils.setField(service, "pendingOrderStore", store);
        ReflectionTestUtils.setField(service, "seckillResultStore", resultStore);
        when(redisson.getLock("lock:order:21")).thenReturn(lock);
        doReturn(null).when(service).getById(100L);
        when(redis.hasKey(SECKILL_REFUND_KEY + 100L)).thenReturn(true);

        TransactionSynchronizationManager.initSynchronization();
        try {
            service.landSeckillOrder(message);
            verify(store).complete(message);
            verify(service, never()).save(any());
            verify(lock, never()).unlock();
            for (TransactionSynchronization synchronization : TransactionSynchronizationManager.getSynchronizations()) {
                synchronization.afterCompletion(TransactionSynchronization.STATUS_COMMITTED);
            }
            verify(lock).unlock();
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }
}
