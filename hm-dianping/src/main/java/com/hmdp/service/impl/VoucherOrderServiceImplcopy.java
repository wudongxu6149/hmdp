package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
//@Service
@Slf4j
public class VoucherOrderServiceImplcopy extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;


    private final static DefaultRedisScript<Long> SECKILL_SCRIPT;

    //初始化脚本
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("scripts\\seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    //阻塞队列
    private BlockingQueue<VoucherOrder> blockingQueue = new ArrayBlockingQueue<>(1024 * 1024);

    //异步线程池->执行下单
    private final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        VoucherOrderHandler voucherOrderHandler = new VoucherOrderHandler();
        SECKILL_ORDER_EXECUTOR.submit(voucherOrderHandler);
    }

    // 控制后台线程运行的标志位
    private volatile boolean isRunning = true;

    @PreDestroy
    private void destroy() {
        // 标识系统正在关闭
        isRunning = false;
        // 优雅关闭线程池并中断阻塞任务
        if (SECKILL_ORDER_EXECUTOR != null) {
            SECKILL_ORDER_EXECUTOR.shutdownNow();
        }
    }


    /*异步线程执行下单工作*/
    /*private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while(true){
                try {

                    VoucherOrder voucherOrder = blockingQueue.take();

                    //1.获取分布式锁
                    RLock lock = redissonClient.getLock("lock:order" + voucherOrder.getUserId()); //多个线程去竞争redis中的同一把锁

                    //2.下单
                    boolean isLock = lock.tryLock();
                    if(!isLock){
                        log.error("不允许重复下单!");
                        return ;
                    }
                    try {
                        proxy.createVoucherOrder(voucherOrder);
                    } catch (IllegalStateException e) {
                        throw new RuntimeException(e);
                    } finally {
                        //释放锁
                        lock.unlock();
                    }

                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }*/

    //使用消息队列进行下单工作
    private class VoucherOrderHandler implements Runnable {
        private final String queueName = "stream.orders";

        @Override
        public void run() {
            while (isRunning && !Thread.currentThread().isInterrupted()) {
                try {
                    //1.获取消息队列队列中的订单信息
                    List<MapRecord<String, Object, Object>> read = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1L).block(Duration.ofSeconds(2L)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2.判断是否获取成功
                    //2.1 如果获取失败，则进行下一次循环
                    if (read == null || read.isEmpty()) {
                        continue;
                    }
                    //2.2 如果获取成功，则可以进行下单步骤
                    MapRecord<String, Object, Object> record = read.get(0);

                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    proxy.createVoucherOrder(voucherOrder);

                    //3.XACK确认完成订单
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    if (!isRunning || Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    log.error("订单处理异常:", e);
                    //处理pendingList中尚未被处理的订单信息
                    handlePendingList();

                }
            }
        }

        //处理pendingList中需要确认的订单
        private void handlePendingList() {

            while (isRunning && !Thread.currentThread().isInterrupted()) {
                try {
                    //1.获取pendingList中的订单信息
                    List<MapRecord<String, Object, Object>> read = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1L),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2.判断是否获取成功
                    //2.1 如果获取失败，说明pendingList中没有尚未被处理的信息
                    if (read == null || read.isEmpty()) {
                        break;
                    }
                    //2.2 如果获取成功，则可以进行下单步骤
                    MapRecord<String, Object, Object> record = read.get(0);

                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    proxy.createVoucherOrder(voucherOrder);

                    //3.XACK确认完成订单
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e1) {
                    if (!isRunning || Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    log.error("订单处理异常:", e1);
                    try {
                        Thread.sleep(100L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); //恢复中断标志
                        break;
                    }
                }
            }
        }
    }


    //获取代理对象
    private IVoucherOrderService proxy;

    @Lazy
    public VoucherOrderServiceImplcopy(IVoucherOrderService proxy){
        this.proxy=proxy;
    }

    //购买秒杀券
    @Override
    public Result seckillVoucher(Long voucherId) {
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();

        //3.获取当前线程的代理对象
        //proxy=(IVoucherOrderService) AopContext.currentProxy();

        long orderId = redisIdWorker.nextId("order"); //生成订单id
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(), //keys
                voucherId.toString(),  //参数1
                userId.toString(), //参数2
                String.valueOf(orderId) //参数3
        );

        //2.判断返回值 0 1 2 , 1 2 无购买资格
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足!请稍后重试" : "请勿重复下单!");
        }

        /*

        //2.放入阻塞队列中下单
        VoucherOrder voucherOrder=new VoucherOrder();
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setUserId(userId);
        voucherOrder.setId(orderId);

        blockingQueue.add(voucherOrder);*/

        return Result.ok(orderId);
    }



    /*=============================================================================================*/


    /*//购买秒杀券
    @Override
    public Result seckillVoucher(Long voucherId) {

        //1.查询优惠券，判断秒杀是否开始/结束
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始!");
        } else if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束!");
        }
        //2.判断库存stock是否足够
        Integer stock = seckillVoucher.getStock();
        if (stock < 1) {
            return Result.fail("优惠券库存已售罄!");
        }


        Long userId = UserHolder.getUser().getId();
        *//*
        //获取分布式锁对象
        SimpleLock simpleLock = new SimpleLock("order:"+userId,stringRedisTemplate);
        boolean lock = simpleLock.tryLock(600);

        if(!lock){
            return Result.fail("不允许重复下单!");
        }
        //获取代理对象（事务）
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId, userId);
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } finally {
            //释放锁
            simpleLock.unLock();
        }
        *//*

        //使用redisson
        RLock lock = redissonClient.getLock("lock:order" + userId);


        boolean isLock = lock.tryLock();
        if(!isLock){
            return Result.fail("不允许重复下单!");
        }
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId, userId);
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } finally {
            //释放锁
            lock.unlock();
        }
    }
       */

    //使用stream模拟消息队列进行下单
    @Transactional(rollbackFor = Exception.class)
    public void createVoucherOrder(VoucherOrder voucherOrder) {

        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if (!isLock) {
            // 获取锁失败，直接返回失败或者重试
            log.error("不允许重复下单！");
            return;
        }

        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                log.error("不允许重复下单！");
                return;
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                log.error("库存不足！");
                return;
            }

            // 7.创建订单
            save(voucherOrder);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }

    /*使用阻塞队列进行下单

    @Transactional(rollbackFor = Exception.class)
    public Result createVoucherOrder(VoucherOrder voucherOrder) {

        Long voucherId = voucherOrder.getVoucherId();
        Long userId = voucherOrder.getUserId();

        //查询数据库种当前用户是否有订单
        VoucherOrder order = query()
                .eq("voucher_id", voucherId)
                .eq("user_id", userId)
                .one();

        if (order != null) {
            return Result.fail("你已经购买过了哟");
        }


        //4.扣除库存，然后更新数据库信息
        boolean success = seckillVoucherService
                .update()
                .setSql("stock=stock-1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();

        if (!success) {
            return Result.fail("优惠券库存已售罄!");
        }
        //写入数据库
        this.save(voucherOrder);

        //返回订单id
        return Result.ok(voucherOrder.getId());

    }*/
}
