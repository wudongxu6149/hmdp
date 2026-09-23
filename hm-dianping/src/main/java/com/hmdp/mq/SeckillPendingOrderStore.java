package com.hmdp.mq;

import com.hmdp.dto.SeckillMessage;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.Set;

import static com.hmdp.utils.RedisConstants.SECKILL_PENDING_ORDER_KEY;

/*
整体流程
Lua 预扣成功
↓
写 pending
score = 15:30:00
↓
恢复任务 15:30:01 扫描
↓
due() 找到它
↓
尝试处理
↓
处理失败
↓
retryLater()
↓
score 改成 15:30:31
↓
暂时不再扫描
↓
15:30:31 之后
↓
due() 再次拿到它*/

/** Lua 预扣时写入的恢复日志；只有订单落库或完成回补后才能删除。 */
@Component
public class SeckillPendingOrderStore {

    private static final DefaultRedisScript<Long> RETRY_IF_PRESENT = new DefaultRedisScript<>();

    static {
        /*如果这个 pending 订单还存在，就修改它的下一次重试时间；如果已经被别人删掉了，就什么都不做。*/
        RETRY_IF_PRESENT.setScriptText("if redis.call('zscore', KEYS[1], ARGV[1]) then " +
                                            "return redis.call('zadd', KEYS[1], ARGV[2], ARGV[1])" +
                                        "end return 0");
        RETRY_IF_PRESENT.setResultType(Long.class);
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /*
    * 寻找出所有score（恢复时间小于当前的消息）
    * */
    public Set<String> due(long now, int limit) {
        return stringRedisTemplate.opsForZSet()
                .rangeByScore(SECKILL_PENDING_ORDER_KEY, 0, now, 0, limit);
    }

    /*
    * 解析从Redis中拿出来的字符串
    * */
    public SeckillMessage parse(String member) {
        String[] parts = member.split(":", -1);
        if (parts.length != 3) {
            throw new IllegalArgumentException("非法秒杀恢复记录: " + member);
        }
        return new SeckillMessage(Long.valueOf(parts[2]), Long.valueOf(parts[1]), Long.valueOf(parts[0]));
    }

    /*
    * 尝试失败后延迟再试
    * */
    public void retryLater(String member, long due) {
        // 与正常消费者清理并发时不能重新插入已完成的订单。
        stringRedisTemplate.execute(RETRY_IF_PRESENT,
                Collections.singletonList(SECKILL_PENDING_ORDER_KEY),
                member,
                String.valueOf(due));
    }

    //移除资格
    public void complete(SeckillMessage msg) {
        stringRedisTemplate.opsForZSet().remove(SECKILL_PENDING_ORDER_KEY, member(msg));
    }

    //获取当前消息的member
    private String member(SeckillMessage msg) {
        return msg.getOrderId() + ":" + msg.getUserId() + ":" + msg.getVoucherId();
    }
}
