package com.hmdp.aspect;

import cn.hutool.core.lang.UUID;
import com.hmdp.annotation.RateLimit;
import com.hmdp.exception.BizException;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.Collections;

/**
 * 【阶段6新增】滑动窗口限流切面：@RateLimit 注解的环绕拦截。
 * Redis ZSet 滑动窗口 + Lua 原子脚本（清窗口外→计数→未超限记录一次），集群共享计数——
 * 双实例部署时限流阈值依然是全局准确的。超限抛 BizException 快速失败，不进入业务方法
 */
@Aspect
@Component
@Slf4j
public class RateLimitAspect {

    private static final String KEY_PREFIX = "rate_limit:";

    private static final DefaultRedisScript<Long> RATE_LIMIT_SCRIPT;

    static {
        RATE_LIMIT_SCRIPT = new DefaultRedisScript<>();
        RATE_LIMIT_SCRIPT.setLocation(new ClassPathResource("scripts/rate_limit.lua"));
        RATE_LIMIT_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Around("@annotation(rateLimit)")
    public Object rateLimit(ProceedingJoinPoint pjp, RateLimit rateLimit) throws Throwable {

        RateLimit.LimitType limitType = rateLimit.limitType();
        String dimension = resolveDimension(limitType);
        // Key 格式：rate_limit:{业务}:{限流类型}[:{用户ID/IP}]
        String key = KEY_PREFIX + rateLimit.key() + ":" + limitType + ":" + dimension;

        //当前的时间
        long now = System.currentTimeMillis();

        // member 唯一化：ZSet 成员唯一，同一毫秒内的多次请求不能互相覆盖
        String member = now + ":" + UUID.randomUUID().toString(true);

        Long allowed = stringRedisTemplate.execute(
                RATE_LIMIT_SCRIPT,
                Collections.singletonList(key),
                String.valueOf(now),                                // ARGV[1] 当前毫秒
                String.valueOf(rateLimit.window() * 1000),          // ARGV[2] 窗口毫秒
                String.valueOf(rateLimit.maxCount()),               // ARGV[3] 窗口内上限
                member);                                            // ARGV[4] 唯一成员
        if (allowed == null || allowed == 0) {
            log.warn("[限流] 触发滑动窗口限流: key={}, window={}s, maxCount={}",
                    key, rateLimit.window(), rateLimit.maxCount());
            throw new BizException("请求过于频繁，请稍后再试");
        }
        return pjp.proceed();
    }

    /**
     * 解析用户或 IP 限流的主体标识。GLOBAL 不需要主体标识。
     */
    private String resolveDimension(RateLimit.LimitType type) {
        switch (type) {
            case USER:
                // 需登录的接口按用户维度；拿不到登录态（如登录前接口）退化为按 IP
                return UserHolder.getUser() != null
                        ? String.valueOf(UserHolder.getUser().getId())
                        : resolveIp();
            case IP:
                return resolveIp();
            case GLOBAL:
            default:
                return "global";
        }
    }

    /**
     * 取客户端 IP：nginx 转发场景优先 X-Forwarded-For 首段，否则 remoteAddr
     */
    private String resolveIp() {

        ServletRequestAttributes attrs =
                (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        if (attrs == null) {
            return "unknown";
        }
        HttpServletRequest request = attrs.getRequest();

        //有些网络通过多层代理，那么获取到的ip就会有多个，一般都是通过逗号（,）分割开来，并且第一个ip为客户端的真实IP
        String xff = request.getHeader("X-Forwarded-For");
        if (xff != null && !xff.isEmpty()) {
            return xff.split(",")[0].trim();
        }
        return request.getRemoteAddr();
    }
}
