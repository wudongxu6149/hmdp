package com.hmdp.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 【阶段6新增】滑动窗口限流注解（Redis ZSet + Lua 原子实现），标注在 Controller 方法上，
 * 由 RateLimitAspect 环绕拦截：超限直接抛 BizException 快速失败，不进入业务方法
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RateLimit {

    /** 业务前缀：不同接口用不同前缀隔离各自的计数器 */
    String key();

    /** 窗口长度（秒），默认 10 秒 */
    long window() default 10;

    /** 窗口内最大允许请求数 */
    long maxCount() default 100;

    /** 限流维度 */
    LimitType limitType() default LimitType.USER;

    enum LimitType {
        /** 按登录用户（UserHolder；未登录自动退化为按 IP） */
        USER,
        /** 按请求 IP（兼容 nginx 转发的 X-Forwarded-For 首段） */
        IP,
        /** 全局一个桶（Redis 计数天然集群共享） */
        GLOBAL
    }
}
