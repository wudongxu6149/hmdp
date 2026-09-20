package com.hmdp.exception;

/**
 * 【阶段6新增】业务异常：可预期、带用户可读信息的异常（限流、参数校验等）。
 * 全局异常处理器对它单独分类——以 warn 级别记录并原样返回消息，而不是笼统的"服务器异常"
 */
public class BizException extends RuntimeException {

    public BizException(String message) {
        super(message);
    }
}
