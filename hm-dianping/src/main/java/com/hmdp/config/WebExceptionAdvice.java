package com.hmdp.config;

import com.hmdp.dto.Result;
import com.hmdp.exception.BizException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@Slf4j
@RestControllerAdvice
public class WebExceptionAdvice {

    /**
     * 【阶段6新增】业务异常单独分类：可预期的异常（限流/参数校验）以 warn 记录并
     * 原样把消息返回前端，与不可预期的系统异常区分开
     */
    @ExceptionHandler(BizException.class)
    public Result handleBizException(BizException e) {
        log.warn("[业务异常] {}", e.getMessage());
        return Result.fail(e.getMessage());
    }

    @ExceptionHandler(RuntimeException.class)
    public Result handleRuntimeException(RuntimeException e) {
        log.error(e.toString(), e);
        return Result.fail("服务器异常");
    }
}
