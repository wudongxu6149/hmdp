package com.hmdp.interceptor;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.utils.UserHolder;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

@Component
@AllArgsConstructor
@Slf4j
public class RefreshTokenInterceptor implements HandlerInterceptor {

    private static final String SECKILL_START_NS = "seckillStartNs";
    private final StringRedisTemplate stringRedisTemplate;

   /*主要功能：责刷新 Token 存活时间和保存用户信息到 ThreadLocal，不负责权限校验
   * */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        if (request.getRequestURI().startsWith("/voucher-order/seckill/")) {
            request.setAttribute(SECKILL_START_NS, System.nanoTime());
        }
        //1.获取请求头中的token
        String token = request.getHeader("authorization");
        //2.从redis中获取用户信息
        if(StrUtil.isBlank(token)){
            //如果是空，说明这个时候还没有登录
            return true;
        }
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(LOGIN_USER_KEY+token);
        //3.判断是否存在用户
        if(userMap.isEmpty()){
            return true;
        }
        //4.如果用户存在，将用户数据转回User
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);

        //如果用户存在，保存用户信息到ThreadLocal中
        UserHolder.saveUser(userDTO);
        //5.刷新token的有效期
        stringRedisTemplate.expire(LOGIN_USER_KEY+token,LOGIN_USER_TTL, TimeUnit.MINUTES);
        //6.放行
        return true;
    }


    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, @Nullable Exception ex) throws Exception {
        Long startNs = (Long) request.getAttribute(SECKILL_START_NS);
        if (startNs != null) {
            long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
            if (elapsedMs >= 500) {
                log.warn("[秒杀Servlet慢请求] totalMs={}", elapsedMs);
            }
        }
        //释放ThreadLocal中的数据
        UserHolder.removeUser();
    }
}
