package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误！");
        }

        //2.生成验证码
        String code = RandomUtil.randomNumbers(6);
        //3.保存验证码到redis 并且设置有效期
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);

        log.debug("发送短信验证码:" + code);
        //4.返回结果
        return Result.ok();
    }

    //登录功能
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        //1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误！");
        }

        //2.从redis中获取验证码并进行校验
        String code = loginForm.getCode();
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        //3.不一致报错
        if (cacheCode == null || !code.equals(cacheCode)) {
            return Result.fail("验证码不一致！");
        }
        //4.一致，根据手机号查询用户
        User user = query().eq("phone", phone).one();
        //5.判断用户是否存在，不存在则创建
        if (user == null) {
            user = createUserWithPhone(phone);
        }

        //6.保存用户信息到redis
        //6.1 生成随机令牌
        String token = UUID.randomUUID().toString(true);
        //6.2将User对象转为hash进行存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);

        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .ignoreNullValue()
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString())); //防止类型转换异常

        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY + token, userMap);

        //刷新用户信息有效期
        stringRedisTemplate.expire(LOGIN_USER_KEY + token, LOGIN_USER_TTL, TimeUnit.MINUTES);

        //返回token
        return Result.ok(token);
    }


    //用户签到功能
    @Override
    public Result sign() {
        //1.获取当前用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("当前尚未登陆!");
        }
        Long userId = user.getId();
        //2.获取当前日期
        LocalDateTime date = LocalDateTime.now();
        String keySuffix = date.format(DateTimeFormatter.ofPattern(":yyyy-MM"));
        //3.拼接key
        String key = USER_SIGN_KEY + userId + keySuffix;
        //4.获取当前是这个月的第几天（redis中需要-1）
        int dayOfMonth = date.getDayOfMonth();

        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);

        return Result.ok();
    }

    //统计本月连续签到
    @Override
    public Result signCount() {
        //1.获取当前用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("当前尚未登陆!");
        }
        Long userId = user.getId();
        //2.获取当前日期
        LocalDateTime date = LocalDateTime.now();
        String keySuffix = date.format(DateTimeFormatter.ofPattern(":yyyy-MM"));
        //3.拼接key
        String key = USER_SIGN_KEY + userId + keySuffix;
        //4.获取当前是这个月的第几天（redis中需要-1）
        int dayOfMonth = date.getDayOfMonth();
        //5.获取本月截至到今天的签到记录
        List<Long> result = stringRedisTemplate.opsForValue().bitField(key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));

        if (result == null || result.isEmpty()) {
            return Result.ok(0); //没有任何签到结果
        }
        //6.循环遍历与数字1做与运算
        Long num = result.get(0);
        int count = 0;
        while (true) {
            if ((num & 1) == 0) {
                break;
            }else{
                count++;
            }
            num >>>= 1;
        }

        return Result.ok(count);
    }

    //用户登出
    @Override
    public Result logout(HttpServletRequest request) {
        // 1. 获取请求头中的 token（前端通过 authorization 字段传递）
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token)) {
            return Result.ok();
        }
        // 2. 拼接 Redis 中的 key
        String key = RedisConstants.LOGIN_USER_KEY + token;
        // 3. 从 Redis 中删除 Token 缓存数据
        stringRedisTemplate.delete(key);
        // 4. 清除当前线程 ThreadLocal 中的用户信息
        UserHolder.removeUser();
        return Result.ok();
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName("user_" + RandomUtil.randomString(10));
        //保存到数据库中
        save(user);
        return user;
    }
}
