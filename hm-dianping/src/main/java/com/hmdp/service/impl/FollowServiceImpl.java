package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private UserServiceImpl userServiceImpl;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public Result follow(Long followUserId, boolean isFollow) {
        //1.获取当前登录用户
        UserDTO user = UserHolder.getUser();

        if (user == null) {
            return Result.fail("当前尚未登录!");
        }
        Long userId = user.getId();
        String key = "follows:" + userId; //关注用户

        //2.判断是关注还是取关
        if (isFollow) {
            //2.1 关注
            Follow follow = new Follow();
            follow.setFollowUserId(followUserId);
            follow.setUserId(userId);
            boolean success = save(follow);
            if (success) {
                //写入数据到redis中
                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }
        } else {
            //2.2 取关，从数据库中删除关注对象
            QueryWrapper<Follow> queryWrapper = new QueryWrapper<>();
            queryWrapper.eq("user_id", userId).eq("follow_user_id", followUserId);
            boolean remove = remove(queryWrapper);
            if (remove) {
                stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        return Result.ok();
    }

    //查询是否关注
    @Override
    public Result follow(Long followUserId) {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("当前尚未登录!");
        }

        List<Follow> list = query().eq("user_id", user.getId()).eq("follow_user_id", followUserId).list();
        return Result.ok(!list.isEmpty());
    }


    //查询共同关注的用户
    @Override
    public Result followCommons(Long followUserId) {
        //1.查询当前用户
        UserDTO user = UserHolder.getUser();

        if (user == null) {
            return Result.fail("当前尚未登录!");
        }
        Long userId = user.getId();
        String userKey = "follows:" + userId;
        String followUserKey = "follows:" + followUserId;
        //2.redis中查询set集合交际
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(userKey, followUserKey);
        if (intersect == null || intersect.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        //3.转换回userId
        List<Long> ids = intersect.stream()
                .map(Long::valueOf)
                .collect(Collectors.toList());
        //4.查询对应的用户
        List<UserDTO> userDTOS = BeanUtil.copyToList(userServiceImpl.listByIds(ids), UserDTO.class);

        return Result.ok(userDTOS);
    }
}
