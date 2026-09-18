package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.ScrollResult;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;


/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    private IUserService userService;
    private StringRedisTemplate stringRedisTemplate;
    private IFollowService followService;

    public BlogServiceImpl(IUserService userService, StringRedisTemplate stringRedisTemplate, IFollowService followService) {
        this.userService = userService;
        this.stringRedisTemplate = stringRedisTemplate;
        this.followService = followService;
    }

    /*
    查询全部热门博客
    */
    @Override
    public Result queryHotBlog(Integer current) {

        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            queryBlogUser(blog);
            queryBlogLiked(blog);
        });
        return Result.ok(records);
    }

    /*
    保存笔记的同时对关注粉丝推送
    */
    @Override
    public Result saveBlog(Blog blog) {
        if (blog.getShopId() == null) {
            return Result.fail("尚未选择商铺!");
        }

        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("当前尚未登录!");
        }
        Long userId = user.getId(); //当前用户的id
        blog.setUserId(userId);

        // 2.保存探店博文
        save(blog);
        // 3.对当前用户的粉丝进行推送
        List<Follow> fans = followService.query().eq("follow_user_id", userId.toString()).list();
        for (Follow follow : fans) {
            //3.1 获取当前粉丝id
            Long fansId = follow.getUserId();
            String key = "feed:" + fansId;
            //3.2 推送
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }

        return Result.ok(blog.getId());
    }


    /*
    查询feed流推送的关注博主所发布的推文
    */
    @Override
    public Result queryBlogByFollow(Long max, Integer offset) {
        System.out.println("========== 查询Feed ==========");
        System.out.println("max = " + max);
        System.out.println("offset = " + offset);
        System.out.println("==============================");
        // 1. 查询当前用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("当前尚未登录");
        }

        Long userId = user.getId();
        String key = FEED_KEY + userId;

        // 2. 查询收件箱 ZREVRANGEBYSCORE
        Set<ZSetOperations.TypedTuple<String>> typedTuples =
                stringRedisTemplate.opsForZSet()
                        .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        // 3. 非空判断
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }

        // 4. 解析数据
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0L;
        int of = 0;

        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            // 获取博客ID
            String id = typedTuple.getValue();
            ids.add(Long.valueOf(id));
            // 获取时间戳
            long time = typedTuple.getScore().longValue();

            // 计算最小时间和偏移量
                if (time == minTime) {
                    of++;
                } else {
                    minTime = time;
                    of = 1;
            }
        }

        // 5. 根据ID查询博客
        String idsStr = StrUtil.join(",", ids);
        List<Blog> blogs = query()
                .in("id", ids).last("ORDER BY FIELD(id," + idsStr + ")").list();

        // 6. 查询博客对应的用户以及点赞状态
        for (Blog blog : blogs) {
            queryBlogUser(blog);
            queryBlogLiked(blog);
        }

        // 7. 封装结果
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setMinTime(minTime);
        scrollResult.setOffset(of);
        scrollResult.setList(blogs);

        return Result.ok(scrollResult);
    }

    //查询单个笔记
    @Override
    public Result queryBlogById(Long id) {
        //1.查询笔记
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在!");
        }
        //2.查询笔记对应的用户
        queryBlogUser(blog);
        //3.查询当前笔记是否被点过赞
        queryBlogLiked(blog);
        return Result.ok(blog);
    }

    @Override
    public Result likeBlog(Long id) {
        //1.查询当前的登录用户
        Long userId = UserHolder.getUser().getId();
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("不存在的笔记!");
        }

        //2.判断用户是否已经点过赞
        String key = BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());//判断是否在redis的set集合中

        if (score == null) {
            //2.1如果没点赞，则更新数据库+1
            boolean success = update().setSql("liked=liked+1").eq("id", id).update();
            if (success) {
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {
            //2.2如果点赞了，则数据库-1
            boolean success1 = update().setSql("liked=liked-1").eq("id", id).update();
            if (success1) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }

        return Result.ok();

    }

    //查询笔记点赞的前五名
    @Override
    public Result queryBlogLikes(Long id) {

        String key = BLOG_LIKED_KEY + id;

        Set<String> range = stringRedisTemplate.opsForZSet().range(key, 0, 4);  //按照点赞时间，从最早开始展示
        //非空判断
        if (range == null || range.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        //根据id查询用户
        List<Long> ids = range.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", ids);
        //转换用户类型
        List<UserDTO> users = userService
                .query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        return Result.ok(users);
    }


    //查询当前笔记是否点过赞
    private void queryBlogLiked(Blog blog) {
        //1.获取当前的用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return; //说明当前用户未登录
        }
        Long userId = user.getId();

        String key = BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());//判断是否在redis的set集合中

        //2.如果获取到的值为空，说明还没点过赞  score==null 尚未点赞 score!=null 已经点赞
        blog.setIsLike(score != null);
    }

    //查询笔记对应的用户
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        if (userId == null) {
            return;
        }
        User user = userService.getById(userId);
        if (user == null) {
            return;
        }
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
