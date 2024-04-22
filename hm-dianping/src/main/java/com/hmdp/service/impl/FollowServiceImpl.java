package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import net.bytebuddy.implementation.auxiliary.AuxiliaryType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
    @Autowired
    private IUserService userService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        //获取用户id
        Long userId = UserHolder.getUser().getId();
        String key = "follows:" + userId;

        // 1.判断到底是关注还是取关
        if (isFollow) {
            // 2.关注，新增数据
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean isSuccess = save(follow);
            if (isSuccess) {
                // 先操作数据库，再操作redis。把关注用户的id，放入redis的set集合 sadd userId followerUserId
                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }
        } else {
            // 3.取关，删除 delete from tb_follow where user_id = ? and follow_user_id = ?
            boolean isSuccess = remove(new QueryWrapper<Follow>()
                    .eq("user_id", userId).eq("follow_user_id", followUserId));
            if(isSuccess){
                stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        return Result.ok();
    }
    @Override
    public Result isFollow(Long followUserId) {
        // 1.获取登录用户
        Long id = UserHolder.getUser().getId();
        //sql语句：select * from tb_follow where user_id = id and follow_user_id = followUserId
        //查询是否关注 select count(*) from tb_follow where user_id = ? and follow_user_id = ?
        Integer count = query().eq("user_id", id).eq("follow_user_id", followUserId).count();
        return Result.ok(count > 0);
    }

    @Override
    public Result followCommons(Long id) {
        // 1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        String key = "follows:" + userId;
        // 2.求交集
        String key2 = "follows:" + id;
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key, key2);
        if (intersect == null || intersect.isEmpty()) {
            // 无交集
            return Result.ok(Collections.emptyList());
        }
        List<Long> list = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        List<UserDTO> userDTOS = list.stream().map(a -> BeanUtil.copyProperties(userService.getById(a), UserDTO.class)).collect(Collectors.toList());
        return Result.ok(userDTOS);
    }
}
