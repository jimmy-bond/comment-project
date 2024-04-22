package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
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
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryTypeList() {
        String key = RedisConstants.CACHE_SHOPTYPE_KEY;
        String shopTypeList = stringRedisTemplate.opsForValue().get(key);
        //redis中有存储
        if(StrUtil.isNotBlank(shopTypeList)){
            List<ShopType> types = JSONUtil.toList(shopTypeList, ShopType.class);
        types = types.stream().sorted(Comparator.comparing(ShopType::getSort)).collect(Collectors.toList());
            return Result.ok(types);
        }
        //redis中没有
        List<ShopType> list = query().orderByAsc("sort").list();
        if(list != null){
            String jsonStr = JSONUtil.toJsonStr(list);
            stringRedisTemplate.opsForValue().set(key,jsonStr);
            return Result.ok(list);
        }
        return Result.fail("没有找到对应的商铺集合");
    }
}
