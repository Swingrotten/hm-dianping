package com.hmdp.service.impl;

import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {


    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    public ShopServiceImpl(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public Result queryShopById(Long id) {

        //缓存穿透
        //Shop shop = queryWithPassThrough(id);

        //互斥锁解决缓存击穿
        Shop shop = queryWithMutex(id);
        if (shop == null) {
            return Result.fail("店铺不存在! ");
        }
        //返回
        return Result.ok(shop);
    }

    public Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;

        //1. 从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //2. 判断是否存在
        if (CharSequenceUtil.isNotBlank(shopJson)){
            // 3.存在,直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        //判断命中的是否为空值
        if(shopJson!=null){
            return null;
        }

        //4. 实现缓存重建
        //4.1 获取互斥锁
        String lokKey = LOCK_SHOP_KEY + id;
        Shop shop = null;

        try {
            boolean isLock = tryLock(lokKey);

            //4.2 判断是否获取成功
            if(!isLock){
                //4.3 失败,则休眠重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            //4.4 成功, 根据id查询数据库
            shop = getById(id);

            //模拟重建的延迟
            Thread.sleep(200);

            //5. 不存在,返回错误
            if (shop==null){
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }

            //6. 存在,写入redis
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //7. 释放互斥锁
            unLock(lokKey);
        }
        //8. 返回
        return shop;
    }

    public Shop queryWithPassThrough(Long id){
        String key = CACHE_SHOP_KEY + id;

        //1. 从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //2. 判断是否存在
        if (CharSequenceUtil.isNotBlank(shopJson)){
            // 3.存在,直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        //判断命中的是否为空值
        if(shopJson!=null){
            return null;
        }

        //4. 不存在,根据id查询数据库
        Shop shop = getById(id);

        //5. 不存在,返回错误
        if (shop==null){
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        //6. 存在,写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //7. 返回
        return shop;
    }

    //上锁
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    //解锁
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    @Override
    @Transactional
    public Result updateShop(Shop shop) {
        Long shopId = shop.getId();
        if(shopId==null){
            return Result.fail("商铺ID不能为空");
        }
        //1. 更新数据库
        updateById(shop);

        //2. 删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shopId );

        return Result.ok();
    }
}
