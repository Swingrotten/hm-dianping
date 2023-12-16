package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 */
@RequiredArgsConstructor
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {


    private final CacheClient cacheClient;

    private final StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryShopById(Long id) {

        Shop shop = cacheClient
                .queryWithLogicalExpire(CACHE_SHOP_KEY,id,Shop.class,this::getById,20L,TimeUnit.SECONDS);
        if (shop == null) {
            return Result.fail("店铺不存在! ");
        }
        //返回
        return Result.ok(shop);
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

    //缓存预热
    public void saveShop2Redis(Long id , Long expireSeconds) {
        //1. 查询店铺数据
        Shop shop = getById(id);

        //2. 封装逻辑过期时间
        RedisData<Shop> redisData = new RedisData<>();
        redisData.setData(shop);
        //  将expireSeconds与当前时间相加
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        //3. 写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }
}

////上锁
//private boolean tryLock(String key){
//    Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
//    return BooleanUtil.isTrue(flag);
//}
//
////解锁
//private void unLock(String key){
//    stringRedisTemplate.delete(key);
//}

//public Shop queryWithMutex(Long id){
//    String key = CACHE_SHOP_KEY + id;
//
//    //1. 从redis查询商铺缓存
//    String shopJson = stringRedisTemplate.opsForValue().get(key);
//
//    //2. 判断是否存在
//    if (CharSequenceUtil.isNotBlank(shopJson)){
//        // 3.存在,直接返回
//        return JSONUtil.toBean(shopJson, Shop.class);
//    }
//
//    //判断命中的是否为空值
//    if(shopJson!=null){
//        return null;
//    }
//
//    //4. 实现缓存重建
//    //4.1 获取互斥锁
//    String lokKey = LOCK_SHOP_KEY + id;
//    Shop shop = null;
//
//    try {
//        boolean isLock = tryLock(lokKey);
//
//        //4.2 判断是否获取成功
//        if(!isLock){
//            //4.3 失败,则休眠重试
//            Thread.sleep(50);
//            return queryWithMutex(id);
//        }
//
//        //4.4 成功, 根据id查询数据库
//        shop = getById(id);
//
//        //模拟重建的延迟
//        Thread.sleep(200);
//
//        //5. 不存在,返回错误
//        if (shop==null){
//            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
//            return null;
//        }
//
//        //6. 存在,写入redis
//        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
//
//    } catch (InterruptedException e) {
//        throw new RuntimeException(e);
//    } finally {
//        //7. 释放互斥锁
//        unLock(lokKey);
//    }
//    //8. 返回
//    return shop;
//}

//public Shop queryWithPassThrough(Long id){
//    String key = CACHE_SHOP_KEY + id;
//
//    //1. 从redis查询商铺缓存
//    String shopJson = stringRedisTemplate.opsForValue().get(key);
//
//    //2. 判断是否存在
//    if (CharSequenceUtil.isNotBlank(shopJson)){
//        // 3.存在,直接返回
//        return JSONUtil.toBean(shopJson, Shop.class);
//    }
//
//    //判断命中的是否为空值
//    if(shopJson!=null){
//        return null;
//    }
//
//    //4. 不存在,根据id查询数据库
//    Shop shop = getById(id);
//
//    //5. 不存在,返回错误
//    if (shop==null){
//        stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
//        return null;
//    }
//
//    //6. 存在,写入redis
//    stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
//
//    //7. 返回
//    return shop;
//}
////创建线程池
//private static final ExecutorService CACHE_REBUILD_EXECUTOR =
//        Executors.newFixedThreadPool(5);
//
////TODO:使用逻辑过期解决缓存击穿
//public Shop queryWithLogicalExpire(Long id){
//    String key = CACHE_SHOP_KEY + id;
//
//    //1. 从redis查询商铺缓存
//    String shopJson = stringRedisTemplate.opsForValue().get(key);
//
//    //2. 判断是否存在
//    if (CharSequenceUtil.isBlank(shopJson)){
//        // 3. 不存在,返回空
//        return null;
//    }
//
//    //4. 命中,需要先把json反序列化为对象
//    RedisData<Shop> redisData = JSONUtil.toBean(shopJson, new TypeReference<RedisData<Shop>>() {}, false);
//    Shop shop = redisData.getData();
//    LocalDateTime expireTime = redisData.getExpireTime();
//
//    //5. 判读是否过期
//    if(expireTime.isAfter(LocalDateTime.now())){
//        //5.1 如果没过期, 直接返回店铺信息
//        return shop;
//    }
//
//
//    //5.2 已过期, 需要缓存重建
//    //6. 缓存重建
//    //6.1 获取互斥锁
//    String lokKey = LOCK_SHOP_KEY + id;
//    boolean isLock = tryLock(lokKey);
//
//    //6.2判断是否获取锁成功
//    if(isLock){
//
//        //做双重检查锁定 (DoubleChecked)
//        if(expireTime.isAfter(LocalDateTime.now())){
//            //  如果没过期, 直接返回店铺信息
//            return shop;
//        }
//        CACHE_REBUILD_EXECUTOR.submit( ()->{
//            try {
//                //重建缓存
//                this.saveShop2Redis(id,20L);
//            }catch (Exception e){
//                throw new RuntimeException(e);
//            }finally {
//                unLock(lokKey);
//            }
//        });
//    }
//    //7. 返回
//    return shop;
//}
