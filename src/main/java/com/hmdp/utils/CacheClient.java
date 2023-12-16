package com.hmdp.utils;


import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;

@Slf4j
@Component
@RequiredArgsConstructor
public class CacheClient{


    private final StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 将任意Java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间
     */
    public void set(String key, Object value,Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    /**
     *   将任意Java对象序列化为json并存储在string类型的key中，并且可以设置逻辑过期时间，
     *   用于处理缓存击穿问题
     */
    public <T> void setWithLogicalExpire(String key, T value,Long time, TimeUnit unit ){
        //设置逻辑过期
        RedisData<T> redisData =new RedisData<>();
        redisData.setData((value));
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));

        //写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));

    }

    /**
     * 根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题
     */
    public <T,ID> T queryWithPassThrough(
            String keyPrefix, ID id, Class<T> type, Function<ID, T> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;

        //1. 从redis查询商铺缓存
        String Json = stringRedisTemplate.opsForValue().get(key);

        //2. 判断是否存在
        if (CharSequenceUtil.isNotBlank(Json)){
            // 3.存在,直接返回
            return JSONUtil.toBean(Json, type);
        }

        //判断命中的是否为空值
        if(Json!=null){
            return null;
        }

        //4. 不存在,根据id查询数据库
        T t = dbFallback.apply(id);

        //5. 不存在,返回错误
        if (t==null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误信息
            return null;
        }

        //6. 存在,写入redis
        this.set(key,t,time,unit);
        //7. 返回
        return t;
    }

    /**
     * 根据指定的key查询缓存，并反序列化为指定类型，需要利用逻辑过期解决缓存击穿问题
     */
    public <T,ID> T queryWithLogicalExpire(
            String keyPrefix, ID id, Class<T> type,Function<ID, T> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;

        //1. 从redis查询商铺缓存
        String Json = stringRedisTemplate.opsForValue().get(key);

        //2. 判断是否存在
        if (CharSequenceUtil.isBlank(Json)){
            // 3. 不存在,返回空
            return null;
        }

        //4. 命中,需要先把json反序列化为对象
        RedisData<T> redisData = JSONUtil.toBean(Json, RedisData.class);
        T t = JSONUtil.toBean((JSONObject) redisData.getData(),type);
        LocalDateTime expireTime = redisData.getExpireTime();

        //5. 判读是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //5.1 如果没过期, 直接返回店铺信息
            return t;
        }


        //5.2 已过期, 需要缓存重建
        //6. 缓存重建
        //6.1 获取互斥锁
        String lokKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lokKey);

        //6.2判断是否获取锁成功
        if(isLock){

            //做双重检查锁定 (DoubleChecked)
            if(expireTime.isAfter(LocalDateTime.now())){
                //  如果没过期, 直接返回店铺信息
                return t;
            }
            CACHE_REBUILD_EXECUTOR.submit( ()->{
                try {
                    //查询数据库
                    T t1 = dbFallback.apply(id);
                    //重建缓存
                    this.setWithLogicalExpire(key, t1, time, unit );
                }catch (Exception e){
                    throw new RuntimeException(e);
                }finally {
                    unLock(lokKey);
                }
            });
        }
        //7. 返回
        return t;
    }

    /**
     * 根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题
     * 加入互斥锁
     */
    public <T, ID> T queryWithMutex(
            String keyPrefix, ID id, Class<T> type, Function<ID, T> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (CharSequenceUtil.isNotBlank(shopJson)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(shopJson, type);
        }
        // 判断命中的是否是空值
        if (shopJson != null) {
            // 返回一个错误信息
            return null;
        }

        // 4.实现缓存重建
        // 4.1.获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        T t = null;
        try {
            boolean isLock = tryLock(lockKey);
            // 4.2.判断是否获取成功
            if (!isLock) {
                // 4.3.获取锁失败，休眠并重试
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit);
            }
            // 4.4.获取锁成功，根据id查询数据库
            t = dbFallback.apply(id);
            // 5.不存在，返回错误
            if (t == null) {
                // 将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                // 返回错误信息
                return null;
            }
            // 6.存在，写入redis
            this.set(key, t, time, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            // 7.释放锁
            unLock(lockKey);
        }
        // 8.返回
        return t;
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

}