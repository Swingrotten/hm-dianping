package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.BooleanUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class SimpleRedisLock implements ILock {

    private final String name;

    private final StringRedisTemplate stringRedisTemplate;

    private static final String ID_PREFIX = UUID.randomUUID().toString(true)+"-";

    private static final String KEY_PREFIX = "lock:";
    @Override
    public boolean tryLock(Long timeoutSec) {
        // 获取线程标示
        String threadId = ID_PREFIX + Thread.currentThread().getId();

        //获取锁
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + name, threadId , timeoutSec, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(success);
    }

    @Override
    public void unlock() {
        //获取线程标示
        String threadId = ID_PREFIX + Thread.currentThread().getId();

        //获取锁
        String id = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);

        //判断标示是否一致
        if(threadId.equals(id)){
            //释放
            stringRedisTemplate.delete(KEY_PREFIX+name);
        }

    }
}
