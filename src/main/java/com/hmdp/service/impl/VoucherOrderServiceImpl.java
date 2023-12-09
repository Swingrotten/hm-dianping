package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private final ISeckillVoucherService seckillVoucherService;
    private final RedisIdWorker redisIdWorker;
    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    private IVoucherOrderService proxy;

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    @Override
    public Result seckillVoucher(Long voucherId) {

        //获取用户
        Long userId = UserHolder.getUser().getId();
        //获取订单Id
        long orderId = redisIdWorker.nextId("order");

        //1. 执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        int r = result.intValue();
        //2. 判断结果是为0
        if (r != 0) {
            // 2.1 不为0,代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        //3. 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        //4. 返回订单Id
        return Result.ok(orderId);
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //1. 获取用户
        Long voucherOrder1 = voucherOrder.getUserId();

        //2. 创建锁对象
        RLock lock = redissonClient.getLock("order:" + voucherOrder1);

        //3. 获取锁
        boolean isLock = lock.tryLock();

        //4. 判断是否获取成功
        if (!isLock) {
            log.error("不允许重复下单");
            return;
        }

        try {
            //创建订单
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            //释放锁
            lock.unlock();
        }

    }
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {

        //5. 一人一单
        Long userId = voucherOrder.getId();

        //5.1 查询订单
        Long count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();

        //5.2 判断是否存在
        if (count > 0) {
            // 用户已经购买过了
            log.error("用户已经购买过一次了! ");
            return;
        }
        //6. 扣减库存
        boolean update = seckillVoucherService.update()
                .setSql("stock=stock-1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!update) {
            log.error("库存不足");
            return;
        }

        //7 创建订单
        save(voucherOrder);
    }

    private class VoucherOrderHandler implements Runnable {

        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                    //1. 获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    //2. 判断是否获取成功
                    if (list == null || list.isEmpty()) {
                        // 如果为null, 说明没有消息, 继续下一次循环
                        continue;
                    }

                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                    //3. 如果获取成功, 可以下单
                    handleVoucherOrder(voucherOrder);

                    //4. ACK确认
                    stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }

        }

        private void handlePendingList() {
            while (true) {
                try {
                    //1. 获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                    List<MapRecord<String, Object, Object>> read = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );

                    //2. 判断是否获取成功
                    if (read == null || read.isEmpty()) {
                        // 如果为null, 说明pending-list没有消息, 结束
                        break;
                    }

                    // 解析数据
                    MapRecord<String, Object, Object> entries = read.get(0);
                    Map<Object, Object> value = entries.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                    //3. 如果获取成功, 可以下单
                    handleVoucherOrder(voucherOrder);

                    //4. ACK确认
                    stringRedisTemplate.opsForStream().acknowledge("s1", "g1", entries.getId());
                } catch (Exception e) {
                    log.error("处理pendding订单异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }

        }

    }
}


//public Result seckillVoucher(Long voucherId) {
//
//    //获取用户
//    Long userId = UserHolder.getUser().getId();
//
//    //1. 执行lua脚本
//    Long result = stringRedisTemplate.execute(
//            SECKILL_SCRIPT,
//            Collections.emptyList(),
//            voucherId.toString(), userId.toString()
//    );
//    int r = result.intValue();
//    //2. 判断结果是为0
//    if(r!=0){
//        // 2.1 不为0,代表没有购买资格
//        return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
//    }
//
//    //2.2 为0,有购买资格,把下单信息保存到阻塞队列
//    VoucherOrder voucherOrder = new VoucherOrder();
//    //2.3 订单Id
//    long orderId = redisIdWorker.nextId("order");
//    voucherOrder.setId(orderId);
//    //2.4 用户Id
//    voucherOrder.setUserId(userId);
//    //2.5 代金券Id
//    voucherOrder.setVoucherId(voucherId);
//    //2.6 放入阻塞队列
//    orderTasks.add(voucherOrder);
//
//    //3. 获取代理对象
//    proxy = (IVoucherOrderService) AopContext.currentProxy();
//
//    //4. 返回订单Id
//    return Result.ok(orderId);
//
//    /*
//              //创建锁对象
//              RLock lock = redissonclient.getlock("order:" + userid);
//
//              //获取锁
//              boolean islock = lock.trylock();
//
//              //获取锁失败
//              if(!islock){
//                  return result.fail("不允许重复下单! ");
//              }
//
//              try {
//                  //获取事务代理对象
//                  ivoucherorderservice proxy = (ivoucherorderservice)aopcontext.currentproxy();
//                  return proxy.createvoucherorder(voucherid);
//              }finally {
//                  //释放锁
//                  lock.unlock();
//              }
//     */
//}

//private final BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);
//
//private class VoucherOrderHandler implements Runnable{
//
//    @Override
//    public void run() {
//        while (true){
//            try {
//                //1. 获取队列中的订单信息
//                VoucherOrder voucherOrder = orderTasks.take();
//                //2. 创建订单
//                handleVoucherOrder(voucherOrder);
//            } catch (Exception e) {
//                log.error("处理订单异常", e);
//            }
//        }
//
//    }


//}

