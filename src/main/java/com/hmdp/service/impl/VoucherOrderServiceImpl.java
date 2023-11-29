package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@RequiredArgsConstructor
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    private final ISeckillVoucherService seckillVoucherService;

    private final RedisIdWorker redisIdWorker;
    @Override
    @Transactional
    public Result seckillVoucher(Long voucherId) {
        //1. 查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2. 判断秒杀是否开始
        if(voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始! ");
        }

        //3. 判断秒杀是否结束
        if(voucher.getEndTime().isBefore(LocalDateTime.now())){
            return Result.fail("秒杀已结束! ");
        }

        //4. 判断库存是否充足
        if(voucher.getStock()<1){
            return Result.fail("库存不足! ");
        }

        //5. 扣减库存
        boolean update = seckillVoucherService.update()
                .setSql("stock=stock-1")
                .eq("voucher_id", voucherId).update();
        if(!update){
            return Result.fail("库存不足! ");
        }

        //6. 创建工单
        VoucherOrder voucherOrder = new VoucherOrder();
        //6.1 订单Id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //6.2 用户Id
        Long userId = UserHolder.getUser().getId();
        voucherOrder.setUserId(userId);
        //6.3 代金券Id
        voucherOrder.setVoucherId(voucherId);
        //6.4 保存
        save(voucherOrder);

        return Result.ok(orderId);
    }
}
