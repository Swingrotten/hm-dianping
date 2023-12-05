package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    public UserServiceImpl(StringRedisTemplate redisTemplate) {
        this.stringRedisTemplate = redisTemplate;
    }
    @Override
    public Result sendCode(String phone, HttpSession session) {
        //验证手机号
        if(RegexUtils.isPhoneInvalid(phone)){
            //如果不符合,返回错误信息
            return Result.fail("404");

        }
        //符合,生成验证码
        String code = RandomUtil.randomNumbers(6);

        //保存验证码到redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code,60,TimeUnit.SECONDS);

        //发送验证码
        log.debug("发送成功 , 验证码: {}", code);

        //返回成功
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //  验证手机号
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone)){
            //   如果不符合返回错误信息
            return Result.fail("手机号格式错误");
        }

        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);

        String code = loginForm.getCode();
        if(cacheCode == null || !cacheCode.toString().equals(code)){

            return Result.fail("验证码错误");
        }

        User user = query().eq("phone", phone).one();

        if(user == null){
            user = createUserWithPhone(phone);
        }


        //    保存用户信息到redis

        //随机生成token,作为登录令牌
        String token = UUID.randomUUID().toString(true);

        //将User对象转为HashMap储存
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO);

        // 将 long 类型的值转换为 String 类型
        for (Map.Entry<String, Object> entry : userMap.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Long) {
                entry.setValue(String.valueOf(value));
            }
        }

        //存储
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey,userMap);


        //设置token有效期
        stringRedisTemplate.expire(tokenKey,LOGIN_USER_TTL , TimeUnit.MINUTES);

        //返回token
        return Result.ok(token);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(9));
        save(user);
        return  user;
    }
}
