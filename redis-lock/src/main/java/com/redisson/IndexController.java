package com.zhuge;

import org.redisson.Redisson;
import org.redisson.RedissonRedLock;
import org.redisson.api.RLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.TimeUnit;

@RestController
public class IndexController {

    @Autowired
    private Redisson redisson;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private RedisTemplate redisTemplate;

    @RequestMapping("/deduct_stock")
    public String deductStock() {
        String lockKey = "lock:product_101";
        //Boolean result = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, "zhuge");
        //stringRedisTemplate.expire(lockKey, 10, TimeUnit.SECONDS);
        /*String clientId = UUID.randomUUID().toString();
        Boolean result = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, clientId, 30, TimeUnit.SECONDS); //jedis.setnx(k,v)
        if (!result) {
            return "error_code";
        }*/
        //获取锁对象
        RLock redissonLock = redisson.getLock(lockKey);
        //加分布式锁
        redissonLock.lock();  //  .setIfAbsent(lockKey, clientId, 30, TimeUnit.SECONDS);
        try {
            int stock = Integer.parseInt(stringRedisTemplate.opsForValue().get("stock")); // jedis.get("stock")
            if (stock > 0) {
                int realStock = stock - 1;
                stringRedisTemplate.opsForValue().set("stock", realStock + ""); // jedis.set(key,value)
                System.out.println("扣减成功，剩余库存:" + realStock);
            } else {
                System.out.println("扣减失败，库存不足");
            }
        } finally {
            /*if (clientId.equals(stringRedisTemplate.opsForValue().get(lockKey))) {
                stringRedisTemplate.delete(lockKey);
            }*/
            //解锁
            redissonLock.unlock();
        }


        return "end";
    }


    @RequestMapping("/redlock")
    public String redlock() {
        String lockKey = "product_001";
        //这里需要自己实例化不同redis实例的redisson客户端连接，这里只是伪代码用一个redisson客户端简化了
        RLock lock1 = redisson.getLock(lockKey);
        RLock lock2 = redisson.getLock(lockKey);
        RLock lock3 = redisson.getLock(lockKey);

        /**
         * 根据多个 RLock 对象构建 RedissonRedLock （最核心的差别就在这里）
         */
        RedissonRedLock redLock = new RedissonRedLock(lock1, lock2, lock3);
        try {
            /**
             * waitTimeout 尝试获取锁的最大等待时间，超过这个值，则认为获取锁失败
             * leaseTime   锁的持有时间,超过这个时间锁会自动失效（值应设置为大于业务处理的时间，确保在锁有效期内业务能处理完）
             */
            boolean res = redLock.tryLock(10, 30, TimeUnit.SECONDS);
            if (res) {
                //成功获得锁，在这里处理业务
            }
        } catch (Exception e) {
            throw new RuntimeException("lock fail");
        } finally {
            //无论如何, 最后都要解锁
            redLock.unlock();
        }

        return "end";
    }

}