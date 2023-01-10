package com.zhuge.service;

import com.alibaba.fastjson.JSON;
import com.zhuge.common.RedisKeyPrefixConst;
import com.zhuge.common.RedisUtil;
import com.zhuge.dao.ProductDao;
import com.zhuge.model.Product;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Service
public class ProductService {

    @Autowired
    private ProductDao productDao;

    @Autowired
    private RedisUtil redisUtil;

    @Autowired
    private Redisson redisson;

    public static final Integer PRODUCT_CACHE_TIMEOUT = 60 * 60 * 24;
    public static final String EMPTY_CACHE = "{}";
    public static final String LOCK_PRODUCT_HOT_CACHE_PREFIX = "lock:product:hot_cache:";
    public static final String LOCK_PRODUCT_UPDATE_PREFIX = "lock:product:update:";
    public static Map<String, Product> productMap = new ConcurrentHashMap<>();

    @Transactional
    public Product create(Product product) {
        // 此处也需要加分布式锁，为解决双写不一致问题
        Product productResult = productDao.create(product);
        redisUtil.set(RedisKeyPrefixConst.PRODUCT_CACHE + productResult.getId(), JSON.toJSONString(productResult),
                genProductCacheTimeout(), TimeUnit.SECONDS);
        return productResult;
    }

    @Transactional
    public Product update(Product product) {
        Product productResult = null;

        //RLock updateProductLock = redisson.getLock(LOCK_PRODUCT_UPDATE_PREFIX + product.getId());
        RReadWriteLock readWriteLock = redisson.getReadWriteLock(LOCK_PRODUCT_UPDATE_PREFIX + product.getId()); // 解决双写不一致问题,加读写锁
        RLock writeLock = readWriteLock.writeLock(); // 写锁
        writeLock.lock();
        try {
            productResult = productDao.update(product); // 修改商品数据
            redisUtil.set(RedisKeyPrefixConst.PRODUCT_CACHE + productResult.getId(), JSON.toJSONString(productResult),
                    genProductCacheTimeout(), TimeUnit.SECONDS); // 防止缓存失效(击穿),设置随机的过期时间，
            productMap.put(RedisKeyPrefixConst.PRODUCT_CACHE + productResult.getId(), product);
        } finally {
            writeLock.unlock();
        }
        return productResult;
    }

    public Product get(Long productId) throws InterruptedException {
        Product product = null;
        String productCacheKey = RedisKeyPrefixConst.PRODUCT_CACHE + productId;

        product = getProductFromCache(productCacheKey);
        if (product != null) {
            return product;
        }
        //DCL,解决热点缓存并发重建问题 ----> 加分布式锁
        RLock hotCacheLock = redisson.getLock(LOCK_PRODUCT_HOT_CACHE_PREFIX + productId);
        hotCacheLock.lock();
        /**
         * 串行转并发的方案:
         * 假设有10万条请求来查询数据,当第一个锁查询到数据并存储缓存后，后续的请求加锁失败后，可直接从缓存那数据，避免10万条请求都要做一次加解锁。
         * 但也有弊端，万一第一个拿到锁的请求在限时时间内未完成查询，就会出现BUG
         */
        //boolean result = hotCacheLock.tryLock(3, TimeUnit.SECONDS); // 如果3秒内加锁成功,返回true,反之返回false;
        try {
            product = getProductFromCache(productCacheKey);
            if (product != null) {
                return product;
            }

            //RLock updateProductLock = redisson.getLock(LOCK_PRODUCT_UPDATE_PREFIX + productId);
            RReadWriteLock readWriteLock = redisson.getReadWriteLock(LOCK_PRODUCT_UPDATE_PREFIX + productId); // 解决双写不一致问题,加读写锁,注意读写锁的锁必须一致
            RLock rLock = readWriteLock.readLock(); // 读锁,多个线程可以同时加锁成功，并行执行，不存在互斥，大大提高查询效率(底层重入逻辑，值加一)
            rLock.lock();
            try {
                product = productDao.get(productId);
                if (product != null) {
                    redisUtil.set(productCacheKey, JSON.toJSONString(product),
                            genProductCacheTimeout(), TimeUnit.SECONDS); // 查询数据不为空,更新缓存数据
                    productMap.put(productCacheKey, product);
                } else { // 防止缓存穿透,设置空缓存;  设置过期时间,防止黑客攻击,设置过期时间()
                    redisUtil.set(productCacheKey, EMPTY_CACHE, genEmptyCacheTimeout(), TimeUnit.SECONDS);
                }
            } finally {
                rLock.unlock(); // (底层重入逻辑，值减一)
            }
        } finally {
            hotCacheLock.unlock();
        }
        return product;
    }


    private Integer genProductCacheTimeout() { // 防止缓存失效(击穿),随机过期时间
        return PRODUCT_CACHE_TIMEOUT + new Random().nextInt(5) * 60 * 60;
    }

    private Integer genEmptyCacheTimeout() { // 防止缓存穿透,随机过期时间
        return 60 + new Random().nextInt(30);
    }

    private Product getProductFromCache(String productCacheKey) { // 从缓存中读取商品信息; 两种情况：一种缓存数据为空,一种为真数据
        Product product = productMap.get(productCacheKey);
        if (product != null) { // 防止缓存与数据库都没数据而生成的缓存，返回为空商品信息
            return product;
        }

        String productStr = redisUtil.get(productCacheKey);
        if (!StringUtils.isEmpty(productStr)) {
            if (EMPTY_CACHE.equals(productStr)) { // 空缓存情况下，延期过期时间
                redisUtil.expire(productCacheKey, genEmptyCacheTimeout(), TimeUnit.SECONDS);
                return new Product();
            }
            product = JSON.parseObject(productStr, Product.class);
            redisUtil.expire(productCacheKey, genProductCacheTimeout(), TimeUnit.SECONDS); //读延期
        }
        return product;
    }

}
