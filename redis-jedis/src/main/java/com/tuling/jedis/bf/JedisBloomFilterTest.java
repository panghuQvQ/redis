package com.tuling.jedis.bf;

import com.google.common.hash.Funnels;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * 访问redis单机
 *
 * @author 图灵-诸葛老师
 */
public class JedisBloomFilterTest {
    public static void main(String[] args) throws IOException {

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(10);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setMinIdle(2);

        // timeout，这里既是连接超时又是读写超时，从Jedis 2.8开始有区分connectionTimeout和soTimeout的构造函数
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, "192.168.0.60", 6380, 3000, null);

        Jedis jedis = null;
        try {
            //从redis连接池里拿出一个连接执行命令
            jedis = jedisPool.getResource();

            //******* Redis测试布隆方法 ********
            BloomFilterHelper<CharSequence> bloomFilterHelper = new BloomFilterHelper<>(Funnels.stringFunnel(Charset.defaultCharset()), 1000, 0.1);
            RedisBloomFilter<Object> redisBloomFilter = new RedisBloomFilter<>(jedis);
            int j = 0;
            for (int i = 0; i < 100; i++) {
                redisBloomFilter.addByBloomFilter(bloomFilterHelper, "bloom", i+"");
            }
            for (int i = 0; i < 1000; i++) {
                boolean result = redisBloomFilter.includeByBloomFilter(bloomFilterHelper, "bloom", i+"");
                if (!result) {
                    j++;
                }
            }
            System.out.println("漏掉了" + j + "个");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //注意这里不是关闭连接，在JedisPool模式下，Jedis会被归还给资源池。
            if (jedis != null)
                jedis.close();
        }
    }
}
