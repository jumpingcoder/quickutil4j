package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;

/**
 * Redis工具
 *
 * @author 0.5
 */
public class JedisUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(JedisUtil.class);
    private static final String SPLIT = "::::";
    private JedisPool jedisPool;

    public JedisUtil(String host, int port, int timeout, String password, int database, boolean isSsl, String caPath, Properties jedisPoolProperties) {
        jedisPool = buildJedisPool(host, port, timeout, password, database, isSsl, caPath, jedisPoolProperties);
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void closeJedis() {
        if (jedisPool == null)
            return;
        jedisPool.close();
    }

    private JedisPool buildJedisPool(String host, int port, int timeout, String password, int database, boolean isSsl, String caPath, Properties pool) {
        if (isSsl) {
            if (caPath == null || caPath.isEmpty()) {
                LOGGER.error("证书路径不能为空!");
                return null;
            }
            System.setProperty("javax.net.ssl.trustStore", caPath);
        }
        JedisPoolConfig config = new JedisPoolConfig();
        if (pool == null)
            return new JedisPool(config, host, port, timeout, password, database, isSsl);
        if (pool.getProperty("BlockWhenExhausted") != null)
            config.setBlockWhenExhausted(Boolean.parseBoolean(pool.getProperty("BlockWhenExhausted")));
        if (pool.getProperty("EvictionPolicyClassName") != null)
            config.setEvictionPolicyClassName(pool.getProperty("EvictionPolicyClassName"));
        if (pool.getProperty("Fairness") != null)
            config.setFairness(Boolean.parseBoolean(pool.getProperty("Fairness")));
        if (pool.getProperty("JmxEnabled") != null)
            config.setJmxEnabled(Boolean.parseBoolean(pool.getProperty("JmxEnabled")));
        if (pool.getProperty("JmxNameBase") != null)
            config.setJmxNameBase(pool.getProperty("JmxNameBase"));
        if (pool.getProperty("JmxNamePrefix") != null)
            config.setJmxNamePrefix(pool.getProperty("JmxNamePrefix"));
        if (pool.getProperty("Lifo") != null)
            config.setLifo(Boolean.parseBoolean(pool.getProperty("Lifo")));
        if (pool.getProperty("MaxIdle") != null)
            config.setMaxIdle(Integer.parseInt(pool.getProperty("MaxIdle")));
        if (pool.getProperty("MaxTotal") != null)
            config.setMaxTotal(Integer.parseInt(pool.getProperty("MaxTotal")));
        if (pool.getProperty("MaxWaitMillis") != null)
            config.setMaxWaitMillis(Long.parseLong(pool.getProperty("MaxWaitMillis")));
        if (pool.getProperty("MinEvictableIdleTimeMillis") != null)
            config.setMinEvictableIdleTimeMillis(Long.parseLong(pool.getProperty("MinEvictableIdleTimeMillis")));
        if (pool.getProperty("MinIdle") != null)
            config.setMinIdle(Integer.parseInt(pool.getProperty("MinIdle")));
        if (pool.getProperty("NumTestsPerEvictionRun") != null)
            config.setNumTestsPerEvictionRun(Integer.parseInt(pool.getProperty("NumTestsPerEvictionRun")));
        if (pool.getProperty("SoftMinEvictableIdleTimeMillis") != null)
            config.setSoftMinEvictableIdleTimeMillis(Long.parseLong(pool.getProperty("SoftMinEvictableIdleTimeMillis")));
        if (pool.getProperty("TestOnBorrow") != null)
            config.setTestOnBorrow(Boolean.parseBoolean(pool.getProperty("TestOnBorrow")));
        if (pool.getProperty("TestOnCreate") != null)
            config.setTestOnCreate(Boolean.parseBoolean(pool.getProperty("TestOnCreate")));
        if (pool.getProperty("TestOnReturn") != null)
            config.setTestOnReturn(Boolean.parseBoolean(pool.getProperty("TestOnReturn")));
        if (pool.getProperty("TestWhileIdle") != null)
            config.setTestWhileIdle(Boolean.parseBoolean(pool.getProperty("TestWhileIdle")));
        if (pool.getProperty("AcquireIncrement") != null)
            config.setTimeBetweenEvictionRunsMillis(Long.parseLong(pool.getProperty("TimeBetweenEvictionRunsMillis")));
        return new JedisPool(config, host, port, timeout, password, database, isSsl);
    }

    /**
     * @param pattern-表达式
     * @return 查询keys
     */
    public List<String> getKeys(String pattern) {
        Jedis jedis = jedisPool.getResource();
        List<String> keyList = new ArrayList<String>();
        if (pattern == null)
            return keyList;
        try {
            Iterator<String> iterator = jedis.keys(pattern).iterator();
            while (iterator.hasNext()) {
                keyList.add(iterator.next().toString());
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return keyList;
    }

    /**
     * 删除key
     *
     * @param key-key名
     * @return
     */
    public Long deleteKey(String key) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.del(key);
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

    /**
     * 清空数据
     */
    public String flushData() {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.flushDB();
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

    /**
     * 设置超时
     *
     * @param key-key名
     * @param seconds-有效时间
     */
    public Long setExpire(String key, int seconds) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.expire(key, seconds);
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

    /**
     * 存储字符串
     *
     * @param key-key名
     * @param value-数据内容
     */
    public String setString(String key, String value) {
        if (key == null || value == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.set(key, value);
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

    /**
     * 查询字符串
     *
     * @param key-key名
     * @return
     */
    public String getString(String key) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.get(key);
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

    /**
     * 左入队列
     *
     * @param key-key名
     * @param list-数据内容
     */
    public Long lpush(String key, List<String> list) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.lpush(key, list.toArray(new String[list.size()]));
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

    /**
     * 右入队列
     *
     * @param key-key名
     * @param list-数据内容
     */
    public Long rpush(String key, List<String> list) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.rpush(key, list.toArray(new String[list.size()]));
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

    /**
     * 左出队列
     *
     * @param key-key名
     */
    public String lpop(String key) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.lpop(key);
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

    /**
     * 右出队列
     *
     * @param key-key名
     */
    public String rpop(String key) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.rpop(key);
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

    /**
     * 左查询队列（不删除）
     *
     * @param key-key名
     * @param start-起始位置
     * @param end-结束位置
     * @return
     */
    public List<String> lrange(String key, long start, long end) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.lrange(key, start, end);
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

    private static final String rpopQueueLua = "local list={}; for i=1,ARGV[1] do local element=redis.call('RPOP',KEYS[1]), if element then table.insert(list,tostring(element)); end; end; return list;";

    /**
     * 批量右出队列（基于Lua）
     *
     * @param key-key名
     * @param count-数量
     * @return
     */
    @SuppressWarnings("unchecked")
    public List<String> rpopBulk(String key, Integer count) {
        if (key == null || count == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            List<String> list = (List<String>) jedis.eval(rpopQueueLua, 1, key, count.toString());
            return list;
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

    private static final String lpopQueueLua = "local list={}; for i=1,ARGV[1] do local element=redis.call('LPOP',KEYS[1]), if element then table.insert(list,tostring(element)); end; end; return list;";

    /**
     * 批量左出队列（基于Lua）
     *
     * @param key-key名
     * @param count-数量
     * @return
     */
    @SuppressWarnings("unchecked")
    public List<String> lpopBulk(String key, Integer count) {
        if (key == null || count == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            List<String> list = (List<String>) jedis.eval(lpopQueueLua, 1, key, count.toString());
            return list;
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }


    /**
     * 写入哈希表
     *
     * @param key-key名
     * @param hash-哈希表
     * @return
     */
    public String setHash(String key, Map<String, String> hash) {
        if (key == null || hash == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.hmset(key, hash);
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

    private static final String setHashLua = "for i=1,#ARGV,3 do redis.call('HSET',ARGV[i],ARGV[i+1],ARGV[i+2]); end;";

    /**
     * 批量写入哈希表（基于Lua）
     *
     * @param keyList-key数组，一一对应
     * @param fieldList-field数组，一一对应
     * @param valueList-value数组，一一对应
     * @return
     */
    public boolean setHashBulk(String jedisName, List<String> keyList, List<String> fieldList, List<String> valueList) {
        Jedis jedis = jedisPool.getResource();
        if (keyList == null || fieldList == null || valueList == null)
            return false;
        if (keyList.size() != fieldList.size() || keyList.size() != valueList.size())
            return false;
        try {
            List<String> paramList = new LinkedList<String>();
            for (int i = 0; i < keyList.size(); i++) {
                paramList.add(keyList.get(i));
                paramList.add(fieldList.get(i));
                paramList.add(valueList.get(i));
            }
            String[] params = paramList.toArray(new String[paramList.size()]);
            jedis.eval(setHashLua, 0, params);
            return true;
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return false;
    }

    /**
     * 查询哈希表
     *
     * @param key-key名
     * @param fieldList-field数组
     * @return
     */
    public List<String> getHash(String key, List<String> fieldList) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.hmget(key, fieldList.toArray(new String[fieldList.size()]));
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

    private static final String getHashLua = "local list={};for i=1,#ARGV,2 do local element=redis.call('HGET',ARGV[i],ARGV[i+1]); if element then table.insert(list,tostring(element)); else table.insert(list,'NULL'); end;end;return list";

    /**
     * 批量查询哈希表（基于Lua）
     *
     * @param keyList-key数组，一一对应
     * @param fieldList-field数组，一一对应
     * @return
     */
    @SuppressWarnings("unchecked")
    public List<String> getHashBulk(List<String> keyList, List<String> fieldList) {
        Jedis jedis = jedisPool.getResource();
        try {
            List<String> paramList = new LinkedList<String>();
            for (int i = 0; i < keyList.size(); i++) {
                paramList.add(keyList.get(i));
                paramList.add(fieldList.get(i));
            }
            String[] params = paramList.toArray(new String[paramList.size()]);
            return (List<String>) jedis.eval(getHashLua, 0, params);
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

    /**
     * 获取全部哈希表
     *
     * @param key-key名
     * @return
     */
    public Map<String, String> getHashAll(String key) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.hgetAll(key);
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

    /**
     * 获取哈希表的全部值
     *
     * @param key-key名
     * @return
     */
    public List<String> getHashAllValues(String key) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.hvals(key);
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            if (jedis != null)
                jedis.close();
        }
        return null;
    }

}
