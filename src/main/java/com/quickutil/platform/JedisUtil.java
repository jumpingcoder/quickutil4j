package com.quickutil.platform;

import ch.qos.logback.classic.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.quickutil.platform.constants.Symbol;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.params.SetParams;

/**
 * Redis工具
 *
 * @author 0.5
 */
public class JedisUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(JedisUtil.class);
    private static final String SPLIT = "::::";
    private static final String rpopQueueLua = "local list={}; for i=1,ARGV[1] do local element=redis.call('RPOP',KEYS[1]), if element then table.insert(list,tostring(element)); end; end; return list;";
    private static final String lpopQueueLua = "local list={}; for i=1,ARGV[1] do local element=redis.call('LPOP',KEYS[1]), if element then table.insert(list,tostring(element)); end; end; return list;";
    private static final String setHashLua = "for i=1,#ARGV,3 do redis.call('HSET',ARGV[i],ARGV[i+1],ARGV[i+2]); end;";
    private static final String getHashLua = "local list={};for i=1,#ARGV,2 do local element=redis.call('HGET',ARGV[i],ARGV[i+1]); if element then table.insert(list,tostring(element)); else table.insert(list,'NULL'); end;end;return list";
    private JedisPool jedisPool;

    public JedisUtil(String host, int port, int timeout, String password, int database, boolean isSsl, String caPath, Properties jedisPoolProperties) {
        jedisPool = buildJedisPool(host, port, timeout, password, database, isSsl, caPath, jedisPoolProperties);
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }

    public void closeJedis() {
        if (jedisPool == null) {
            return;
        }
        jedisPool.close();
    }

    private JedisPool buildJedisPool(String host, int port, int timeout, String password, int database, boolean isSsl, String caPath, Properties jedisPoolProperties) {
        if (isSsl) {
            if (caPath == null || caPath.isEmpty()) {
                LOGGER.error("证书路径不能为空!");
                return null;
            }
            System.setProperty("javax.net.ssl.trustStore", caPath);
        }
        JedisPoolConfig config = new JedisPoolConfig();
        if (jedisPoolProperties == null) {
            return new JedisPool(config, host, port, timeout, password, database, isSsl);
        }
        if (jedisPoolProperties.getProperty("BlockWhenExhausted") != null) {
            config.setBlockWhenExhausted(Boolean.parseBoolean(jedisPoolProperties.getProperty("BlockWhenExhausted")));
        }
        if (jedisPoolProperties.getProperty("EvictionPolicyClassName") != null) {
            config.setEvictionPolicyClassName(jedisPoolProperties.getProperty("EvictionPolicyClassName"));
        }
        if (jedisPoolProperties.getProperty("Fairness") != null) {
            config.setFairness(Boolean.parseBoolean(jedisPoolProperties.getProperty("Fairness")));
        }
        if (jedisPoolProperties.getProperty("JmxEnabled") != null) {
            config.setJmxEnabled(Boolean.parseBoolean(jedisPoolProperties.getProperty("JmxEnabled")));
        }
        if (jedisPoolProperties.getProperty("JmxNameBase") != null) {
            config.setJmxNameBase(jedisPoolProperties.getProperty("JmxNameBase"));
        }
        if (jedisPoolProperties.getProperty("JmxNamePrefix") != null) {
            config.setJmxNamePrefix(jedisPoolProperties.getProperty("JmxNamePrefix"));
        }
        if (jedisPoolProperties.getProperty("Lifo") != null) {
            config.setLifo(Boolean.parseBoolean(jedisPoolProperties.getProperty("Lifo")));
        }
        if (jedisPoolProperties.getProperty("MaxIdle") != null) {
            config.setMaxIdle(Integer.parseInt(jedisPoolProperties.getProperty("MaxIdle")));
        }
        if (jedisPoolProperties.getProperty("MaxTotal") != null) {
            config.setMaxTotal(Integer.parseInt(jedisPoolProperties.getProperty("MaxTotal")));
        }
        if (jedisPoolProperties.getProperty("MaxWaitMillis") != null) {
            config.setMaxWaitMillis(Long.parseLong(jedisPoolProperties.getProperty("MaxWaitMillis")));
        }
        if (jedisPoolProperties.getProperty("MinEvictableIdleTimeMillis") != null) {
            config.setMinEvictableIdleTimeMillis(Long.parseLong(jedisPoolProperties.getProperty("MinEvictableIdleTimeMillis")));
        }
        if (jedisPoolProperties.getProperty("MinIdle") != null) {
            config.setMinIdle(Integer.parseInt(jedisPoolProperties.getProperty("MinIdle")));
        }
        if (jedisPoolProperties.getProperty("NumTestsPerEvictionRun") != null) {
            config.setNumTestsPerEvictionRun(Integer.parseInt(jedisPoolProperties.getProperty("NumTestsPerEvictionRun")));
        }
        if (jedisPoolProperties.getProperty("SoftMinEvictableIdleTimeMillis") != null) {
            config.setSoftMinEvictableIdleTimeMillis(Long.parseLong(jedisPoolProperties.getProperty("SoftMinEvictableIdleTimeMillis")));
        }
        if (jedisPoolProperties.getProperty("TestOnBorrow") != null) {
            config.setTestOnBorrow(Boolean.parseBoolean(jedisPoolProperties.getProperty("TestOnBorrow")));
        }
        if (jedisPoolProperties.getProperty("TestOnCreate") != null) {
            config.setTestOnCreate(Boolean.parseBoolean(jedisPoolProperties.getProperty("TestOnCreate")));
        }
        if (jedisPoolProperties.getProperty("TestOnReturn") != null) {
            config.setTestOnReturn(Boolean.parseBoolean(jedisPoolProperties.getProperty("TestOnReturn")));
        }
        if (jedisPoolProperties.getProperty("TestWhileIdle") != null) {
            config.setTestWhileIdle(Boolean.parseBoolean(jedisPoolProperties.getProperty("TestWhileIdle")));
        }
        if (jedisPoolProperties.getProperty("AcquireIncrement") != null) {
            config.setTimeBetweenEvictionRunsMillis(Long.parseLong(jedisPoolProperties.getProperty("TimeBetweenEvictionRunsMillis")));
        }
        return new JedisPool(config, host, port, timeout, password, database, isSsl);
    }

    /**
     * @return 查询keys
     */
    public List<String> getKeys(String pattern) {
        Jedis jedis = jedisPool.getResource();
        List<String> keyList = new ArrayList<String>();
        if (pattern == null) {
            return keyList;
        }
        try {
            Iterator<String> iterator = jedis.keys(pattern).iterator();
            while (iterator.hasNext()) {
                keyList.add(iterator.next().toString());
            }
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return keyList;
    }

    /**
     * 删除key
     */
    public Long deleteKey(String key) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.del(key);
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
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
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    /**
     * 设置超时
     */
    public Long setExpire(String key, int seconds) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.expire(key, seconds);
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    /**
     * 存储字符串
     */
    public String setString(String key, String value, SetParams params) {
        if (key == null || value == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.set(key, value, params);
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    /**
     * 查询字符串
     */
    public String getString(String key) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.get(key);
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    public List<String> getStrings(List<String> keys) {
        if (keys == null) {
            return null;
        }
        List<Response<String>> responseList = new ArrayList<>();
        List<String> resultList = new ArrayList<>();
        Jedis jedis = jedisPool.getResource();
        Pipeline pipeline = jedis.pipelined();
        try {
            for (String key : keys) {
                responseList.add(pipeline.get(key));
            }
            pipeline.sync();
            for (Response<String> response : responseList) {
                resultList.add(response.get());
            }
            return resultList;
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            jedis.close();
        }
        return null;
    }

    public boolean setnxWithExpire(String key, String value, int expireSeconds) {
        Jedis jedis = jedisPool.getResource();
        try {
            return "OK".equals(jedis.set(key, value, new SetParams().nx().ex(expireSeconds)));
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return false;
    }

    /**
     * 左入队列
     */
    public Long lpush(String key, List<String> list) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.lpush(key, list.toArray(new String[list.size()]));
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    /**
     * 右入队列
     */
    public Long rpush(String key, List<String> list) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.rpush(key, list.toArray(new String[list.size()]));
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    /**
     * 左出队列
     */
    public String lpop(String key) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.lpop(key);
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    /**
     * 右出队列
     */
    public String rpop(String key) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.rpop(key);
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    /**
     * 左查询队列（不删除）
     */
    public List<String> lrange(String key, long start, long end) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.lrange(key, start, end);
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    /**
     * 批量右出队列（基于Lua）
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
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    /**
     * 批量左出队列（基于Lua）
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
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    /**
     * 写入哈希表
     */
    public String setHash(String key, Map<String, String> hash) {
        if (key == null || hash == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.hmset(key, hash);
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    /**
     * 批量写入哈希表（基于Lua）
     */
    public boolean setHashBulk(String jedisName, List<String> keyList, List<String> fieldList, List<String> valueList) {
        Jedis jedis = jedisPool.getResource();
        if (keyList == null || fieldList == null || valueList == null) {
            return false;
        }
        if (keyList.size() != fieldList.size() || keyList.size() != valueList.size()) {
            return false;
        }
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
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return false;
    }

    /**
     * 查询哈希表
     */
    public List<String> getHash(String key, List<String> fieldList) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.hmget(key, fieldList.toArray(new String[fieldList.size()]));
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    /**
     * 批量查询哈希表（基于Lua）
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
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    /**
     * 获取全部哈希表
     */
    public Map<String, String> getHashAll(String key) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.hgetAll(key);
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

    /**
     * 获取哈希表的全部值
     */
    public List<String> getHashAllValues(String key) {
        if (key == null) {
            return null;
        }
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.hvals(key);
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
        return null;
    }

}
