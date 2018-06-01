/**
 * Redis工具
 * 
 * @class JedisUtil
 * @author 0.5
 */
package com.quickutil.platform;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisUtil {

	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(JedisUtil.class);

	private static Map<String, JedisPool> JedisPoolMap = new HashMap<String, JedisPool>();
	private static final String SPLIT = "::::";

	/**
	 * 增加JedisPool，默认pool配置
	 * 
	 * @param jedis-jedis配置
	 */
	public static boolean addJedisPool(Properties jedis) {
		return addJedisPool(jedis, null);
	}

	/**
	 * 增加JedisPool，指定pool配置
	 * 
	 * @param jedis-jedis配置
	 * @param pool-pool配置
	 */
	public static boolean addJedisPool(Properties jedis, Properties pool) {
		try {
			Enumeration<?> keys = jedis.propertyNames();
			List<String> keyList = new ArrayList<String>();
			while (keys.hasMoreElements()) {
				String key = (String) keys.nextElement();
				key = key.split("\\.")[0];
				if (!keyList.contains(key)) {
					keyList.add(key);
				}
			}
			for (String key : keyList) {
				String host = jedis.getProperty(key + ".host");
				int port = Integer.parseInt(jedis.getProperty(key + ".port"));
				int timeout = Integer.parseInt(jedis.getProperty(key + ".timeout"));
				String password = jedis.getProperty(key + ".password");
				int database = Integer.parseInt(jedis.getProperty(key + ".database"));
				JedisPoolMap.put(key, buildJedisPool(host, port, timeout, password, database, pool));
			}
			return true;
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return false;
	}

	/**
	 * 增加JedisPool，默认pool配置
	 * 
	 * @param dbName-数据库名称
	 * @param host-数据库HOST
	 * @param port-数据库端口
	 * @param timeout-超时时间
	 * @param password-密码
	 * @param database-数据库编号
	 */
	public static void addJedisPool(String dbName, String host, int port, int timeout, String password, int database) {
		JedisPoolMap.put(dbName, buildJedisPool(host, port, timeout, password, database, null));
	}

	/**
	 * 增加JedisPool，指定pool配置
	 * 
	 * @param dbName-数据库名称
	 * @param host-数据库HOST
	 * @param port-数据库端口
	 * @param timeout-超时时间
	 * @param password-密码
	 * @param database-数据库编号
	 * @param pool-pool配置
	 */
	public static void addJedisPool(String dbName, String host, int port, int timeout, String password, int database, Properties pool) {
		JedisPoolMap.put(dbName, buildJedisPool(host, port, timeout, password, database, pool));
	}

	/**
	 * 获取生成的JedisPool
	 * 
	 * @param dbName-数据库名称
	 * @return
	 */
	public static JedisPool getJedisPool(String dbName) {
		return JedisPoolMap.get(dbName);
	}

	/**
	 * 释放生成的JedisPool
	 * 
	 * @param dbName
	 */
	public void destroyJedisPool(String dbName) {
		JedisPool pool = JedisPoolMap.get(dbName);
		if (pool == null)
			return;
		pool.destroy();
		JedisPoolMap.remove(dbName);
	}

	/**
	 * 释放所有的的JedisPool
	 * 
	 */
	public void destroyAllJedisPool() {
		for (String dbName : JedisPoolMap.keySet()) {
			destroyJedisPool(dbName);
		}
	}

	private static JedisPool buildJedisPool(String host, int port, int timeout, String password, int database, Properties pool) {
		JedisPoolConfig config = new JedisPoolConfig();
		if (pool == null)
			return new JedisPool(config, host, port, timeout, password, database);
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
		return new JedisPool(config, host, port, timeout, password, database);
	}

	/**
	 * 查询keys
	 * 
	 * @param dbName-数据库名称
	 * @param pattern-表达式
	 * @return
	 */
	public static List<String> getKeys(String dbName, String pattern) {
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
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
	 * @param dbName-数据库名称
	 * @param key-key名
	 */
	public static Long deleteKey(String dbName, String key) {
		if (key == null) {
			return null;
		}
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
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
	 * 
	 * @param dbName-数据库名称
	 */
	public static String flushData(String dbName) {
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
		return jedis.flushDB();
	}

	/**
	 * 设置超时
	 * 
	 * @param dbName-数据库名称
	 * @param key-key名
	 * @param seconds-有效时间
	 */
	public static Long setExpire(String dbName, String key, int seconds) {
		if (key == null) {
			return null;
		}
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
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
	 * @param dbName-数据库名称
	 * @param key-key名
	 * @param value-数据内容
	 */
	public static String setString(String dbName, String key, String value) {
		if (key == null || value == null) {
			return null;
		}
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
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
	 * @param dbName-数据库名称
	 * @param key-key名
	 * @return
	 */
	public static String getString(String dbName, String key) {
        if (key == null ) {
            return null;
        }
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
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
	 * 写入队列
	 * 
	 * @param dbName-数据库名称
	 * @param key-key名
	 * @param list-数据内容
	 */
	public static Long pushQueue(String dbName, String key, List<String> list) {
        if (key == null ) {
            return null;
        }
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
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
	 * 取出队列
	 * 
	 * @param dbName-数据库名称
	 * @param key-key名
	 */
	public static String popQueue(String dbName, String key) {
        if (key == null ) {
            return null;
        }
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
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

	private static final String popQueueLua = "local list={}; for i=1,ARGV[1] do local element=redis.call('RPOP',KEYS[1]), if element then table.insert(list,tostring(element)); end; end; return list;";

	/**
	 * 取出队列（基于Lua）
	 * 
	 * @param dbName-数据库名称
	 * @param key-key名
	 * @param count-数量
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static List<String> popQueueX(String dbName, String key, Integer count) {
        if (key == null || count == null) {
            return null;
        }
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
		try {
			List<String> list = (List<String>) jedis.eval(popQueueLua, 1, key, count.toString());
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
	 * 放回队列
	 * 
	 * @param dbName-数据库名称
	 * @param key-key名
	 * @param list-数据内容
	 */
	public static Long backtoQueue(String dbName, String key, List<String> list) {
        if (key == null ) {
            return null;
        }
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
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
	 * 查询队列（不删除）
	 * 
	 * @param dbName-数据库名称
	 * @param key-key名
	 * @param start-起始位置
	 * @param end-结束位置
	 * @return
	 */
	public static List<String> rangeQueue(String dbName, String key, long start, long end) {
        if (key == null) {
            return null;
        }
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
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

	/**
	 * 写入哈希表
	 * 
	 * @param dbName-数据库名称
	 * @param key-key名
	 * @param hash-哈希表
	 * @return
	 */
	public static String setHash(String dbName, String key, Map<String, String> hash) {
        if (key == null || hash == null) {
            return null;
        }
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
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
	 * 写入哈希表（基于Lua）
	 * 
	 * @param dbName-数据库名称
	 * @param keyList-key数组，一一对应
	 * @param fieldList-field数组，一一对应
	 * @param valueList-value数组，一一对应
	 * @return
	 */
	public static boolean setHashX(String jedisName, List<String> keyList, List<String> fieldList, List<String> valueList) {
		JedisPool pool = JedisPoolMap.get(jedisName);
		Jedis jedis = pool.getResource();
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
	 * @param dbName-数据库名称
	 * @param key-key名
	 * @param fieldList-field数组
	 * @return
	 */
	public static List<String> getHash(String dbName, String key, List<String> fieldList) {
        if (key == null) {
            return null;
        }
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
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
	 * 查询哈希表（基于Lua）
	 * 
	 * @param dbName-数据库名称
	 * @param keyList-key数组，一一对应
	 * @param fieldList-field数组，一一对应
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static List<String> getHashX(String dbName, List<String> keyList, List<String> fieldList) {
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
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
	 * @param dbName-数据库名称
	 * @param key-key名
	 * @return
	 */
	public static Map<String, String> getHashAll(String dbName, String key) {
        if (key == null) {
            return null;
        }
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
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
	 * @param dbName-数据库名称
	 * @param key-key名
	 * @return
	 */
	public static List<String> getHashAllValues(String dbName, String key) {
        if (key == null) {
            return null;
        }
		JedisPool pool = (JedisPool) JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
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

	private static final String setHashTableLua = "for i=1,#ARGV,3 do redis.call('HSET',ARGV[i],ARGV[i+1],ARGV[i+2]); end;";

	/**
	 * 行存储转kv存储，写入数据（基于Lua）
	 * 
	 * @param dbName-数据库名称
	 * @param key-key名
	 * @param hashTable-数据哈希表
	 * @return
	 */
	public static boolean setHashTable(String dbName, String key, Map<String, Map<String, Object>> hashTable) {
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
		try {
			List<String> list = new LinkedList<String>();
			for (String tablekey : hashTable.keySet()) {
				Map<String, Object> column = hashTable.get(tablekey);
				for (String columnkey : column.keySet()) {
					list.add(key);
					list.add(tablekey + SPLIT + columnkey);
					list.add(column.get(columnkey).toString());
				}
			}
			String[] params = list.toArray(new String[list.size()]);
			jedis.eval(setHashTableLua, 0, params);
			return true;
		} catch (Exception e) {
			LOGGER.error("", e);
		} finally {
			if (jedis != null)
				jedis.close();
		}
		return false;
	}

	private static final String getHashTableLua = "local list={}; local resultTable=redis.call('HGETALL',ARGV[1]); for i=2,#ARGV,1 do for j=1,#resultTable,2 do local index=string.find(resultTable[j],ARGV[i]) if(index) then table.insert(list,resultTable[j]);  table.insert(list,resultTable[j+1]);  end; end; end; return list;";

	/**
	 * 行存储转kv存储，查询数据（基于Lua）
	 * 
	 * @param dbName-数据库名称
	 * @param key-key名
	 * @param tableList-表名
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Map<String, Object>> getHashTable(String dbName, String key, List<String> tableList) {
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
		try {
			tableList.add(0, key);
			String[] params = tableList.toArray(new String[tableList.size()]);
			List<String> list = (List<String>) jedis.eval(getHashTableLua, 0, params);
			Map<String, Map<String, Object>> map = new HashMap<>();
			for (int i = 0; i < list.size() - 1; i = i + 2) {
				String[] tableAndColumn = list.get(i).split(SPLIT);
				String tableName = tableAndColumn[0];
				String columnName = tableAndColumn[1];
				String value = list.get(i + 1);
				if (map.get(tableName) == null)
					map.put(tableName, new HashMap<>());
				map.get(tableName).put(columnName, value);
			}
			return map;
		} catch (Exception e) {
			LOGGER.error("", e);
		} finally {
			if (jedis != null)
				jedis.close();
		}
		return null;
	}

	private static final String setHincrbyLua = "local list={}; for i=1,#ARGV,3 do local element=redis.call('HINCRBY',ARGV[i],ARGV[i+1],ARGV[i+2]); end;";

	/**
	 * 哈希表计数器（基于Lua）
	 * 
	 * @param dbName-数据库名称
	 * @param keyList-key数组，一一对应
	 * @param fieldList-field数组，一一对应
	 * @param countList-count数组，一一对应
	 */
	public static boolean setHincrby(String dbName, List<String> keyList, List<String> fieldList, List<Long> countList) {
		JedisPool pool = JedisPoolMap.get(dbName);
		Jedis jedis = pool.getResource();
		try {
			List<String> paramList = new LinkedList<String>();
			for (int i = 0; i < keyList.size(); i++) {
				paramList.add(keyList.get(i));
				paramList.add(fieldList.get(i));
				paramList.add(countList.get(i).toString());
			}
			String[] params = paramList.toArray(new String[paramList.size()]);
			jedis.eval(setHincrbyLua, 0, params);
			return true;
		} catch (Exception e) {
			LOGGER.error("", e);
		} finally {
			if (jedis != null)
				jedis.close();
		}
		return false;
	}
}
