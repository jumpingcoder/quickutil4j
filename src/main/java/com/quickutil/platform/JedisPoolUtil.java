/**
 * Redis工具
 *
 * @class JedisUtil
 * @author 0.5
 */
package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JedisPoolUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(JedisPoolUtil.class);

    private Map<String, JedisUtil> jedisUtilMap = new HashMap<>();

    public JedisPoolUtil() {
    }

    public JedisPoolUtil(Properties jedisProperties, Properties jedisPoolProperties) {
        try {
            Enumeration<?> keys = jedisProperties.propertyNames();
            List<String> keyList = new ArrayList<String>();
            while (keys.hasMoreElements()) {
                String key = (String) keys.nextElement();
                key = key.split("\\.")[0];
                if (!keyList.contains(key)) {
                    keyList.add(key);
                }
            }
            for (String key : keyList) {
                String host = jedisProperties.getProperty(key + ".host");
                int port = Integer.parseInt(jedisProperties.getProperty(key + ".port"));
                int timeout = Integer.parseInt(jedisProperties.getProperty(key + ".timeout"));
                String password = jedisProperties.getProperty(key + ".password");
                int database = Integer.parseInt(jedisProperties.getProperty(key + ".database"));
                boolean isSsl = Boolean.parseBoolean(jedisProperties.getProperty(key + ".isSsl"));
                String caPath = jedisProperties.getProperty(key + ".caPath");
                jedisUtilMap.put(key, new JedisUtil(host, port, timeout, password, database, isSsl, caPath, jedisPoolProperties));
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
    }

    public void add(String key, String host, int port, int timeout, String password, int database, boolean isSsl, String caPath, Properties jedisPoolProperties) {
        jedisUtilMap.put(key, new JedisUtil(host, port, timeout, password, database, isSsl, caPath, jedisPoolProperties));
    }

    public JedisUtil get(String dbName) {
        return jedisUtilMap.get(dbName);
    }

    public void remove(String dbName) {
        JedisUtil jedisUtil = jedisUtilMap.get(dbName);
        if (jedisUtil == null)
            return;
        jedisUtil.closeJedis();
        jedisUtilMap.remove(dbName);
    }

}
