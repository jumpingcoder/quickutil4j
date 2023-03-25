package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.constants.Symbol;
import com.quickutil.platform.exception.MissingParametersException;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Redis工具-配置池
 *
 * @author 0.5
 */
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
                String portStr = jedisProperties.getProperty(key + ".port");
                if (host == null || portStr == null)
                    throw new MissingParametersException("init requires host, port");
                int port = Integer.parseInt(portStr);
                String timeoutStr = jedisProperties.getProperty(key + ".timeout");
                int timeout = timeoutStr == null ? 2000 : Integer.parseInt(timeoutStr);
                String databaseStr = jedisProperties.getProperty(key + ".database");
                int database = databaseStr == null ? 0 : Integer.parseInt(databaseStr);
                String password = jedisProperties.getProperty(key + ".password");
                String isSslStr = jedisProperties.getProperty(key + ".isSsl");
                boolean isSsl = isSslStr == null ? false : Boolean.parseBoolean(isSslStr);
                String caPath = jedisProperties.getProperty(key + ".caPath");
                jedisUtilMap.put(key, new JedisUtil(host, port, timeout, password, database, isSsl, caPath, jedisPoolProperties));
            }
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        }
    }

    public JedisUtil get(String redisName) {
        return jedisUtilMap.get(redisName);
    }

    public void add(String redisName, JedisUtil jedis) {
        jedisUtilMap.put(redisName, jedis);
    }

    public void remove(String redisName) {
        JedisUtil jedisUtil = jedisUtilMap.get(redisName);
        if (jedisUtil == null)
            return;
        jedisUtil.closeJedis();
        jedisUtilMap.remove(redisName);
    }

}
