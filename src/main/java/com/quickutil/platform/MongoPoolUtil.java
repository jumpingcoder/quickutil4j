package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.constants.Symbol;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * MongoDB客户端工具-配置池
 *
 * @author 0.5
 */
public class MongoPoolUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(MongoPoolUtil.class);

    public static Map<String, MongoUtil> mongoUtilMap = new HashMap<>();

    public MongoPoolUtil() {
    }

    public MongoPoolUtil(Properties mongoProperties, Properties builderProperties) {
        Enumeration<?> keys = mongoProperties.propertyNames();
        List<String> keyList = new ArrayList<String>();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            key = key.split("\\.")[0];
            if (!keyList.contains(key)) {
                keyList.add(key);
            }
        }
        for (String key : keyList) {
            String username = mongoProperties.getProperty(key + ".username");
            String password = mongoProperties.getProperty(key + ".password");
            String host = mongoProperties.getProperty(key + ".host");
            int port = Integer.parseInt(mongoProperties.getProperty(key + ".port"));
            String database = mongoProperties.getProperty(key + ".database");
            try {
                mongoUtilMap.put(key, new MongoUtil(username, password, host, port, database, builderProperties));
            } catch (Exception e) {
                LOGGER.error(Symbol.BLANK, e);
            }
        }
    }

    public void add(String key, String username, String password, String host, int port, String database, Properties builderProperties) {
        mongoUtilMap.put(key, new MongoUtil(username, password, host, port, database, builderProperties));
    }

    public MongoUtil get(String key) {
        return mongoUtilMap.get(key);
    }

    public void remove(String key) {
        mongoUtilMap.get(key).closeMongoDB();
        mongoUtilMap.remove(key);
    }
}
