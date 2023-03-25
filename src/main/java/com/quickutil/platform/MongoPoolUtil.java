package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.constants.Symbol;
import com.quickutil.platform.exception.MissingParametersException;
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
            try {
                String username = mongoProperties.getProperty(key + ".username");
                String password = mongoProperties.getProperty(key + ".password");
                String host = mongoProperties.getProperty(key + ".host");
                String portStr = mongoProperties.getProperty(key + ".port");
                String database = mongoProperties.getProperty(key + ".database");
                if (host == null || portStr == null || username == null || password == null || database == null)
                    throw new MissingParametersException("init requires host, port, username, password, database");
                int port = Integer.parseInt(portStr);
                mongoUtilMap.put(key, new MongoUtil(username, password, host, port, database, builderProperties));
            } catch (Exception e) {
                LOGGER.error(Symbol.BLANK, e);
            }
        }
    }

    public void add(String mongoName, MongoUtil mongo) {
        mongoUtilMap.put(mongoName, mongo);
    }

    public MongoUtil get(String mongoName) {
        return mongoUtilMap.get(mongoName);
    }

    public void remove(String mongoName) {
        MongoUtil mongo = mongoUtilMap.get(mongoName);
        if (mongo == null)
            return;
        mongo.closeMongoDB();
        mongoUtilMap.remove(mongoName);
    }
}
