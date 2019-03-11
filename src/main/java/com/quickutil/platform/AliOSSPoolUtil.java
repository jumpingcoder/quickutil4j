package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class AliOSSPoolUtil {
    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(AliOSSPoolUtil.class);

    private static Map<String, AliOSSUtil> ossMap = new HashMap<>();

    public AliOSSPoolUtil(Properties properties) {
        Enumeration<?> keys = properties.propertyNames();
        Set<String> keyList = new HashSet<String>();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            key = key.split("\\.")[0];
            keyList.add(key);
        }
        for (String key : keyList) {
            try {
                String accessKeyId = properties.getProperty(key + ".accessKeyId");
                String accessKeySecret = properties.getProperty(key + ".accessKeySecret");
                String endpoint = properties.getProperty(key + ".endpoint");
                String bucketname = properties.getProperty(key + ".bucketname");
                ossMap.put(key, new AliOSSUtil(accessKeyId, accessKeySecret, endpoint, bucketname));
            } catch (Exception e) {
                LOGGER.error("", e);
            }
        }
    }

    public AliOSSUtil get(String key) {
        return ossMap.get(key);
    }
}
