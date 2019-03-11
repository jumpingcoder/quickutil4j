package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class AWSS3PoolUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(AWSS3PoolUtil.class);

    private static Map<String, AWSS3Util> s3Map = new HashMap<>();

    public AWSS3PoolUtil(Properties s3Properties) {
        Enumeration<?> keys = s3Properties.propertyNames();
        Set<String> keyList = new HashSet<String>();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            key = key.split("\\.")[0];
            keyList.add(key);
        }
        for (String key : keyList) {
            try {
                String accessKey = s3Properties.getProperty(key + ".accessKey");
                String secretKey = s3Properties.getProperty(key + ".secretKey");
                String endPoint = s3Properties.getProperty(key + ".endPoint");
                String region = s3Properties.getProperty(key + ".region");
                String bucketname = s3Properties.getProperty(key + ".bucketname");
                s3Map.put(key, new AWSS3Util(accessKey, secretKey, endPoint, region, bucketname));
            } catch (Exception e) {
                LOGGER.error("", e);
            }
        }
    }

    public AWSS3Util get(String key) {
        return s3Map.get(key);
    }
}
