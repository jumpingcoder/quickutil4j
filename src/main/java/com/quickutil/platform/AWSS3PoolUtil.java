package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.constants.Symbol;
import com.quickutil.platform.exception.MissingParametersException;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * AWS对象存储工具-配置池 官方文档参见：https://aws.amazon.com/cn/sdk-for-java/
 *
 * @author 0.5
 */
public class AWSS3PoolUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(AWSS3PoolUtil.class);

    private Map<String, AWSS3Util> s3Map = new HashMap<>();

    public AWSS3PoolUtil() {
    }

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
                String bucketName = s3Properties.getProperty(key + ".bucketname");
                if (accessKey == null || secretKey == null || endPoint == null || region == null || bucketName == null)
                    throw new MissingParametersException("init requires accessKey, secretKey, endPoint, region, bucketname");
                s3Map.put(key, new AWSS3Util(accessKey, secretKey, endPoint, region, bucketName));
            } catch (Exception e) {
                LOGGER.error(Symbol.BLANK, e);
            }
        }
    }

    public AWSS3Util get(String name) {
        return s3Map.get(name);
    }

    public void add(String name, AWSS3Util awsS3) {
        s3Map.put(name, awsS3);
    }

    public void remove(String name) {
        s3Map.remove(name);
    }

}
