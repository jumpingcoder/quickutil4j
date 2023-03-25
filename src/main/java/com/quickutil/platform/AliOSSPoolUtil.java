package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.constants.Symbol;
import com.quickutil.platform.exception.MissingParametersException;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 阿里云对象存储工具-配置池
 * 官方文档参见：https://help.aliyun.com
 *
 * @author 0.5
 */
public class AliOSSPoolUtil {
    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(AliOSSPoolUtil.class);

    private Map<String, AliOSSUtil> ossMap = new HashMap<>();

    public AliOSSPoolUtil() {
    }

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
                String bucketName = properties.getProperty(key + ".bucketname");
                if (accessKeyId == null || accessKeySecret == null || endpoint == null || bucketName == null)
                    throw new MissingParametersException("init requires accessKeyId, accessKeySecret, endpoint, bucketname");
                ossMap.put(key, new AliOSSUtil(accessKeyId, accessKeySecret, endpoint, bucketName));
            } catch (Exception e) {
                LOGGER.error(Symbol.BLANK, e);
            }
        }
    }

    public AliOSSUtil get(String name) {
        return ossMap.get(name);
    }

    public void add(String name, AliOSSUtil alioss) {
        ossMap.put(name, alioss);
    }

    public void remove(String name) {
        ossMap.remove(name);
    }

}
