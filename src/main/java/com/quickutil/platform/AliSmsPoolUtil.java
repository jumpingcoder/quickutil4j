package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.constants.Symbol;
import com.quickutil.platform.exception.MissingParametersException;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 阿里云大于发送短信工具-配置池
 * 官方文档参见：https://help.aliyun.com
 *
 * @author 0.5
 */
public class AliSmsPoolUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(AliSmsPoolUtil.class);

    private Map<String, AliSmsUtil> smsMap = new HashMap<>();

    public AliSmsPoolUtil() {
    }

    public AliSmsPoolUtil(Properties smsProperties) {
        Enumeration<?> keys = smsProperties.propertyNames();
        Set<String> keyList = new HashSet<String>();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            key = key.split("\\.")[0];
            keyList.add(key);
        }
        for (String key : keyList) {
            try {
                String accessKey = smsProperties.getProperty(key + ".accessKey");
                String accessSecret = smsProperties.getProperty(key + ".accessSecret");
                String templateCode = smsProperties.getProperty(key + ".templateCode");
                String signName = smsProperties.getProperty(key + ".signName");
                if (accessKey == null || accessSecret == null || templateCode == null || signName == null)
                    throw new MissingParametersException("init requires accessKey, accessSecret, templateCode, signName");
                signName = new String(signName.getBytes("ISO-8859-1"), "utf-8");
                smsMap.put(key, new AliSmsUtil(accessKey, accessSecret, templateCode, signName));
            } catch (Exception e) {
                LOGGER.error(Symbol.BLANK, e);
            }
        }

    }

    public AliSmsUtil get(String name) {
        return smsMap.get(name);
    }

    public void add(String name, AliSmsUtil aliSms) {
        smsMap.put(name, aliSms);
    }

    public void remove(String name) {
        smsMap.remove(name);
    }
}
