package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.constants.Symbol;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置文件工具
 *
 * @author 0.5
 */
public class PropertiesUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(PropertiesUtil.class);

    /**
     * 获取资源文件字节流
     */
    public static InputStream getInputStream(String filePath) {
        return new PropertiesUtil().getResourceStream(filePath);
    }

    /**
     * 将资源文件读取为Properties
     */
    public static Properties getProperties(String filePath) {
        return getProperties(getInputStream(filePath));
    }

    /**
     * 将字节流读取为properties
     */
    public static Properties getProperties(InputStream stream) {
        Properties properties = new Properties();
        try {
            properties.load(stream);
            return properties;
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        return null;
    }

    /**
     * 获取非标准aes解密properties，如未加密也可填明文
     */
    public static Properties getPropertiesWithKey(String filePath, String password) {
        Properties properties = getProperties(filePath);
        for (Object pkey : properties.keySet()) {
            String pvalue = properties.getProperty(pkey.toString());
            try {
                if (pvalue.length() > 1) {
                    properties.setProperty(pkey.toString(), CryptoUtil.aesDecryptStr(pvalue, password));
                }
            } catch (Exception e) {
            }
        }
        return properties;
    }

    /**
     * 获取非标准aes解密properties，如未加密也可填明文
     */
    public static Properties getPropertiesWithKey(InputStream stream, String password) {
        Properties properties = getProperties(stream);
        for (Object pkey : properties.keySet()) {
            String pvalue = properties.getProperty(pkey.toString());
            try {
                if (pvalue.length() > 1) {
                    properties.setProperty(pkey.toString(), CryptoUtil.aesDecryptStr(pvalue, password));
                }
            } catch (Exception e) {
            }
        }
        return properties;
    }

    private InputStream getResourceStream(String filePath) {
        InputStream stream = null;
        try {
            stream = this.getClass().getClassLoader().getResourceAsStream(filePath);
        } catch (Exception e) {
            LOGGER.debug(Symbol.BLANK, e);
        }
        if (stream == null) {
            try {
                stream = this.getClass().getResourceAsStream("/resources/" + filePath);
            } catch (Exception e) {
                LOGGER.debug(Symbol.BLANK, e);
            }
        }
        if (stream == null) {
            try {
                stream = this.getClass().getResourceAsStream(filePath);
            } catch (Exception e) {
                LOGGER.debug(Symbol.BLANK, e);
            }
        }
        return stream;
    }
}