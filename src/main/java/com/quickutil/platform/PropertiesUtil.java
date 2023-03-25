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

    public enum EncryptType {
        AESRandom, AESECB, AESCBC
    }

    public enum CodecType {
        HEX, Base64
    }

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(PropertiesUtil.class);

    /**
     * 将资源文件读取为Properties
     */
    public static Properties getResourceProperties(String filePath) {
        return getProperties(FileUtil.resource2stream(filePath));
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

    public static Properties getPropertiesWithKey(String filePath, EncryptType encryptType, CodecType codecType, String password) {
        Properties properties = getResourceProperties(filePath);
        return getPropertiesWithKey(properties, encryptType, codecType, password);
    }

    public static Properties getPropertiesWithKey(InputStream stream, EncryptType encryptType, CodecType codecType, String password) {
        Properties properties = getProperties(stream);
        return getPropertiesWithKey(properties, encryptType, codecType, password);
    }

    public static Properties getPropertiesWithKey(Properties properties, EncryptType encryptType, CodecType codecType, String password) {
        for (Object key : properties.keySet()) {
            String value = properties.getProperty(key.toString());
            try {
                if (value.length() == 0)
                    continue;
                byte[] encrypted = null;
                //decode
                if (codecType == CodecType.HEX) {
                    encrypted = CryptoUtil.hex2byte(value);
                } else {
                    encrypted = CryptoUtil.base64ToByte(value);
                }
                //decrypt
                if (encryptType == EncryptType.AESRandom) {
                    value = new String(CryptoUtil.aesRandomDecrypt(encrypted, password));
                } else if (encryptType == EncryptType.AESECB) {
                    value = new String(CryptoUtil.aesecbDecrypt(encrypted, password));
                } else if (encryptType == EncryptType.AESCBC) {
                    value = new String(CryptoUtil.aescbcDecrypt(encrypted, password, new byte[16]));
                }
                properties.setProperty(key.toString(), value);
            } catch (Exception e) {
            }
        }
        return properties;
    }
}