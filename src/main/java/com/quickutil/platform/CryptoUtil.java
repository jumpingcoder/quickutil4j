package com.quickutil.platform;

import com.quickutil.platform.constants.Symbol;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Random;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;

/**
 * 加解密工具
 *
 * @author 0.5
 */
public class CryptoUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(CryptoUtil.class);

    private static final String UTF_8 = "UTF-8";
    private static final String MD5 = "MD5";
    private static Random random = new Random();

    /**
     * 获取随机md5
     *
     * @return 随机的md5字符串
     */
    public static String randomMd5Code() {
        return md5Encode((System.currentTimeMillis() + random.nextDouble() + "").getBytes());
    }

    /**
     * 获取随机sha1
     *
     * @return 随机的sha1字符串
     */
    public static String randomSha1Code() {
        return sha1Encode((System.currentTimeMillis() + random.nextDouble() + "").getBytes());
    }

    /**
     * 计算md5
     *
     * @return 计算过md5的字符串
     */
    public static String md5Encode(byte[] bt) {
        try {
            return byte2hex(MessageDigest.getInstance("MD5").digest(bt));
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
            return null;
        }
    }

    /**
     * 计算sha1
     *
     * @return 计算过sha1的字符串
     */
    public static String sha1Encode(byte[] bt) {
        try {
            return byte2hex(MessageDigest.getInstance("SHA-1").digest(bt));
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error(Symbol.BLANK, e);
            return null;
        }
    }

    /**
     * hmac-md5签名
     *
     * @return 计算过HMAC-md5的字符串
     */
    public static byte[] HmacMD5Encrypt(byte[] encryptContent, String encryptKey) {
        try {
            byte[] data = encryptKey.getBytes(UTF_8);
            SecretKey secretKey = new SecretKeySpec(data, "HmacMD5");
            Mac mac = Mac.getInstance("HmacMD5");
            mac.init(secretKey);
            return mac.doFinal(encryptContent);
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
            return null;
        }
    }

    /**
     * hmac-sha1签名
     *
     * @return 计算过HMAC-sha1的字符串
     */
    public static byte[] HmacSHA1Encrypt(byte[] encryptContent, String encryptKey) {
        try {
            byte[] data = encryptKey.getBytes(UTF_8);
            SecretKey secretKey = new SecretKeySpec(data, "HmacSHA1");
            Mac mac = Mac.getInstance("HmacSHA1");
            mac.init(secretKey);
            return mac.doFinal(encryptContent);
        } catch (Exception e) {
            LOGGER.error("", e);
            return null;
        }
    }

    /**
     * hmac-sha1签名
     *
     * @return 计算过HMAC-sha256的字符串
     */
    public static String HmacSHA256Encrypt(byte[] encryptContent, String encryptKey) {
        try {
            byte[] data = encryptKey.getBytes(UTF_8);
            SecretKey secretKey = new SecretKeySpec(data, "HmacSHA256");
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(secretKey);
            byte[] secretByte = mac.doFinal(encryptContent);
            return byte2hex(secretByte);
        } catch (Exception e) {
            LOGGER.error("", e);
            return null;
        }
    }

    /**
     * byte[]转16进制字符串
     *
     * @return 字节流序列化成16进制字符串表示的字符串
     */
    public static String byte2hex(byte[] bt) {
        if (bt == null) {
            return null;
        }
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < bt.length; ++i) {
            int x = bt[i] & 0xFF, h = x >>> 4, l = x & 0x0F;
            sb.append((char) (h + ((h < 10) ? '0' : 'a' - 10)));
            sb.append((char) (l + ((l < 10) ? '0' : 'a' - 10)));
        }
        return sb.toString();
    }

    /**
     * 16进制字符串转byte[]
     *
     * @return 16进制字符串表示的字符串反序列化成字节流
     */
    public static byte[] hex2byte(String hexString) {
        if (hexString == null || hexString.equals("")) {
            return null;
        }
        hexString = hexString.toUpperCase();
        int length = hexString.length() / 2;
        char[] hexChars = hexString.toCharArray();
        byte[] bt = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            bt[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return bt;
    }

    private static byte charToByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }

    /**
     * base64字符串转换为byte[]
     *
     * @return 字节流
     */
    public static byte[] base64ToByte(String baseString) {
        try {
            return Base64.getDecoder().decode(baseString);
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        }
        return null;
    }

    /**
     * byte[]转换为base64字符串
     *
     * @return 字符串
     */
    public static String byteToBase64(byte[] bt) {
        try {
            return Base64.getEncoder().encodeToString(bt);
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        }
        return null;
    }

    /**
     * aes加密
     *
     * @return aes加密后的字节流
     */
    public static byte[] aesRandomEncrypt(byte[] content, String password) {
        try {
            KeyGenerator kgen = KeyGenerator.getInstance("AES");
            SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
            random.setSeed(password.getBytes());
            kgen.init(128, random);
            SecretKey secretKey = kgen.generateKey();
            byte[] enCodeFormat = secretKey.getEncoded();
            SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");
            Cipher cipher = Cipher.getInstance("AES");// 创建密码器
            cipher.init(Cipher.ENCRYPT_MODE, key);// 初始化
            return cipher.doFinal(content); // 加密
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        }
        return null;
    }

    /**
     * aes解密
     *
     * @return aes解密后的字节流
     */
    public static byte[] aesRandomDecrypt(byte[] content, String password) throws Exception {
        KeyGenerator kgen = KeyGenerator.getInstance("AES");
        SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
        random.setSeed(password.getBytes());
        kgen.init(128, random);
        SecretKey secretKey = kgen.generateKey();
        byte[] enCodeFormat = secretKey.getEncoded();
        SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");
        Cipher cipher = Cipher.getInstance("AES");// 创建密码器
        cipher.init(Cipher.DECRYPT_MODE, key);// 初始化
        return cipher.doFinal(content); // 解密
    }

    /**
     * aes-ecb加密
     *
     * @return aes-ebc加密后的字节流
     */
    public static byte[] aesecbEncrypt(byte[] content, String password) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");// 创建密码器
            SecretKeySpec key = new SecretKeySpec(password.getBytes(), "AES");
            cipher.init(Cipher.ENCRYPT_MODE, key);// 初始化
            return cipher.doFinal(content); // 加密
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        }
        return null;
    }

    /**
     * aes-ecb解密
     *
     * @return aes-ebc解密后的字节流
     */
    public static byte[] aesecbDecrypt(byte[] content, String password) {
        try {
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");// 创建密码器
            SecretKeySpec key = new SecretKeySpec(password.getBytes(), "AES");
            cipher.init(Cipher.DECRYPT_MODE, key);// 初始化
            return cipher.doFinal(content); // 解密
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        }
        return null;
    }

    /**
     * aes-cbc加密
     *
     * @return aes-cbc加密后的字节流
     */
    public static byte[] aescbcEncrypt(byte[] content, String password, byte[] iv) {
        try {
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");// 创建密码器
            SecretKeySpec key = new SecretKeySpec(password.getBytes(), "AES");
            cipher.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
            return cipher.doFinal(content); // 加密
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        }
        return null;
    }

    /**
     * 标准aes-cbc解密
     *
     * @return aes-cbc解密后的字节流
     */
    public static byte[] aescbcDecrypt(byte[] content, String password, byte[] iv) {
        try {
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");// 创建密码器
            SecretKeySpec key = new SecretKeySpec(password.getBytes(), "AES");
            cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv));// 初始化
            return cipher.doFinal(content); // 解密
        } catch (Exception e) {
            LOGGER.error(Symbol.BLANK, e);
        }
        return null;
    }

}
