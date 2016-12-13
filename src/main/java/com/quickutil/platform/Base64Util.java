/**
 * Base64
 * 
 * @class Base64Util
 * @author 0.5
 */
package com.quickutil.platform;

import java.util.Base64;

public class Base64Util {

    /**
     * base64字符串转换为byte[]
     * 
     * @param baseString-base64字符串
     * @return
     */
    public static byte[] base64ToByte(String baseString) {
        try {
            return Base64.getDecoder().decode(baseString);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * byte[]转换为base64字符串
     * 
     * @param bt-字节数组
     * @return
     */
    public static String byteToBase64(byte[] bt) {
        try {
            return Base64.getEncoder().encodeToString(bt);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
