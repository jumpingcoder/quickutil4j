/**
 * Json工具
 * 
 * @class JsonUtil
 * @author 0.5
 */
package com.quickutil.platform;

import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

public class JsonUtil {

    private static Gson gson = new Gson();

    /**
     * json字符串转字符数组
     * 
     * @param json-json字符串
     * @return
     */
    public static List<String> toListString(String json) {
        return gson.fromJson(json, new TypeToken<List<String>>() {
        }.getType());
    }

    /**
     * json字符串转哈希表
     * 
     * @param json-json字符串
     * @return
     */
    public static Map<String, Object> toMap(String json) {
        return gson.fromJson(json, new TypeToken<Map<String, Object>>() {
        }.getType());
    }

    /**
     * json字符串转哈希表数组
     * 
     * @param json-json字符串
     * @return
     */
    public static List<Map<String, Object>> toList(String json) {
        return gson.fromJson(json, new TypeToken<List<Map<String, Object>>>() {
        }.getType());
    }

    /**
     * json字符串转json对象
     * 
     * @param json-json字符串
     * @return
     */
    public static JsonObject toJsonMap(String json) {
        return gson.toJsonTree(toMap(json)).getAsJsonObject();
    }

    /**
     * json字符串转json数组
     * 
     * @param json-json字符串
     * @return
     */
    public static JsonArray toJsonArray(String json) {
        return gson.toJsonTree(toList(json)).getAsJsonArray();
    }

    /**
     * 对象转json字符串
     * 
     * @param object-对象
     * @return
     */
    public static String toJson(Object object) {
        return gson.toJson(object);
    }

    /**
     * 将json字符串封装为jsonp所需格式
     * 
     * @param object-对象
     * @return
     */
    public static String toJsonP(String callback, String json) {
        if (callback == null)
            return json;
        return callback + "(" + json + ")";
    }
}
