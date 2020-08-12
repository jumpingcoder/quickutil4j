/**
 * Json工具
 *
 * @class JsonUtil
 * @author 0.5
 */
package com.quickutil.platform;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import java.util.List;
import java.util.Map;

public class JsonUtil {

	private static Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().serializeNulls().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

	/**
	 * json字符串转Map
	 */
	public static Map<String, Object> toMap(String json) {
		return gson.fromJson(json, new TypeToken<Map<String, Object>>() {
		}.getType());
	}

	/**
	 * json字符串转List<String>
	 */
	public static List<String> toListString(String json) {
		return gson.fromJson(json, new TypeToken<List<String>>() {
		}.getType());
	}

	/**
	 * json字符串转List<Map<String,Object>>
	 */
	public static List<Map<String, Object>> toList(String json) {
		return gson.fromJson(json, new TypeToken<List<Map<String, Object>>>() {
		}.getType());
	}

	/**
	 * json字符串转json对象
	 */
	public static JsonObject toJsonMap(String json) {
		return gson.toJsonTree(toMap(json)).getAsJsonObject();
	}

	/**
	 * json字符串转json数组
	 */
	public static JsonArray toJsonArray(String json) {
		return gson.toJsonTree(toList(json)).getAsJsonArray();
	}

	/**
	 * 对象转json字符串
	 */
	public static String toJson(Object object) {
		return gson.toJson(object);
	}

	/**
	 * 对象转json字符串
	 */
	public static String toJsonWithFormat(Object object, GsonBuilder builder) {
		return builder.create().toJson(object);
	}

	/**
	 * 将json字符串封装为jsonp所需格式
	 */
	public static String toJsonP(String callback, String json) {
		if (callback == null) {
			return json;
		}
		return callback + "(" + json + ")";
	}

	public void init(GsonBuilder builder) {
		gson = builder.create();
	}
}
