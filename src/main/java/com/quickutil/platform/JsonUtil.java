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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class JsonUtil {

	private static Gson gson = new GsonBuilder().serializeNulls().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

	public static void init(GsonBuilder builder) {
		gson = builder.create();
	}

	/**
	 * json字符串转List<String>
	 */
	public static List<String> toListString(String json) {
		return gson.fromJson(json, new TypeToken<List<String>>() {
		}.getType());
	}

	/**
	 * json字符串转Map
	 */
	public static Map<String, Object> toMap(String json) {
		return gson.fromJson(json, new TypeToken<Map<String, Object>>() {
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
	public static JsonObject toJsonObject(String json) {
		return gson.fromJson(json, JsonObject.class);
	}

	/**
	 * json字符串转json数组
	 */
	public static JsonArray toJsonArray(String json) {
		return gson.fromJson(json, JsonArray.class);
	}

	/**
	 * json字符串转class对象
	 */
	public static <T>T toObject(String json,Class<T> clazz) {
		return gson.fromJson(json, clazz);
	}

	/**
	 * json字符串转class数组
	 */
	public static <T> List<T> toObjectList(String json,Class<T> clazz) {
		return gson.fromJson(json, new ParameterizedTypeImpl(clazz));
	}

	private static class ParameterizedTypeImpl implements ParameterizedType {
		Class<?> clazz;

		public ParameterizedTypeImpl(Class<?> clazz) {
			this.clazz = clazz;
		}

		@Override
		public Type[] getActualTypeArguments() {
			return new Type[]{clazz};
		}

		@Override
		public Type getRawType() {
			return List.class;
		}

		@Override
		public Type getOwnerType() {
			return null;
		}
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

}
