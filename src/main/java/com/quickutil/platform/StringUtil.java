/**
 * 字符串工具
 * 
 * @class StringUtil
 * @author 0.5
 * @author shijie.ruan
 */
package com.quickutil.platform;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

public class StringUtil {

	public enum ObjectType {
		integer, longer, doubler, string, list, map, other;
	}

	/**
	 * 获取object原始类型
	 * 
	 * @param object-输入的对象
	 * @return
	 */
	public static ObjectType getObjectType(Object object) {
		if (object instanceof Integer) {
			return ObjectType.integer;
		}
		if (object instanceof Long) {
			return ObjectType.longer;
		}
		if (object instanceof Double) {
			return ObjectType.doubler;
		}
		if (object instanceof String) {
			return ObjectType.string;
		}
		if (object instanceof List) {
			return ObjectType.list;
		}
		if (object instanceof Map) {
			return ObjectType.map;
		}
		return ObjectType.other;
	}

	/**
	 * string转数值
	 * 
	 * @param content-输入的字符串
	 * @return
	 */
	public static Object getObjectFromString(String content) {
		ObjectType type = getObjectTypeFromString(content);
		switch (type) {
		case integer:
			return Integer.parseInt(content);
		case longer:
			return Long.parseLong(content);
		case doubler:
			return Double.parseDouble(content);
		default:
			return content;
		}
	}

	/**
	 * 获取string原始类型
	 * 
	 * @param content-输入的字符串
	 * @return
	 */
	public static ObjectType getObjectTypeFromString(String content) {
		if (content.matches("^[-+]?([0-9]+)[.]([0-9]+)$"))
			return ObjectType.doubler;
		if (content.matches("^[-+]?[0-9]+")) {
			long number = Long.parseLong(content);
			if (number < Integer.MAX_VALUE && number > Integer.MIN_VALUE)
				return ObjectType.integer;
			return ObjectType.longer;
		}
		return ObjectType.string;
	}

	/**
	 * object转成map
	 * 
	 * @param object-对象
	 * @return
	 */
	public static Map<String, Object> objectToMap(Object object) {
		Map<String, Object> map = new HashMap<>();
		try {
			for (Field field : object.getClass().getFields()) {
				map.put(field.getName(), field.get(object));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return map;
	}

	/**
	 * 判断是否为字母+数字
	 * 
	 * @param content-输入的字符串
	 * @param minLength-最小长度
	 * @param maxLength-最大长度
	 * @return
	 */
	public static boolean letNumLegal(String content, int minLength, int maxLength) {
		return content.matches("[0-9a-zA-Z]{" + minLength + "," + maxLength + "}");
	}

	/**
	 * 判断是否为数字
	 * 
	 * @param content-输入的字符串
	 * @param minLength-最小长度
	 * @param maxLength-最大长度
	 * @return
	 */
	public static boolean numLegal(String content, int minLength, int maxLength) {
		return content.matches("[0-9]{" + minLength + "," + maxLength + "}");
	}

	/**
	 * 判断是否为用户名格式
	 * 
	 * @param content-输入的字符串
	 * @return
	 */
	public static boolean usernameLegal(String content) {
		return content.matches("[0-9a-zA-Z_]{5,16}");
	}

	/**
	 * 判断是否为密码格式
	 * 
	 * @param content-输入的字符串
	 * @return
	 */
	public static boolean passwordLegal(String content) {
		return content.matches("(?![a-z]+$|[0-9]+$)^[a-zA-Z0-9]{6,16}$");
	}

	/**
	 * 判断是否为昵称格式
	 * 
	 * @param content-输入的字符串
	 * @return
	 */
	public static boolean nickLegal(String content) {
		return content.matches("[a-zA-Z0-9\u4e00-\u9fa5]{3,8}");
	}

	/**
	 * 判断是否为中国大陆手机号
	 * 
	 * @param content-输入的字符串
	 * @return
	 */
	public static boolean mobileLegal(String content) {
		return content.matches("^((13[0-9])|(14[0-9])|(15[0-9])|(17[0-9])|(18[0-9]))\\d{8}$");
	}

	/**
	 * 判断是否为邮箱格式
	 * 
	 * @param content-输入的字符串
	 * @return
	 */
	public static boolean emailLegal(String content) {
		return content.matches("^([a-z0-9A-Z]+[-|\\.]?)+[a-z0-9A-Z]@([a-z0-9A-Z]+(-[a-z0-9A-Z]+)?\\.)+[a-zA-Z]{2,}$");
	}

	/**
	 * 把数组构造成字符串 eg: array = {"a","b"}, delimiter = " OR ", prefix = "(" , suffix = ")" result = (a OR b)
	 * 
	 * @param array-字符串数组
	 * @param delimiter-分隔符(required)
	 * @param prefix-前缀(optional,default empty)
	 * @param suffix-后缀(optional,default empty)
	 * @return
	 */
	public static String joinString(String[] array, String delimiter, String prefix, String suffix) {
		Objects.requireNonNull(delimiter, "The delimiter must not be null");
		if (null == prefix)
			prefix = "";
		if (null == suffix)
			suffix = "";
		StringJoiner sj = new StringJoiner(delimiter, prefix, suffix);
		if (array != null && 0 != array.length) {
			for (String e : array) {
				sj.add(e);
			}
			return sj.toString();
		}
		return null;
	}

}
