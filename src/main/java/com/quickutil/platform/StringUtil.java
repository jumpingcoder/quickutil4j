package com.quickutil.platform;

import java.util.Objects;
import java.util.Random;
import java.util.StringJoiner;

/**
 * @author shijie.ruan
 */
public class StringUtil {
	private static final char[] symbols;

	static {
		StringBuilder tmp = new StringBuilder();
		for (char ch = '0'; ch <= '9'; ++ch)
			tmp.append(ch);
		for (char ch = 'a'; ch <= 'z'; ++ch)
			tmp.append(ch);
		for (char ch = 'A'; ch <= 'Z'; ++ch)
			tmp.append(ch);
		symbols = tmp.toString().toCharArray();
	}

	/**
	 * 把数组构造成字符串
	 * eg: array = {"a","b"}, delimiter = " OR ", prefix = "(" , suffix = ")"
	 * result = (a OR b)
	 * @param array
	 * @param delimiter 分隔符(required)
	 * @param prefix 前缀(optional, default empty)
	 * @param suffix 后缀(optional, default empty)
	 * @return
	 */
	public static String mkString(String[] array, String delimiter, String prefix, String suffix) {
		Objects.requireNonNull(delimiter, "The delimiter must not be null");
		if (null == prefix) prefix = "";
		if (null == suffix) suffix = "";
		StringJoiner sj = new StringJoiner(delimiter, prefix, suffix);
		if (array != null && 0 != array.length) {
			for (String e: array) { sj.add(e); }
			return sj.toString();
		} else {
			System.out.println("empty array");
			return "";
		}
	}

	/**
	 * 把某个字段的多个可匹配值作为或条件子查询
	 * @param array
	 * @return
	 */
	public static String mkORSubQuery(String[] array) {
		for(int i = 0; i < array.length; i++) {
			if (array[i].contains(" "))
				array[i] = "\"" + array[i] + "\"";
//				array[i] = array[i].replace(" ", "\\\\ ");
		}
		return mkString(array, " OR ", "(", ")");
	}

	/**
	 * {a,b}
	 * 用中括号和逗号进行包装, 用于聚合返回的结果中带上多个源字段，或者搜索多个字段
	 * ["a", "b"]
	 * @param array
	 * @return
	 */
	public static String mkStringForMultiFields(String[] array) {
		for(int i = 0; i < array.length; i++) { array[i] = "\"" + array[i] + "\""; }
		return mkString(array, ",", "[", "]");
	}

	/**
	 * 返回一个有字母数字组成的字符串
	 * @param len
	 * @return
	 */
	public static String getRandomString(int len) {
		char[] buf = new char[len];
		Random random = new Random();
		for (int i = 0; i < buf.length; i++) {
			buf[i] = symbols[random.nextInt(symbols.length)];
		}
		return new String(buf);
	}

	/**
	 * 主要用于净化客户端上报的 firmware_sign 取最后两个等号的位置
	 * @param str
	 * @returnHttpStatusCodes.STATUS_CODE_OK
	 */
	public static String correctBase64(String str) {
		String origin = str.trim();
		int lastIndexOfEqual = origin.lastIndexOf("=");
		if (lastIndexOfEqual == -1) {
			System.out.println("error base64: " + origin);
			return null;
		}
		// TODO: 还有4个等号的
		origin = origin.substring(0, lastIndexOfEqual + 1);
		if (origin.length() > 24) {
			if (origin.length() == 26 && origin.endsWith("====")) { // 三个等号结尾脏数据,去除最后两个等号
				return origin.substring(0, origin.length() - 2);
			} else if (origin.length() == 25 && origin.endsWith("===")) {// 三个等号结尾脏数据,去除最后一个等号
				return origin.substring(0, origin.length() - 1);
			} else if (22 == origin.lastIndexOf("==")) { // 双等号后还有别的信息的脏数据,去掉之后的
				return origin.substring(0, 24);
			} else {
				System.out.println("error base64: " + origin);
				return null;
			}
		} else {
			return origin;
		}
	}
}

