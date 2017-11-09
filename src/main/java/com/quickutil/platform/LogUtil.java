/**
 * Log工具
 * 
 * @class LogUtil
 * @author 0.5
 */
package com.quickutil.platform;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogUtil {

	private static final String time = "Time";
	private static final DateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final String level = "Level";
	private static final String error = "Error";
	private static final String warn = "Warn";
	private static final String info = "Info";
	private static final String exception = "Exception";
	private static final String stack = "Stack";
	private static final String invoke = "invoke";
	private static final String content = "Content";

	/**
	 * error级别的日志
	 * 
	 * @param e-异常对象
	 * @param object-附加的内容对象
	 */
	public static void error(Exception e, Object object) {
		Map<String, Object> result = new HashMap<String, Object>();
		result.put(level, error);
		result.put(time, datetimeFormat.format(new Date()));
		result.put(exception, e.toString());
		result.put(content, object);
		List<StackTraceElement> stackList = new ArrayList<StackTraceElement>();
		for (StackTraceElement stackE : e.getStackTrace()) {
			if (!stackE.getMethodName().equals(invoke))
				stackList.add(stackE);
			break;
		}
		result.put(stack, stackList);
		System.out.println(JsonUtil.toJson(result));
	}

	/**
	 * warn级别的日志
	 * 
	 * @param e-异常对象
	 * @param object-附加的内容对象
	 */
	public static void warn(Exception e, Object object) {
		Map<String, Object> result = new HashMap<String, Object>();
		result.put(level, warn);
		result.put(time, datetimeFormat.format(new Date()));
		result.put(stack, e);
		result.put(content, object);
		List<StackTraceElement> stackList = new ArrayList<StackTraceElement>();
		for (StackTraceElement stackE : e.getStackTrace()) {
			if (!stackE.getMethodName().equals(invoke))
				stackList.add(stackE);
			break;
		}
		result.put(stack, stackList);
		System.out.println(JsonUtil.toJson(result));
	}

	/**
	 * info级别的日志
	 * 
	 * @param object-输出的内容对象
	 */
	public static void info(Object object) {
		Map<String, Object> result = new HashMap<String, Object>();
		result.put(level, info);
		result.put(time, datetimeFormat.format(new Date()));
		result.put(content, object);
		System.out.println(JsonUtil.toJson(result));
	}

}
