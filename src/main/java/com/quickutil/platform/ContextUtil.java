package com.quickutil.platform;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.InputStream;
import java.util.Map;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Http会话过滤器-容器
 *
 * @author 0.5
 */
public class ContextUtil {

	public static final String iOS = "iOS";
	public static final String Android = "Android";
	public static final String Web = "web";
	public static final String Unknown = "Unknown";
	private static final String iPhone = "iPhone";
	private static final String iPad = "iPad";
	private static final String CFNetwork = "CFNetwork";
	private static final String Wechat = "MicroMessenger";
	private static final String UserAgent = "user-agent";
	private static final String Referer = "Referer";
	private static final String X_Real_IP = "X-Real-IP";
	private static final String X_Forwarded_For = "X-Forwarded-For";
	private static final String X_Forwarded_Proto = "X-Forwarded-Proto";

	private static final Logger LOGGER = LoggerFactory.getLogger(ContextUtil.class);
	protected transient static ThreadLocal<HttpServletRequest> request = new ThreadLocal<>();
	protected transient static ThreadLocal<HttpServletResponse> response = new ThreadLocal<>();
	protected transient static ThreadLocal<byte[]> stream = new ThreadLocal<>();

	/**
	 * 获取request
	 */
	public static HttpServletRequest getRequest() {
		return request.get();
	}

	/**
	 * 设置request
	 */
	public static void setRequest(HttpServletRequest httpRequest) {
		request.set(httpRequest);
	}

	/**
	 * 获取response
	 */
	public static HttpServletResponse getResponse() {
		return response.get();
	}

	/**
	 * 设置response
	 */
	public static void setResponse(HttpServletResponse httpResponse) {
		response.set(httpResponse);
	}

	/**
	 * 获取请求的IP，获取顺序X-Real-IP,X-Forwarded-For第一段,直连机器的ip
	 */
	public static String getIp() {
		String ip = request.get().getHeader(X_Real_IP);
		if (ip == null) {
			ip = request.get().getHeader(X_Forwarded_For);
			if (ip != null) {
				ip = ip.split(",")[0];
				return ip;
			}
		}
		return request.get().getRemoteAddr();
	}

	/**
	 * 获取Header
	 */
	public static String getHeader(String key) {
		return request.get().getHeader(key);
	}

	/**
	 * 获取user-agent
	 */
	public static String getUserAgent() {
		return request.get().getHeader(UserAgent);
	}

	/**
	 * 获取Referer
	 */
	public static String getReferer() {
		return request.get().getHeader(Referer);
	}

	/**
	 * 获取COOKIE
	 */
	public static String getCookie(String cookieKey) {
		Cookie[] cookies = request.get().getCookies();
		if (cookies == null) {
			return null;
		}
		for (Cookie cookie : cookies) {
			if (cookie.getName().equals(cookieKey)) {
				return cookie.getValue();
			}
		}
		return null;
	}

	/**
	 * 获取完整HOST，错误的方法，建议使用getUrlWithoutPath
	 */
	@Deprecated
	public static String getHost() {
		return String.format("%s://%s:%s", request.get().getScheme(), request.get().getServerName(), request.get().getServerPort());
	}

	/**
	 * 获取url的一部分，并根据X-Forwarded-Proto替换schema https://quickutil.com/hello?xx=123
	 */
	public static String getUrl() {
		String url = ContextUtil.getRequest().getRequestURL().toString();
		//重新组装Schema
		String proto = ContextUtil.getRequest().getHeader(X_Forwarded_Proto);
		if (proto != null) {
			int start = url.indexOf(":");
			url = proto + url.substring(start);
		}
		//重新组装参数
		String query = ContextUtil.getRequest().getQueryString();
		if (query != null) {
			url = url + "?" + query;
		}
		return url;
	}

	/**
	 * 获取url的一部分，并根据X-Forwarded-Proto替换schema https://quickutil.com/hello?xx=123 -> https://quickutil.com/hello
	 */
	public static String getUrlWithoutQuery() {
		String url = ContextUtil.getRequest().getRequestURL().toString();
		//重新组装Schema
		String proto = ContextUtil.getRequest().getHeader(X_Forwarded_Proto);
		if (proto != null) {
			int start = url.indexOf(":");
			url = proto + url.substring(start);
		}
		return url;
	}

	/**
	 * 获取url的一部分，并根据X-Forwarded-Proto替换schema https://quickutil.com/hello?xx=123 -> https://quickutil.com
	 */
	public static String getUrlWithoutPath() {
		String url = getUrlWithoutQuery();
		if (ContextUtil.getRequest().getRequestURI().equals("/")) {
			return url.substring(0, url.length() - 1);
		} else if (ContextUtil.getRequest().getRequestURI().equals("//")) {
			return url.substring(0, url.length() - 2);
		}
		return url.replace(ContextUtil.getRequest().getRequestURI(), "");
	}

	/**
	 * 是否移动设备
	 */
	public static boolean isMobileDevice() {
		String useragent = getUserAgent();
		if (useragent == null) {
			return false;
		}
		if (useragent.contains(iPhone) || useragent.contains(iPad) || useragent.contains(CFNetwork) || useragent.contains(Android)) {
			return true;
		}
		return false;
	}

	/**
	 * 获取系统类型
	 */
	public static String getSystemType() {
		String useragent = getUserAgent();
		if (useragent == null) {
			return Unknown;
		}
		if (useragent.contains(iPhone) || useragent.contains(iPad) || useragent.contains(CFNetwork)) {
			return iOS;
		} else if (useragent.contains(Android)) {
			return Android;
		} else {
			return Web;
		}
	}

	/**
	 * 请求源是否是微信
	 */
	public static boolean isWechat() {
		String useragent = getUserAgent();
		if (useragent == null) {
			return false;
		}
		if (useragent.contains(Wechat)) {
			return true;
		}
		return false;
	}

	/**
	 * 以inputstream的方式获取body
	 */
	public static InputStream getRequestStream() {
		try {
			if (ContextUtil.getHeader("content-type").equals("application/x-www-form-urlencoded")) {
				return FileUtil.string2stream(buildQueryString(request.get().getParameterMap()));
			}
			return request.get().getInputStream();
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return null;
	}

	/**
	 * 以String的方式获取body
	 */
	public static String getRequestString() {
		try {
			if (ContextUtil.getHeader("content-type").equals("application/x-www-form-urlencoded")) {
				return buildQueryString(request.get().getParameterMap());
			}
			return FileUtil.stream2string(request.get().getInputStream());
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return null;
	}

	private static String buildQueryString(Map<String, String[]> map) {
		StringBuilder sb = new StringBuilder();
		for (String key : map.keySet()) {
			for (String value : map.get(key)) {
				sb.append(key).append("=").append(value).append("&");
			}
		}
		if (sb.length() > 0) {
			return sb.deleteCharAt(sb.length() - 1).toString();
		}
		return "";
	}

	/**
	 * 当请求的content-type是application/json，且请求结构是{}时
	 */
	public static JsonObject getRequestJsonObject() {
		try {
			String body = null;
			if (ContextUtil.getHeader("content-type").equals("application/x-www-form-urlencoded")) {
				body = JsonUtil.toJson(request.get().getParameterMap());
			} else {
				body = FileUtil.stream2string(request.get().getInputStream());
			}
			return JsonUtil.toJsonMap(body);
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return null;
	}

	/**
	 * 当请求的content-type是application/json，且请求结构是[]时
	 */
	public static JsonArray getRequestJsonArray() {
		try {
			if (ContextUtil.getHeader("content-type").equals("application/x-www-form-urlencoded")) {
				LOGGER.warn("Only support application/json");
				return null;
			}
			return JsonUtil.toJsonArray(FileUtil.stream2string(request.get().getInputStream()));
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return null;
	}

	public static <T> T getRequestObject(Class<T> tClass) {
		try {
			String body = null;
			if (ContextUtil.getHeader("content-type").equals("application/x-www-form-urlencoded")) {
				body = JsonUtil.toJson(request.get().getParameterMap());
			} else {
				body = FileUtil.stream2string(request.get().getInputStream());
			}
			return new Gson().fromJson(body, tClass);
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return null;
	}
}
