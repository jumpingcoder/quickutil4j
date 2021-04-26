package com.quickutil.platform;

import com.quickutil.platform.constants.Symbol;
import java.net.URI;
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
	public static final String HOST = "Host";
	public static final String USER_AGENT = "User-Agent";
	private static final String REFERER = "Referer";
	private static final String X_Forwarded_Proto = "X-Forwarded-Proto";
	private static final String X_Forwarded_Host = "X-Forwarded-Host";
	private static final String X_Forwarded_For = "X-Forwarded-For";
	public static final String X_REAL_IP = "X-Real-IP";
	public static final String X_REQUEST_ID = "X-Request-ID";

	private static final Logger LOGGER = LoggerFactory.getLogger(ContextUtil.class);
	protected transient static ThreadLocal<HttpServletRequest> request = new ThreadLocal<>();
	protected transient static ThreadLocal<HttpServletResponse> response = new ThreadLocal<>();

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
	 * 获取Header
	 */
	public static String getHeader(String key) {
		return request.get().getHeader(key);
	}

	/**
	 * 获取user-agent
	 */
	public static String getUserAgent() {
		return request.get().getHeader(USER_AGENT);
	}

	/**
	 * 获取Referer
	 */
	public static String getReferer() {
		return request.get().getHeader(REFERER);
	}

	/**
	 * 获取Cookie
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
	 * 获取请求的IP，获取顺序X-Real-IP,X-Forwarded-For第一段,直连机器的ip
	 */
	public static String getRealIp() {
		String ip = request.get().getHeader(X_REAL_IP);
		if (ip != null) {
			return ip;
		}
		ip = request.get().getHeader(X_Forwarded_For);
		if (ip != null) {
			ip = ip.split(Symbol.COMMA)[0];
			return ip;
		}
		return request.get().getRemoteAddr();

	}

	/**
	 * 获取完整HOST，错误的方法，请使用getOriginalUrlWithoutPath
	 */
	@Deprecated
	public static String getHost() {
		return String.format("%s://%s:%s", request.get().getScheme(), request.get().getServerName(), request.get().getServerPort());
	}

	/**
	 * 获取原始URI
	 */
	public static URI getOriginalUri() {
		try {
			return new URI(getOriginalUrl());
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return null;
	}

	/**
	 * 获取原始url
	 */
	public static String getOriginalUrl() {
		String url = ContextUtil.getRequest().getRequestURL().toString();
		String proto = ContextUtil.getRequest().getHeader(X_Forwarded_Proto);
		String host = ContextUtil.getRequest().getHeader(X_Forwarded_Host);
		return getOriginalUrl(url, proto, host);
	}

	/**
	 * 获取原始url的一部分，https://quickutil.com/hello?xx=123#part1 -> https://quickutil.com
	 */
	public static String getOriginalUrlWithoutPath() {
		URI uri = getOriginalUri();
		return uri.getScheme() + "://" + uri.getAuthority();
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

	public static String getOriginalUrl(String url, String originalSchema, String originalHost) {
		try {
			StringBuilder sb = new StringBuilder();
			URI uri = new URI(url);
			originalSchema = (originalSchema == null) ? uri.getScheme() : originalSchema;
			sb.append(originalSchema);
			sb.append("://");
			if (originalHost == null) {
				sb.append(uri.getAuthority());
			} else {
				sb.append(uri.getAuthority().replaceAll(uri.getHost(), originalHost));
			}
			if (uri.getPath() != null) {
				sb.append(uri.getPath());
			}
			if (uri.getQuery() != null) {
				sb.append("?" + uri.getQuery());
			}
			if (uri.getFragment() != null) {
				sb.append("#" + uri.getFragment());
			}
			return sb.toString();
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		return url;
	}

}
