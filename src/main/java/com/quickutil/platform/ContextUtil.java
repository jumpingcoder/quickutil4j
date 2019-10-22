package com.quickutil.platform;

import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Http会话过滤器-容器
 *
 * @author 0.5
 */
public class ContextUtil {

    private static final String iPhone = "iPhone";
    private static final String iPad = "iPad";
    private static final String CFNetwork = "CFNetwork";
    private static final String Wechat = "MicroMessenger";
    public static final String iOS = "iOS";
    public static final String Android = "Android";
    public static final String Web = "web";
    public static final String Unknown = "Unknown";
    private static final String UserAgent = "user-agent";
    private static final String Referer = "Referer";
    private static final String X_Forwarded_For = "X-Forwarded-For";
    private static final String X_Real_IP = "X-Real-IP";

    private static final String localhost = "%s://%s:%s";

    protected transient static ThreadLocal<HttpServletRequest> request = new ThreadLocal<HttpServletRequest>();
    protected transient static ThreadLocal<HttpServletResponse> response = new ThreadLocal<HttpServletResponse>();
    private static final Logger LOGGER = LoggerFactory.getLogger(ContextUtil.class);

    /**
     * 设置request
     *
     * @param httpRequest-会话请求
     */
    public static void setRequest(HttpServletRequest httpRequest) {
        request.set(httpRequest);
    }

    /**
     * 获取request
     */
    public static HttpServletRequest getRequest() {
        return request.get();
    }

    /**
     * 设置response
     *
     * @param httpResponse-会话响应
     */
    public static void setResponse(HttpServletResponse httpResponse) {
        response.set(httpResponse);
    }

    /**
     * 获取response
     *
     * @return
     */
    public static HttpServletResponse getResponse() {
        return response.get();
    }

    /**
     * 获取Header
     *
     * @param key-header的key
     * @return
     */
    public static String getHeader(String key) {
        return request.get().getHeader(key);
    }

    /**
     * 获取请求的IP
     *
     * @return
     */
    public static String getIp() {
        String ip = request.get().getHeader(X_Real_IP);
        if (ip == null)
            ip = request.get().getHeader(X_Forwarded_For);
        if (ip == null)
            ip = request.get().getRemoteAddr();
        if (ip == null)
            ip = "127.0.0.1";
        return ip;
    }

    /**
     * 获取user-agent
     *
     * @return
     */
    public static String getUserAgent() {
        return request.get().getHeader(UserAgent);
    }

    /**
     * 获取Referer
     *
     * @return
     */
    public static String getReferer() {
        return request.get().getHeader(Referer);
    }

    /**
     * 获取COOKIE
     *
     * @param cookieKey-cookie的key
     * @return
     */
    public static String getCookie(String cookieKey) {
        Cookie[] cookies = request.get().getCookies();
        if (cookies == null)
            return null;
        for (Cookie cookie : cookies) {
            if (cookie.getName().equals(cookieKey))
                return cookie.getValue();
        }
        return null;
    }

    /**
     * 获取完整HOST
     *
     * @return
     */
    public static String getHost() {
        return String.format(localhost, request.get().getScheme(), request.get().getServerName(), request.get().getServerPort());
    }

    /**
     * 获取系统类型
     *
     * @return
     */
    public static String getSystemType() {
        String useragent = getUserAgent();
        if (useragent == null)
            return Unknown;
        if (useragent.contains(iPhone) || useragent.contains(iPad) || useragent.contains(CFNetwork))
            return iOS;
        else if (useragent.contains(Android))
            return Android;
        else
            return Web;
    }

    /**
     * 请求源是否是微信
     *
     * @return
     */
    public static boolean isWechat() {
        String useragent = getUserAgent();
        if (useragent == null)
            return false;
        if (useragent.contains(Wechat))
            return true;
        return false;
    }

    public static JsonObject getRequestJson() {
        try {
            return JsonUtil.toJsonMap(FileUtil.stream2string(request.get().getInputStream()));
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        return null;
    }

}
