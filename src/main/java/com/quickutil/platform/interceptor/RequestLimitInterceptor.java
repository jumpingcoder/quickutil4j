package com.quickutil.platform.interceptor;

import com.quickutil.platform.ContextUtil;
import com.quickutil.platform.JsonUtil;
import com.quickutil.platform.entity.HttpTraceLog;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * Http 请求数量拦截器
 *
 * @author 0.5
 */
public class RequestLimitInterceptor implements HandlerInterceptor {

	private static final Logger LOGGER = LoggerFactory.getLogger(RequestLimitInterceptor.class);
	//限流设置
	private int serviceLimit = 200;
	private int pathLimitDefault = 100;
	private String limitTips = "Rate Limit";
	private int serviceUsed = 0;
	private Map<String, Integer> pathLimitMap = new HashMap<>();
	private Map<String, Integer> pathUsedMap = new HashMap<>();
	//日志设置
	private boolean openTraceLog = false;
	private final String HTTP_LOG = "HTTP_LOG";//程序中可以使用detail字段插入自定义内容

	//如果serviceLimit>server.tomcat.threads.max，则永远不会触发拦截器服务限流
	//如果serviceLimit<=server.tomcat.threads.max，则tomcat队列永远不会等待
	public RequestLimitInterceptor(int serviceLimit, int pathLimitDefault, String limitTips, boolean openTraceLog) {
		this.serviceLimit = serviceLimit;
		this.pathLimitDefault = pathLimitDefault;
		this.limitTips = limitTips;
		this.openTraceLog = openTraceLog;
	}

	//设置路由限制
	public RequestLimitInterceptor addPathLimit(String path, int pathLimit) {
		pathLimitMap.put(path, pathLimit);
		return this;
	}

	@Override
	public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
			throws IOException, ServletException {
		//限流设置
		if (!bind(request)) {
			response.setStatus(503);
			response.getOutputStream().println(limitTips);
			return false;
		}
		//日志设置
		if (openTraceLog) {
			HttpTraceLog log = new HttpTraceLog()
					.setHost(request.getHeader(ContextUtil.HOST))
					.setPath(request.getRequestURI())
					.setRequestTime(System.currentTimeMillis())
					.setUserAgent(request.getHeader(ContextUtil.USER_AGENT))
					.setXRequestId(request.getHeader(ContextUtil.X_REQUEST_ID))
					.setXRealIp(request.getHeader(ContextUtil.X_REAL_IP) == null ? request.getRemoteAddr() : request.getHeader(ContextUtil.X_REAL_IP));
			request.setAttribute(HTTP_LOG, log);
		}
		return true;
	}

	@Override
	public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, @Nullable Exception ex) {
		//限流设置
		free(request);
		//日志设置
		if (openTraceLog) {
			HttpTraceLog log = (HttpTraceLog) request.getAttribute(HTTP_LOG);
			log.setCost(System.currentTimeMillis() - log.getRequestTime());
			LOGGER.info(JsonUtil.toJson(log));
		}
	}

	public int getServiceUsed() {
		return serviceUsed;
	}

	public int getPathLimitDefault() {
		return pathLimitDefault;
	}

	public Map<String, Integer> getPathUsed() {
		return pathUsedMap;
	}

	public Map<String, Integer> getPathLimitMap() {
		return pathLimitMap;
	}

	//绑定连接
	private synchronized boolean bind(HttpServletRequest request) {
		//判断全局流量
		if (serviceUsed >= serviceLimit) {
			LOGGER.error("Connection number of service >= " + serviceLimit);
			return false;
		}
		//判断PATH流量
		List<String> pathKeys = getPathKeys(request.getRequestURI());
		for (String pathKey : pathKeys) {
			pathUsedMap.putIfAbsent(pathKey, 0);
			int limit = pathLimitMap.get(pathKey) == null ? pathLimitDefault : pathLimitMap.get(pathKey);
			if (pathUsedMap.get(pathKey) >= limit) {
				LOGGER.error("Connection number of " + pathKey + " >= " + limit);
				return false;
			}
		}
		//避免异步请求重复计数
		if (request.getAttribute("X-Request-Binded") != null) {
			return true;
		}
		request.setAttribute("X-Request-Binded", 1);
		//计数
		serviceUsed++;
		for (String pathKey : pathKeys) {
			pathUsedMap.put(pathKey, pathUsedMap.get(pathKey) + 1);
		}
		return true;
	}

	//释放连接
	private synchronized boolean free(HttpServletRequest request) {
		//避免异步请求重复计数
		if (request.getAttribute("X-Request-Freed") != null) {
			return true;
		}
		request.setAttribute("X-Request-Freed", 1);
		serviceUsed--;
		List<String> pathKeys = getPathKeys(request.getRequestURI());
		for (String pathKey : pathKeys) {
			pathUsedMap.putIfAbsent(pathKey, 1);
			int result = pathUsedMap.get(pathKey) - 1;
			if (result == 0) {
				pathUsedMap.remove(pathKey);
			} else {
				pathUsedMap.put(pathKey, result);
			}
		}
		return true;
	}

	//获得所有的uri配置
	private List<String> getPathKeys(String uri) {
		List<String> pathKeys = new ArrayList<>();
		for (String key : pathLimitMap.keySet()) {
			if (uri.startsWith(key)) {
				pathKeys.add(key);
			}
		}
		if (pathKeys.size() == 0) {
			pathKeys.add(uri);
		}
		return pathKeys;
	}
}
