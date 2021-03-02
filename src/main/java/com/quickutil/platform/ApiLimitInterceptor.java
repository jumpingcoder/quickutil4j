package com.quickutil.platform;

import com.quickutil.platform.constants.Symbol;
import com.quickutil.platform.entity.HttpTraceLog;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * Http 请求数量过滤器
 *
 * @author 0.5
 */
public class ApiLimitInterceptor implements HandlerInterceptor {

	private boolean openTraceLog = false;
	private int serviceLimit = 200;
	private String limitTips = "Rate Limit";
	private int serviceUsed = 0;
	private int pathLimitDefault = 100;
	private Map<String, Integer> pathLimitMap = new HashMap<>();
	private Map<String, Integer> pathUsedMap = new HashMap<>();
	private static final Logger LOGGER = LoggerFactory.getLogger(ApiLimitInterceptor.class);
	private static final String HTTP_LOG = "HTTP_LOG";//程序中可以使用detail字段插入自定义内容

	public ApiLimitInterceptor(boolean openTraceLog, int serviceLimit, String limitTips) {
		this.openTraceLog = openTraceLog;
		this.serviceLimit = serviceLimit;
		this.limitTips = limitTips;
	}

	public ApiLimitInterceptor setPathLimit(String path, int pathLimit) {
		pathLimitMap.put(path, pathLimit);
		return this;
	}

	@Override
	public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
			throws IOException, ServletException {
		int limit = pathLimitMap.get(request.getRequestURI()) == null ? pathLimitDefault : pathLimitMap.get(request.getRequestURI());
		if (!bind(request, limit)) {
			LOGGER.error(request.getRequestURI() + Symbol.GREATER + limit);
			response.setStatus(503);
			response.getOutputStream().println(limitTips);
			return false;
		}
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
		free(request);
		if (openTraceLog) {
			HttpTraceLog log = (HttpTraceLog) request.getAttribute(HTTP_LOG);
			log.setCost(System.currentTimeMillis() - log.getRequestTime());
			LOGGER.info(JsonUtil.toJson(log));
		}
	}

	private synchronized boolean bind(HttpServletRequest request, int limit) {
		//判断全局流量
		if (serviceUsed >= serviceLimit) {
			return false;
		}
		//判断PATH流量
		pathUsedMap.putIfAbsent(request.getRequestURI(), 0);
		if (pathUsedMap.get(request.getRequestURI()) >= limit) {
			return false;
		}
		//计数
		serviceUsed++;
		pathUsedMap.put(request.getRequestURI(), pathUsedMap.get(request.getRequestURI()) + 1);
		return true;
	}

	private synchronized boolean free(HttpServletRequest request) {
		if (serviceUsed <= 0) {
			return false;
		}
		if (pathUsedMap.get(request.getRequestURI()) == null) {
			return false;
		}
		if (pathUsedMap.get(request.getRequestURI()) <= 0) {
			return false;
		}
		serviceUsed--;
		pathUsedMap.put(request.getRequestURI(), pathUsedMap.get(request.getRequestURI()) - 1);
		return true;
	}

}
