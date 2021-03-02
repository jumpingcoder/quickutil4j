package com.quickutil.platform.entity;

import java.util.HashMap;
import java.util.Map;

public class HttpTraceLog {

	private String host;
	private String path;
	private String userAgent;
	private String xRequestId;
	private String xRealIp;
	private long requestTime;
	private long cost;
	private Map<String, Object> detail = new HashMap<>();

	public String getHost() {
		return host;
	}

	public HttpTraceLog setHost(String host) {
		this.host = host;
		return this;
	}

	public String getPath() {
		return path;
	}

	public HttpTraceLog setPath(String path) {
		this.path = path;
		return this;
	}

	public String getUserAgent() {
		return userAgent;
	}

	public HttpTraceLog setUserAgent(String userAgent) {
		this.userAgent = userAgent;
		return this;
	}

	public String getXRequestId() {
		return xRequestId;
	}

	public HttpTraceLog setXRequestId(String xRequestId) {
		this.xRequestId = xRequestId;
		return this;
	}

	public String getXRealIp() {
		return xRealIp;
	}

	public HttpTraceLog setXRealIp(String xRealIp) {
		this.xRealIp = xRealIp;
		return this;
	}

	public long getRequestTime() {
		return requestTime;
	}

	public HttpTraceLog setRequestTime(long requestTime) {
		this.requestTime = requestTime;
		return this;
	}

	public long getCost() {
		return cost;
	}

	public HttpTraceLog setCost(long cost) {
		this.cost = cost;
		return this;
	}

	public Object getDetail(String key) {
		return detail.get(key);
	}

	public void setDetail(String key, Object value) {
		detail.put(key, value);
	}
}
