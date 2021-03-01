package com.quickutil.platform.entity;

public class HttpLog {

	private String host;
	private String path;
	private String xRequestId;
	private String xRealIp;
	private long cost;

	public String getHost() {
		return host;
	}

	public HttpLog setHost(String host) {
		this.host = host;
		return this;
	}

	public String getPath() {
		return path;
	}

	public HttpLog setPath(String path) {
		this.path = path;
		return this;
	}

	public String getXRequestId() {
		return xRequestId;
	}

	public HttpLog setXRequestId(String xRequestId) {
		this.xRequestId = xRequestId;
		return this;
	}

	public String getXRealIp() {
		return xRealIp;
	}

	public HttpLog setXRealIp(String xRealIp) {
		this.xRealIp = xRealIp;
		return this;
	}

	public long getCost() {
		return cost;
	}

	public HttpLog setCost(long cost) {
		this.cost = cost;
		return this;
	}
}
