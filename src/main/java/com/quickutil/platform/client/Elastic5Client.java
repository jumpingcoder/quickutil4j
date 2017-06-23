package com.quickutil.platform.client;

import com.quickutil.platform.StringUtil;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;

/**
 * @author shijie.ruan
 * 支持 5.4.1 的 elasticsearch
 * 使用函数封装,不一个业务写一个 json template
 */
public class Elastic5Client {
	private final String host;
	private final String version = "5.4.1";

	public final HttpClient client;

	private RequestConfig requestConfig = RequestConfig.custom()
			.setConnectionRequestTimeout(60000)
			.setConnectTimeout(60000)
			.setSocketTimeout(60000).build();

	private PoolingHttpClientConnectionManager cm;

	private HttpRequestRetryHandler httpRequestRetryHandler = new HttpRequestRetryHandler() {
		public boolean retryRequest(IOException exception,
				int executionCount, HttpContext context) {
			if (executionCount >= 5) {// 如果已经重试了5次，就放弃
				return false;
			}
			if (exception instanceof NoHttpResponseException) {// 如果服务器丢掉了连接，那么就重试
				return true;
			}
			if (exception instanceof SSLHandshakeException) {// 不要重试SSL握手异常
				return false;
			}
			if (exception instanceof InterruptedIOException) {// 超时
				return false;
			}
			if (exception instanceof UnknownHostException) {// 目标服务器不可达
				return false;
			}
			if (exception instanceof SSLException) {// SSL握手异常
				return false;
			}
			HttpClientContext clientContext = HttpClientContext
					.adapt(context);
			HttpRequest request = clientContext.getRequest();
			// 如果请求是幂等的，就再次尝试
			if (!(request instanceof HttpEntityEnclosingRequest)) {
				return true;
			}
			return false;
		}
	};

	public Elastic5Client(String host) {
		this.host = host;
		this.cm = new PoolingHttpClientConnectionManager();

		cm.setMaxTotal(50);
		cm.setDefaultMaxPerRoute(50);
		cm.setMaxPerRoute(new HttpRoute(new HttpHost(host)), 50);

		this.client =  HttpClients.custom()
				.setConnectionManager(cm)
				.setRetryHandler(httpRequestRetryHandler).build();
	}

	public String getHost() {
		return host;
	}

	public String getVersion() {
		return version;
	}
}
