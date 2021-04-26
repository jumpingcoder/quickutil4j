package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
import com.quickutil.platform.constants.Symbol;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;

/**
 * HttpClient工具
 *
 * @author 0.5
 */
public class HttpUtil {

	private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(HttpUtil.class);

	/**
	 * map-params转为query string
	 */
	public static String paramToQuery(Map<String, String> params) {
		if (params == null) {
			return "";
		}
		StringBuffer sb = new StringBuffer(Symbol.QUESTION_MARK);
		for (String key : params.keySet()) {
			sb.append(key);
			sb.append(Symbol.EQUAL);
			sb.append(params.get(key));
			sb.append(Symbol.AND);
		}
		return sb.substring(0, sb.length() - 1);
	}

	/**
	 * HttpGet
	 */
	public static HttpResponse httpGet(String url) {
		return httpGet(url, null, null, null);
	}

	public static HttpResponse httpGet(String url, Map<String, String> headers) {
		return httpGet(url, headers, null, null);
	}

	public static HttpResponse httpGet(String url, HttpClientConnectionManager manager) {
		return httpGet(url, null, manager, null);
	}

	public static HttpResponse httpGet(String url, RequestConfig requestConfig) {
		return httpGet(url, null, null, requestConfig);
	}

	public static HttpResponse httpGet(String url, Map<String, String> headers, HttpClientConnectionManager manager) {
		return httpGet(url, headers, manager, null);
	}

	public static HttpResponse httpGet(String url, Map<String, String> headers, RequestConfig requestConfig) {
		return httpGet(url, headers, null, requestConfig);
	}

	public static HttpResponse httpGet(String url, HttpClientConnectionManager manager, RequestConfig requestConfig) {
		return httpGet(url, null, manager, requestConfig);
	}

	public static HttpResponse httpGet(String url, Map<String, String> headers, HttpClientConnectionManager manager, RequestConfig config) {
		CloseableHttpClient httpClient = null;
		HttpGet method = new HttpGet(url);
		CloseableHttpResponse response = null;
		try {
			if (headers != null) {
				for (String key : headers.keySet()) {
					method.setHeader(key, headers.get(key));
				}
			}
			if (manager == null) {
				httpClient = HttpClientBuilder.create().build();
			} else {
				httpClient = HttpClientBuilder.create().setConnectionManager(manager).build();
			}
			if (config != null) {
				method.setConfig(config);
			}
			response = httpClient.execute(method);
			return response;
		} catch (Exception e1) {
			LOGGER.error(Symbol.BLANK, e1);
		}
		return null;
	}

	/**
	 * HttpPost
	 */
	public static HttpResponse httpPost(String url) {
		return httpPost(url, null, null, null, null);
	}

	public static HttpResponse httpPost(String url, byte[] body) {
		return httpPost(url, body, null, null, null);
	}

	public static HttpResponse httpPost(String url, byte[] body, Map<String, String> headers) {
		return httpPost(url, body, headers, null, null);
	}

	public static HttpResponse httpPost(String url, byte[] body, HttpClientConnectionManager manager) {
		return httpPost(url, body, null, manager, null);
	}

	public static HttpResponse httpPost(String url, byte[] body, RequestConfig config) {
		return httpPost(url, body, null, null, config);
	}

	public static HttpResponse httpPost(String url, byte[] body, Map<String, String> headers, HttpClientConnectionManager manager) {
		return httpPost(url, body, headers, manager, null);
	}

	public static HttpResponse httpPost(String url, byte[] body, Map<String, String> headers, RequestConfig config) {
		return httpPost(url, body, headers, null, config);
	}

	public static HttpResponse httpPost(String url, byte[] body, HttpClientConnectionManager manager, RequestConfig config) {
		return httpPost(url, body, null, manager, config);
	}

	public static HttpResponse httpPost(String url, byte[] body, Map<String, String> headers, HttpClientConnectionManager manager, RequestConfig config) {
		CloseableHttpClient httpClient = null;
		HttpPost method = new HttpPost(url);
		CloseableHttpResponse response = null;
		try {
			if (body != null) {
				ByteArrayEntity entity = new ByteArrayEntity(body);
				method.setEntity(entity);
			}
			if (headers != null) {
				for (String key : headers.keySet()) {
					method.setHeader(key, headers.get(key));
				}
			}
			if (manager == null) {
				httpClient = HttpClientBuilder.create().build();
			} else {
				httpClient = HttpClientBuilder.create().setConnectionManager(manager).build();
			}
			if (config != null) {
				method.setConfig(config);
			}
			response = httpClient.execute(method);
			return response;
		} catch (Exception e1) {
			LOGGER.error(Symbol.BLANK, e1);
		}
		return null;
	}

	/**
	 * HttpPut
	 */
	public static HttpResponse httpPut(String url) {
		return httpPut(url, null, null, null, null);
	}

	public static HttpResponse httpPut(String url, byte[] body) {
		return httpPut(url, body, null, null, null);
	}

	public static HttpResponse httpPut(String url, byte[] body, Map<String, String> headers) {
		return httpPut(url, body, headers, null, null);
	}

	public static HttpResponse httpPut(String url, byte[] body, HttpClientConnectionManager manager) {
		return httpPut(url, body, null, manager, null);
	}

	public static HttpResponse httpPut(String url, byte[] body, RequestConfig config) {
		return httpPut(url, body, null, null, config);
	}

	public static HttpResponse httpPut(String url, byte[] body, Map<String, String> headers, HttpClientConnectionManager manager) {
		return httpPut(url, body, headers, manager, null);
	}

	public static HttpResponse httpPut(String url, byte[] body, Map<String, String> headers, RequestConfig config) {
		return httpPut(url, body, headers, null, config);
	}

	public static HttpResponse httpPut(String url, byte[] body, HttpClientConnectionManager manager, RequestConfig config) {
		return httpPut(url, body, null, manager, config);
	}

	public static HttpResponse httpPut(String url, byte[] data, Map<String, String> headers, HttpClientConnectionManager manager, RequestConfig config) {
		CloseableHttpClient httpClient = null;
		HttpPut method = new HttpPut(url);
		CloseableHttpResponse response = null;
		try {
			if (data != null) {
				ByteArrayEntity entity = new ByteArrayEntity(data);
				method.setEntity(entity);
			}
			if (headers != null) {
				for (String key : headers.keySet()) {
					method.setHeader(key, headers.get(key));
				}
			}
			if (manager == null) {
				httpClient = HttpClientBuilder.create().build();
			} else {
				httpClient = HttpClientBuilder.create().setConnectionManager(manager).build();
			}
			if (config != null) {
				method.setConfig(config);
			}
			response = httpClient.execute(method);
			return response;
		} catch (Exception e1) {
			LOGGER.error(Symbol.BLANK, e1);
		}
		return null;
	}

	/**
	 * HttpDelete
	 */
	public static HttpResponse httpDelete(String url) {
		return httpDelete(url, null, null, null);
	}

	public static HttpResponse httpDelete(String url, Map<String, String> headers) {
		return httpDelete(url, headers, null, null);
	}

	public static HttpResponse httpDelete(String url, HttpClientConnectionManager manager) {
		return httpDelete(url, null, manager, null);
	}

	public static HttpResponse httpDelete(String url, RequestConfig requestConfig) {
		return httpDelete(url, null, null, requestConfig);
	}

	public static HttpResponse httpDelete(String url, Map<String, String> headers, HttpClientConnectionManager manager) {
		return httpDelete(url, headers, manager, null);
	}

	public static HttpResponse httpDelete(String url, Map<String, String> headers, RequestConfig requestConfig) {
		return httpDelete(url, headers, null, requestConfig);
	}

	public static HttpResponse httpDelete(String url, HttpClientConnectionManager manager, RequestConfig requestConfig) {
		return httpDelete(url, null, manager, requestConfig);
	}

	public static HttpResponse httpDelete(String url, Map<String, String> headers, HttpClientConnectionManager manager, RequestConfig config) {
		CloseableHttpClient httpClient = null;
		HttpDelete method = new HttpDelete(url);
		CloseableHttpResponse response = null;
		try {
			if (headers != null) {
				for (String key : headers.keySet()) {
					method.setHeader(key, headers.get(key));
				}
			}
			if (manager == null) {
				httpClient = HttpClientBuilder.create().build();
			} else {
				httpClient = HttpClientBuilder.create().setConnectionManager(manager).build();
			}
			if (config != null) {
				method.setConfig(config);
			}
			response = httpClient.execute(method);
			return response;
		} catch (Exception e1) {
			LOGGER.error(Symbol.BLANK, e1);
		}
		return null;
	}

	// 默认https初始化
	private static final X509TrustManager tm = new X509TrustManager() {
		@Override
		public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
		}

		@Override
		public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
		}

		@Override
		public X509Certificate[] getAcceptedIssuers() {
			return null;
		}
	};

	/**
	 * 生成https连接管理器
	 */
	public static HttpClientConnectionManager buildHttpsClientMananger(InputStream clientCer, String clientPW, InputStream serverCer, String serverPW) {
		try {
			KeyManager[] keysManagers = null;
			TrustManager[] trustManagers = null;
			// 验证客户端证书
			if (clientCer != null) {
				KeyStore ks = KeyStore.getInstance("pkcs12");
				ks.load(clientCer, clientPW.toCharArray());
				KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
				keyManagerFactory.init(ks, clientPW.toCharArray());
				keysManagers = keyManagerFactory.getKeyManagers();
			}
			// 验证服务端证书
			if (serverCer != null) {
				KeyStore ks2 = KeyStore.getInstance("pkcs12");
				ks2.load(serverCer, serverPW.toCharArray());
				TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
				trustManagerFactory.init(ks2);
				trustManagers = trustManagerFactory.getTrustManagers();
			} else {
				trustManagers = new TrustManager[]{tm};
			}
			// 生成ssl参数
			SSLContext context = SSLContext.getInstance("TLS");
			context.init(keysManagers, trustManagers, null);
			SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(context);
			Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create().register("http", PlainConnectionSocketFactory.INSTANCE)
					.register("https", socketFactory).build();
			return new PoolingHttpClientConnectionManager(socketFactoryRegistry);
		} catch (Exception e) {
			LOGGER.error(Symbol.BLANK, e);
			return null;
		}
	}

	/**
	 * 生成连接参数管理器
	 */
	public static RequestConfig buildRequestConfig(int connectionTimeout, int readTimeout) {
		return RequestConfig.custom().setConnectTimeout(connectionTimeout).setSocketTimeout(readTimeout).build();
	}
}
