/**
 * HttpClient工具
 *
 * @class HttpUtil
 * @author 0.5
 */
package com.quickutil.platform;

import ch.qos.logback.classic.Logger;
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

public class HttpUtil {

    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(HttpUtil.class);

    /**
     * map-params转为query string
     *
     * @param params-参数
     * @return
     */
    public static String paramToQuery(Map<String, String> params) {
        if (params == null)
            return "";
        StringBuffer sb = new StringBuffer("?");
        for (String key : params.keySet()) {
            sb.append(key);
            sb.append("=");
            sb.append(params.get(key));
            sb.append("&");
        }
        return sb.substring(0, sb.length() - 1);
    }

    /**
     * HttpGet
     *
     * @param url-请求的URL
     * @return
     */
    public static HttpResponse httpGet(String url) {
        return httpGet(url, null, null, null);
    }

    /**
     * HttpGet
     *
     * @param url-请求的URL
     * @param headers-自定义请求头
     * @return
     */
    public static HttpResponse httpGet(String url, Map<String, String> headers) {
        return httpGet(url, headers, null, null);
    }

    /**
     * HttpGet
     *
     * @param url-请求的URL
     * @param headers-自定义请求头
     * @param manager-设置连接管理参数，通常用于https设置
     * @return
     */
    public static HttpResponse httpGet(String url, Map<String, String> headers, HttpClientConnectionManager manager) {
        return httpGet(url, headers, manager, null);
    }

    /**
     * HttpGet
     *
     * @param url-请求的URL
     * @param headers-自定义请求头
     * @param manager-设置连接管理参数，通常用于https设置
     * @param config-设置请求参数，通常用于超时设置
     * @return
     */
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
            if (manager == null)
                httpClient = HttpClientBuilder.create().build();
            else
                httpClient = HttpClientBuilder.create().setConnectionManager(manager).build();
            if (config != null)
                method.setConfig(config);
            response = httpClient.execute(method);
            return response;
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        return null;
    }

    /**
     * HttpPost
     *
     * @param url-请求的URL
     * @return
     */
    public static HttpResponse httpPost(String url) {
        return httpPost(url, null, null, null, null);
    }

    /**
     * HttpPost
     *
     * @param url-请求的URL
     * @param body-请求的body
     * @return
     */
    public static HttpResponse httpPost(String url, byte[] body) {
        return httpPost(url, body, null, null, null);
    }

    /**
     * HttpPost
     *
     * @param url-请求的URL
     * @param body-请求的body
     * @param headers-自定义请求头
     * @return
     */
    public static HttpResponse httpPost(String url, byte[] body, Map<String, String> headers) {
        return httpPost(url, body, headers, null, null);
    }

    /**
     * HttpPost
     *
     * @param url-请求的URL
     * @param data-请求的body
     * @param headers-自定义请求头
     * @param manager-设置连接管理参数，通常用于https设置
     * @return
     */
    public static HttpResponse httpPost(String url, byte[] data, Map<String, String> headers, HttpClientConnectionManager manager) {
        return httpPost(url, data, headers, manager, null);
    }

    /**
     * HttpPost
     *
     * @param url-请求的URL
     * @param data-请求的body
     * @param headers-自定义请求头
     * @param manager-设置连接管理参数，通常用于https设置
     * @param config-设置请求参数，通常用于超时设置
     * @return
     */
    public static HttpResponse httpPost(String url, byte[] data, Map<String, String> headers, HttpClientConnectionManager manager, RequestConfig config) {
        CloseableHttpClient httpClient = null;
        HttpPost method = new HttpPost(url);
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
            if (manager == null)
                httpClient = HttpClientBuilder.create().build();
            else
                httpClient = HttpClientBuilder.create().setConnectionManager(manager).build();
            if (config != null)
                method.setConfig(config);
            response = httpClient.execute(method);
            return response;
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        return null;
    }

    /**
     * HttpPut
     *
     * @param url-请求的URL
     * @return
     */
    public static HttpResponse httpPut(String url) {
        return httpPut(url, null, null, null, null);
    }

    /**
     * HttpPut
     *
     * @param url-请求的URL
     * @param data-请求的body
     * @return
     */
    public static HttpResponse httpPut(String url, byte[] data) {
        return httpPut(url, data, null, null, null);
    }

    /**
     * HttpPut
     *
     * @param url-请求的URL
     * @param data-请求的body
     * @param headers-自定义请求头
     * @return
     */
    public static HttpResponse httpPut(String url, byte[] data, Map<String, String> headers) {
        return httpPut(url, data, headers, null, null);
    }

    /**
     * HttpPut
     *
     * @param url-请求的URL
     * @param data-请求的body
     * @param headers-自定义请求头
     * @param manager-设置连接管理参数，通常用于https设置
     * @return
     */
    public static HttpResponse httpPut(String url, byte[] data, Map<String, String> headers, HttpClientConnectionManager manager) {
        return httpPut(url, data, headers, manager, null);
    }

    /**
     * HttpPut
     *
     * @param url-请求的URL
     * @param data-请求的body
     * @param headers-自定义请求头
     * @param manager-设置连接管理参数，通常用于https设置
     * @param config-设置请求参数，通常用于超时设置
     * @return
     */
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
            if (manager == null)
                httpClient = HttpClientBuilder.create().build();
            else
                httpClient = HttpClientBuilder.create().setConnectionManager(manager).build();
            if (config != null)
                method.setConfig(config);
            response = httpClient.execute(method);
            return response;
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        return null;
    }

    /**
     * HttpDelete
     *
     * @param url-请求的URL
     * @return
     */
    public static HttpResponse httpDelete(String url) {
        return httpDelete(url, null, null, null);
    }

    /**
     * HttpDelete
     *
     * @param url-请求的URL
     * @param headers-自定义请求头
     * @return
     */
    public static HttpResponse httpDelete(String url, Map<String, String> headers) {
        return httpDelete(url, headers, null, null);
    }

    /**
     * HttpDelete
     *
     * @param url-请求的URL
     * @param headers-自定义请求头
     * @param manager-设置连接管理参数，通常用于https设置
     * @return
     */
    public static HttpResponse httpDelete(String url, Map<String, String> headers, HttpClientConnectionManager manager) {
        return httpDelete(url, headers, manager, null);
    }

    /**
     * HttpDelete
     *
     * @param url-请求的URL
     * @param headers-自定义请求头
     * @param manager-设置连接管理参数，通常用于https设置
     * @param config-设置请求参数，通常用于超时设置
     * @return
     */
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
            if (manager == null)
                httpClient = HttpClientBuilder.create().build();
            else
                httpClient = HttpClientBuilder.create().setConnectionManager(manager).build();
            if (config != null)
                method.setConfig(config);
            response = httpClient.execute(method);
            return response;
        } catch (Exception e) {
            LOGGER.error("", e);
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
     *
     * @param clientCer-客户端证书
     * @param clientPW-客户端证书密钥
     * @param serverCer-服务端证书
     * @param serverPW-服务端证书密钥
     * @return
     */
    public static HttpClientConnectionManager initHttpsClientMananger(InputStream clientCer, String clientPW, InputStream serverCer, String serverPW) {
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
            LOGGER.error("", e);
            return null;
        }
    }
}
