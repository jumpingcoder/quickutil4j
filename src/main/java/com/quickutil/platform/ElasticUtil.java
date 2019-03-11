package com.quickutil.platform;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.quickutil.platform.def.BulkResponse;
import com.quickutil.platform.def.SearchRequest;
import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.Function;

/**
 * @author shijie.ruan
 */
public class ElasticUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticUtil.class);
//    private static final String indexFormat = "/%s/";
//    private static final String indexTypeFormat = "/%s/%s/";

    private HttpClient client;
    private String host;

    private static RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(2 * 60000).setConnectTimeout(2 * 60000).setSocketTimeout(2 * 60000).build();

    private static PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();

    private static HttpRequestRetryHandler httpRequestRetryHandler = new HttpRequestRetryHandler() {
        public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
            if (executionCount >= 5) {// 如果已经重试了5次，就放弃
                return false;
            }
            if (exception instanceof NoHttpResponseException) {// 如果服务器丢掉了连接，那么就重试
                return true;
            }
            if (exception instanceof SSLHandshakeException) {// 不要重试SSL握手异常
                return false;
            }
            if (exception instanceof SocketTimeoutException) {// 超时
                return true;
            }
            if (exception instanceof InterruptedIOException) {// 被打断
                return true;
            }
            if (exception instanceof UnknownHostException) {// 目标服务器不可达
                return false;
            }
            if (exception instanceof SSLException) {// SSL握手异常
                return false;
            }
            if (exception instanceof TruncatedChunkException) {//丢数据
                return true;
            }
            HttpClientContext clientContext = HttpClientContext.adapt(context);
            HttpRequest request = clientContext.getRequest();
            // 如果请求是幂等的，就再次尝试
            if (!(request instanceof HttpEntityEnclosingRequest))
                return true;
            return false;
        }
    };

    public ElasticUtil(String host, String username, String password) {
        List<Header> headers = new ArrayList<>();
        if (username != null && password != null) {
            Header header = new BasicHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes()));
            headers.add(header);
            headers.add(new BasicHeader("Content-Type", "application/json"));
        }
        this.host = host;
        connectionManager.setMaxPerRoute(new HttpRoute(new HttpHost(host)), 50);//设置连接池
        connectionManager.setMaxTotal(50);
        this.client = HttpClients.custom().setDefaultHeaders(headers).setConnectionManager(connectionManager).setRetryHandler(httpRequestRetryHandler).build();
    }

    public HttpClient getElasticClient() {
        return client;
    }

    public String getHost() {
        return host;
    }

    private HttpUriRequest getMethod(String url) {
        HttpGet httpGet = new HttpGet(url);
        httpGet.setConfig(requestConfig);
        return httpGet;
    }

    private HttpUriRequest postMethod(String url, String entity) {
        HttpPost httpPost = new HttpPost(url);
        httpPost.setConfig(requestConfig);
        if (null != entity && !entity.isEmpty()) {
            httpPost.setEntity(new ByteArrayEntity(entity.getBytes()));
        }
        return httpPost;
    }

    /**
     * 使用id查询数据 有任何错误返回都返回空
     *
     * @param index-ES的index
     * @param type-ES的type
     * @param id-ES的id
     * @return 返回结果
     */
    public String selectById(String index, String type, String id) {
        String url = null;
        if (type == null)
            url = String.format("%s/%s/%s/_source", host, index, id);
        else
            url = String.format("%s/%s/%s/%s/_source", host, index, type, id);
        HttpResponse response = null;
        try {
            response = client.execute(getMethod(url));
            if (response == null)
                return null;
            else if (200 != response.getStatusLine().getStatusCode()) {
                LOGGER.warn("response code [{}], with msg [{}], with url [{}]", response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase(), url);
                return null;
            } else {
                return getEntity(response);
            }
        } catch (Exception e) {
            LOGGER.error("fail on url: " + url, e);
        } finally {
            HttpClientUtils.closeQuietly(response);
        }
        return null;
    }

    /**
     * 写入一个文档,返回成功或失败
     *
     * @param index-ES的index
     * @param type-ES的type
     * @param id-ES的id
     * @param source-写入的内容
     * @return 返回结果
     */
    public boolean insert(String index, String type, String id, JsonObject source) {
        if (null == index || null == id) {
            return false;
        }
        String url = null;
        if (type == null)
            url = String.format("%s/%s/%s", host, index, id).replace(" ", "");
        else
            url = String.format("%s/%s/%s/%s", host, index, type, id).replace(" ", "");
        String sourceString = (null == source ? "{}" : JsonUtil.toJson(source));
        HttpResponse response = null;
        try {
            response = client.execute(postMethod(url, sourceString));
            if (200 == response.getStatusLine().getStatusCode() || 201 == response.getStatusLine().getStatusCode()) {
                return true;
            } else {
                LOGGER.error("fail on url: " + url + "\nwith source: " + sourceString + "\nresponse: " + getEntity(response));
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("fail on url: " + url, e);
        } finally {
            HttpClientUtils.closeQuietly(response);
        }
        return false;
    }

    /**
     * 部分更新一个文档,返回成功或失败
     *
     * @param index-ES的index
     * @param type-ES的type
     * @param id-ES的id
     * @param source-更新的内容
     * @param isUpsert-true,表示如果文档不存在则插入,false,时如果不存在则不插入
     * @return 返回结果
     */
    public boolean update(String index, String type, String id, JsonObject source, boolean isUpsert) {
        if (null == index || id == null) {
            return false;
        }
        String url = null;
        if (type == null)
            url = String.format("%s/%s/%s/_update", host, index, id).replace(" ", "");
        else
            url = String.format("%s/%s/%s/%s/_update", host, index, type, id).replace(" ", "");
        Map<String, Object> map = new HashMap<>();
        if (null == source) {
            source = new JsonObject();
        }
        map.put("doc", source);
        map.put("doc_as_upsert", isUpsert);
        String sourceString = JsonUtil.toJson(map);
        HttpResponse response = null;
        try {
            response = client.execute(postMethod(url, sourceString));
            if (200 == response.getStatusLine().getStatusCode() || 201 == response.getStatusLine().getStatusCode()) {
                return true;
            } else if (404 == response.getStatusLine().getStatusCode()) {
                LOGGER.error("fail on url: " + url + "\ndocument missing");
                return false;
            } else {
                LOGGER.error("fail on url: " + url + "\nwith source: " + sourceString + "\nresponse: " + getEntity(response));
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("fail on url: " + url + "\nwith source:" + sourceString + "\n", e);
        } finally {
            HttpClientUtils.closeQuietly(response);
        }
        return false;
    }

    private static final String getIndex = "%s/%s/";
    private static final String getIndexType = "%s/%s/%s/";
    private static final String indexWithoutType = "{\"index\":{\"_index\":\"%s\",\"_id\":\"%s\"}}\n";
    private static final String indexWithType = "{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_id\":\"%s\"}}\n";
    private static final String updateWithoutType = "{\"update\":{\"_index\":\"%s\",\"_id\":\"%s\"}}\n";
    private static final String updateWithType = "{\"update\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_id\":\"%s\"}}\n";
    private static final String deleteWithoutType = "{\"delete\":{\"_index\":\"%s\",\"_id\":\"%s\"}}\n";
    private static final String deleteWithType = "{\"delete\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_id\":\"%s\"}}\n";
    private static final String searchWithoutType = "%s/%s/_search";
    private static final String searchWithType = "%s/%s/%s/_search";
    private static final String scrollWithoutType = "%s/%s/_search?scroll=5m";
    private static final String scrollWithType = "%s/%s/%s/_search?scroll=5m";
    private static final String msearchWithoutType = "%s/%s/_msearch";
    private static final String msearchWithType = "%s/%s/%s/_msearch";

    /**
     * 发起批量请求, 支持 index, create, update, delete(被屏蔽), 其中 index, update, create 下一行都需要是 文档的内容, delete 下一行不能是文档的内容
     *
     * @param index  index
     * @param type   type
     * @param entity entity
     * @return 返回结果
     */
    private BulkResponse bulk(String index, String type, String entity) {
        String url = null;
        if (index == null)
            url = String.format("%s/_bulk", host);
        else if (index != null && type == null)
            url = String.format("%s/%s/_bulk", host, index);
        else
            url = String.format("%s/%s/%s/_bulk", host, index, type);
        HttpResponse response = null;
        String result = "";
        try {
            long start = System.currentTimeMillis();
            response = client.execute(postMethod(url, entity));
            LOGGER.debug("time for execute bulk:" + (System.currentTimeMillis() - start));
            result = getEntity(response);
            if (200 != response.getStatusLine().getStatusCode()) {
                JsonObject bulkRequestError = JsonUtil.toJsonMap(result).getAsJsonObject("error");
                return new BulkResponse(BulkResponse.RequestFail, bulkRequestError);
            } else {
                JsonObject responseObject = JsonUtil.toJsonMap(result);
                boolean hasErrors = responseObject.get("errors").getAsBoolean();
                if (!hasErrors) {
                    return new BulkResponse(BulkResponse.Success);
                } else {
                    JsonArray responseArray = responseObject.getAsJsonArray("items");
                    return new BulkResponse(BulkResponse.PortionFail, responseArray);
                }
            }
        } catch (Exception e) {
            LOGGER.error("bulk request fail on url:" + url, e);
            JsonObject responseObject = new JsonObject();
            responseObject.addProperty("result", result);
            return new BulkResponse(BulkResponse.RequestFail, responseObject);
        } finally {
            HttpClientUtils.closeQuietly(response);
        }
    }

    private long lasttime = 0;
    private StringBuffer sb = new StringBuffer();
    private int count = 0;

    /**
     * 缓存队列式批量写入，每秒写入一次, 时间不到1s, 返回写入成功,其实在程序缓存中, 这时候如果程序崩溃或者重启会丢失这一秒的数据
     *
     * @param index-ES的index
     * @param type-ES的type
     * @param id-ES的id
     * @param source-写入的内容
     * @return 返回结果
     */
    public boolean bulkInsertBuffer(String index, String type, String id, String source) {
        try {
            count++;
            if (type == null)
                sb.append(String.format(indexWithoutType, index, id) + source + "\n");
            else
                sb.append(String.format(indexWithType, index, type, id) + source + "\n");
            String entity = null;
            if (System.currentTimeMillis() - lasttime > 1000) {
                entity = sb.toString();
                lasttime = System.currentTimeMillis();
                sb = new StringBuffer();
                LOGGER.debug("content count:" + count);
                count = 0;
                BulkResponse response = bulk(null, null, entity);
                if (!response.isSuccess()) {
                    LOGGER.error("fail--" + response.errorMessage() + "\n" + entity);
                    return false;
                } else {
                    return true;
                }
            } else {
                return true;
            }
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        return false;
    }

    /**
     * 批量写入
     *
     * @param array-json数组，必须要有_index和_id，_type可选
     * @return 返回结果
     */
    public BulkResponse bulkInsert(JsonArray array) {
        StringBuilder entity = new StringBuilder();
        for (JsonElement e : array) {
            if (!e.getAsJsonObject().has("_index") || !e.getAsJsonObject().has("_id")) {
                JsonObject insertError = new JsonObject();
                insertError.addProperty("msg", "bulk insert must specify index and id");
                return new BulkResponse(BulkResponse.RequestFail, insertError);
            }
            if (!e.getAsJsonObject().has("_type")) {
                entity.append(String.format(indexWithoutType, e.getAsJsonObject().get("_index").getAsString(), e.getAsJsonObject().get("_id").getAsString()) + e.toString() + "\n");
            } else {
                entity.append(String.format(indexWithType, e.getAsJsonObject().get("_index").getAsString(), e.getAsJsonObject().get("_type").getAsString(), e.getAsJsonObject().get("_id").getAsString()) + e.toString() + "\n");
            }
        }
        return bulk(null, null, entity.toString());
    }

    /**
     * 批量写入
     *
     * @param list-map数组，必须要有_index和_id，_type可选
     * @return 返回结果
     */
    public BulkResponse bulkInsert(List<Map<String, Object>> list) {
        StringBuilder entity = new StringBuilder();
        for (Map<String, Object> map : list) {
            if (map.get("_index") == null || map.get("_id") == null) {
                JsonObject insertError = new JsonObject();
                insertError.addProperty("msg", "bulk insert must specify index and id");
                return new BulkResponse(BulkResponse.RequestFail, insertError);
            }
            if (map.get("_type") == null) {
                entity.append(String.format(indexWithoutType, map.get("_index").toString(), map.get("_id").toString()) + JsonUtil.toJson(map) + "\n");
            } else {
                entity.append(String.format(indexWithType, map.get("_index").toString(), map.get("_type").toString(), map.get("_id").toString()) + JsonUtil.toJson(map) + "\n");
            }
        }
        return bulk(null, null, entity.toString());
    }

    /**
     * 批量更新,同一个 index 和 type
     *
     * @param array-更新的内容，必须要有_index和_id，_type可选
     * @param upsert-文档不存在时插入,其实控制粒度是对于每一个文档的,但是这里为了方便输入,粒度为同一次批量的文档
     * @return 返回结果
     */
    public BulkResponse bulkUpdate(JsonArray array, boolean upsert) {
        StringBuilder entity = new StringBuilder();
        for (JsonElement e : array) {
            if (!e.getAsJsonObject().has("_index") || !e.getAsJsonObject().has("_id")) {
                JsonObject insertError = new JsonObject();
                insertError.addProperty("msg", "bulk update must specify index and id");
                return new BulkResponse(BulkResponse.RequestFail, insertError);
            }
            if (!e.getAsJsonObject().has("_type")) {
                entity.append(String.format(updateWithoutType, e.getAsJsonObject().get("_index").getAsString(), e.getAsJsonObject().get("_id").getAsString()));
                JsonObject item = new JsonObject();
                item.add("doc", e);
                if (upsert) {
                    item.addProperty("doc_as_upsert", true);
                }
                entity.append(item.toString() + "\n");
            } else {
                entity.append(String.format(updateWithType, e.getAsJsonObject().get("_index").getAsString(), e.getAsJsonObject().get("_type").getAsString(), e.getAsJsonObject().get("_id").getAsString()));
                JsonObject item = new JsonObject();
                item.add("doc", e);
                if (upsert) {
                    item.addProperty("doc_as_upsert", true);
                }
                entity.append(item.toString() + "\n");
            }
        }
        return bulk(null, null, entity.toString());
    }

    /**
     * 批量更新,同一个 index 和 type
     *
     * @param list-更新的内容，必须要有_index和_id，_type可选
     * @param upsert-文档不存在时插入,其实控制粒度是对于每一个文档的,但是这里为了方便输入,粒度为同一次批量的文档
     * @return 返回结果
     */
    public BulkResponse bulkUpdate(List<Map<String, Object>> list, boolean upsert) {
        StringBuilder entity = new StringBuilder();
        for (Map<String, Object> map : list) {
            if (map.get("_index") == null || map.get("_id") == null) {
                JsonObject insertError = new JsonObject();
                insertError.addProperty("msg", "bulk update must specify index and id");
                return new BulkResponse(BulkResponse.RequestFail, insertError);
            }
            if (map.get("_type") == null) {
                entity.append(String.format(updateWithoutType, map.get("_index").toString(), map.get("_id").toString()));
                Map<String, Object> item = new HashMap<>();
                item.put("doc", map);
                if (upsert) {
                    map.put("doc_as_upsert", true);
                }
                entity.append(JsonUtil.toJson(item) + "\n");
            } else {
                entity.append(String.format(updateWithType, map.get("_index").toString(), map.get("_type").toString(), map.get("_id").toString()));
                Map<String, Object> item = new HashMap<>();
                item.put("doc", map);
                if (upsert) {
                    map.put("doc_as_upsert", true);
                }
                entity.append(JsonUtil.toJson(item) + "\n");
            }
        }
        return bulk(null, null, entity.toString());
    }

    /**
     * 脚本批量更新,使用同一个脚本同一个 index 和 type.
     *
     * @param index-ES的index
     * @param type-ES的type
     * @param source-更新的内容,key为id，value为source
     * @param scriptFile-放在elasticsearch                             home 目录下的 config/script 目录下的groovy脚本, 为了安全,不支持在请求中带上脚本 之所以是 groovy 脚本,因为 groovy 是 2.x 和 5.x 都支持的
     * @param lang-脚本的语言类型,                                          支持 groovy, painless
     * @param upsert-文档不存在时插入,其实控制粒度是对于每一个文档的,但是这里为了方便输入,粒度为同一次批量的文档
     * @return 返回结果
     */
    public BulkResponse bulkUpdateByScript(String index, String type, Map<String, JsonObject> source, String scriptFile, String lang, boolean upsert) {
        String idFormat = "{\"update\": {\"_id\": \"%s\"}}\n";
        if (null == index) {
            JsonObject insertError = new JsonObject();
            insertError.addProperty("msg", "bulk update must specify index");
            return new BulkResponse(BulkResponse.RequestFail, insertError);
        }
        StringBuilder entity = new StringBuilder();
        for (String id : source.keySet()) {
            JsonObject item = new JsonObject();
            JsonObject scriptObject = new JsonObject();
            scriptObject.addProperty("lang", lang);
            scriptObject.addProperty("file", scriptFile);
            scriptObject.add("params", source.get(id));
            item.add("script", scriptObject);
            if (upsert) {
                item.add("upsert", source.get(id));
            }
            entity.append(String.format(idFormat, id)).append(item).append("\n");
        }
        return bulk(index, type, entity.toString());
    }

    /**
     * 使用 groovy 脚本
     */
    public BulkResponse bulkUpdateByScript(String index, String type, Map<String, JsonObject> source, String scriptFile, boolean upsert) {
        return bulkUpdateByScript(index, type, source, scriptFile, "groovy", upsert);
    }

    /**
     * 批量删除
     *
     * @param array-json数组，必须要有_index和_id，_type可选
     * @return 返回结果
     */
    public BulkResponse bulkDelete(JsonArray array) {
        StringBuilder entity = new StringBuilder();
        for (JsonElement e : array) {
            if (!e.getAsJsonObject().has("_index") || !e.getAsJsonObject().has("_id")) {
                JsonObject insertError = new JsonObject();
                insertError.addProperty("msg", "bulk delete must specify index and id");
                return new BulkResponse(BulkResponse.RequestFail, insertError);
            }
            if (!e.getAsJsonObject().has("_type")) {
                entity.append(String.format(deleteWithoutType, e.getAsJsonObject().get("_index").getAsString(), e.getAsJsonObject().get("_id").getAsString()) + e.toString() + "\n");
            } else {
                entity.append(String.format(deleteWithType, e.getAsJsonObject().get("_index").getAsString(), e.getAsJsonObject().get("_type").getAsString(), e.getAsJsonObject().get("_id").getAsString()) + e.toString() + "\n");
            }
        }
        return bulk(null, null, entity.toString());
    }

    /**
     * 批量删除
     *
     * @param list-删除的内容，必须要有_index和_id，_type可选
     * @return 返回结果
     */
    public BulkResponse bulkDelete(List<Map<String, Object>> list) {
        StringBuilder entity = new StringBuilder();
        for (Map<String, Object> map : list) {
            if (map.get("_index") == null || map.get("_id") == null) {
                JsonObject insertError = new JsonObject();
                insertError.addProperty("msg", "bulk delete must specify index and id");
                return new BulkResponse(BulkResponse.RequestFail, insertError);
            }
            if (map.get("_type") == null) {
                entity.append(String.format(deleteWithoutType, map.get("_index").toString(), map.get("_id").toString()) + JsonUtil.toJson(map) + "\n");
            } else {
                entity.append(String.format(deleteWithType, map.get("_index").toString(), map.get("_type").toString(), map.get("_id").toString()) + JsonUtil.toJson(map) + "\n");
            }
        }
        return bulk(null, null, entity.toString());
    }

    /**
     * 查询或者聚合请求
     *
     * @param index         ES的index(可以包含*作为通配符)
     * @param type          ES的type(可以为空)
     * @param searchRequest
     * @return 返回结果
     */
    public String search(String index, String type, SearchRequest searchRequest) {
        HttpResponse response = null;
        String url = null;
        try {
            if (type == null)
                url = String.format(searchWithoutType, host, index);
            else
                url = String.format(searchWithType, host, index, type);
            response = client.execute(postMethod(url, searchRequest.toJson()));
            if (200 != response.getStatusLine().getStatusCode()) {
                LOGGER.error("fail on url: " + url + "\nresponse:" + getEntity(response));
                return null;
            }
            return getEntity(response);
        } catch (Exception e) {
            LOGGER.error("fail on url: " + url, e);
            return null;
        } finally {
            HttpClientUtils.closeQuietly(response);
        }
    }

    /**
     * 对同一个 index(可以包含*作为通配符) 和 type(可以为空) 进行批量搜索
     *
     * @param index-ES的index
     * @param type-ES的type(可以为空)
     * @param searches-请求的列表
     * @return 返回结果
     */
    public String mSearch(String index, String type, List<SearchRequest> searches) {
        HttpResponse response = null;
        String url = null;
        try {
            if (type == null)
                url = String.format(msearchWithoutType, host, index);
            else
                url = String.format(msearchWithType, host, index, type);
            StringBuilder entity = new StringBuilder();
            for (SearchRequest searchRequest : searches) {
                entity.append("{}\n");
                entity.append(searchRequest.toJson()).append("\n");
            }
            response = client.execute(postMethod(url, entity.toString()));
            if (200 != response.getStatusLine().getStatusCode()) {
                LOGGER.error("fail on url: " + url + "with source:\n" + entity.toString());
                return null;
            }
            return getEntity(response);
        } catch (Exception e) {
            LOGGER.error("fail on url: " + url, e);
        } finally {
            HttpClientUtils.closeQuietly(response);
        }
        return null;
    }

    public String getFirstScrollSearch(String index) {
        return getFirstScrollSearch(index, null, null);
    }

    public String getFirstScrollSearch(String index, SearchRequest searchRequest) {
        return getFirstScrollSearch(index, null, searchRequest);
    }

    /**
     * 获取首次 scroll 查询
     *
     * @param index         可以是通配符,不能为空
     * @param type          可以为空
     * @param searchRequest 可以为空
     * @return 返回结果
     */
    public String getFirstScrollSearch(String index, String type, SearchRequest searchRequest) {
        String url = null;
        HttpResponse response = null;
        try {
            if (type == null) {
                url = String.format(scrollWithoutType, host, index);
            } else {
                url = String.format(scrollWithType, host, index, type);
            }
            String query = (null == searchRequest ? "" : searchRequest.toJson());
            response = client.execute(postMethod(url, query));
            if (200 != response.getStatusLine().getStatusCode()) {
                LOGGER.error("fail on scroll search :" + url + "\n response: " + getEntity(response));
                return null;
            }
            return getEntity(response);
        } catch (Exception e) {
            LOGGER.error("fail on scroll search url: " + url);
        } finally {
            HttpClientUtils.closeQuietly(response);
        }
        return null;
    }

    /**
     * 获取后续的 scroll 搜索的 url
     *
     * @param scrollId
     * @return 返回结果
     */
    public String getScrollSearch(String scrollId) {
        String url = String.format("%s/_search/scroll?scroll=5m&scroll_id=%s", host, scrollId);
        HttpResponse response = null;
        try {
            response = client.execute(getMethod(url));
            if (200 != response.getStatusLine().getStatusCode()) {
                LOGGER.error("fail on scroll search :" + url + "\n response: " + getEntity(response));
                return null;
            }
            return getEntity(response);
        } catch (Exception e) {
            LOGGER.error("fail on scroll search url: " + url, e);
        } finally {
            HttpClientUtils.closeQuietly(response);
        }
        return null;
    }

    /**
     * ES管理：正则获取所有相关的 index 名字
     *
     * @return 返回结果
     */
    public String[] getIndexName(String indexNameReg) {
        HttpResponse response = null;
        try {
            response = client.execute(getMethod(host + "/_cat/indices/" + indexNameReg));
            if (200 != response.getStatusLine().getStatusCode()) {
                LOGGER.error("get index name error, with response: " + getEntity(response));
                return null;
            }
            String[] indicesStats = getEntity(response).split("\\n");
            String[] indicesNames = new String[indicesStats.length];
            // 返回的 index 信息形如"green open activation-2014 5 1 2944 4522 2.6mb 1.3mb ",需要获取名字
            for (int i = 0; i < indicesStats.length; i++) {
                String indexName = indicesStats[i].split("\\s+")[2];
                indicesNames[i] = indexName;
            }
            return indicesNames;
        } catch (Exception e) {
            LOGGER.error("get index name fail with reg: " + indexNameReg, e);
        } finally {
            HttpClientUtils.closeQuietly(response);
        }
        return null;
    }

    /**
     * 查看 index 是否存在
     *
     * @param index
     * @return 返回结果
     */
    public boolean checkIndexExist(String index) {
        String getIndexExistUrl = String.format(getIndex, host, index);
        HttpResponse response = null;
        try {
            response = client.execute(getMethod(getIndexExistUrl));
            if (404 == response.getStatusLine().getStatusCode()) {
                return false;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        } finally {
            HttpClientUtils.closeQuietly(response);
        }
        return true;
    }

    /**
     * ES管理：创建索引, 请先运行 checkIndexExist 判断是否存在,不要直接运行
     *
     * @param index
     * @param mappings
     * @return 返回结果
     */
    public boolean createIndex(String index, String mappings) {
        String createIndexUrl = String.format(getIndex, host, index);
        HttpResponse response = null;
        try {
            response = HttpUtil.httpPut(createIndexUrl, mappings.getBytes());
            if (200 != response.getStatusLine().getStatusCode()) {
                LOGGER.error("create index fail, response: " + getEntity(response));
                return false;
            }
            return true;
        } catch (Exception e) {
            LOGGER.error("create index fail", e);
            return false;
        } finally {
            HttpClientUtils.closeQuietly(response);
        }
    }

    /**
     * ES管理：获取 index 的 mapping
     *
     * @param index
     * @return 返回结果
     */
    public String getMapping(String index) {
        String getMappingsUrl = String.format(getIndex + "_mapping", host, index);
        try {
            HttpResponse response = HttpUtil.httpGet(getMappingsUrl);
            return new String(FileUtil.stream2byte(response.getEntity().getContent()));
        } catch (Exception var3) {
            var3.printStackTrace();
            return null;
        }
    }

    private String getEntity(HttpResponse response) throws IOException {
        return EntityUtils.toString(response.getEntity());
    }


    /**
     * 查询 es 的结果保存成 csv
     *
     * @param index         index
     * @param type          type
     * @param searchRequest sr
     * @param filePath      文件路径
     * @param jsonToCSV     将 hit 变成 csv 的一行
     */
    public void dumpESDataToCsv(String index, String type, SearchRequest searchRequest, String
            filePath, Function<JsonObject, String> jsonToCSV) {
        assert (index != null && filePath != null && jsonToCSV != null);
        try {
            File f = new File(filePath);
            if (!f.exists()) {
                f.createNewFile();
            }
            String resp = getFirstScrollSearch(index, type, searchRequest);
            if (null == resp) {
                LOGGER.info("search get empty result, terminate.");
                return;
            }
            JsonObject result = JsonUtil.toJsonMap(resp);
            String scrollId;
            JsonArray array = result.getAsJsonObject("hits").getAsJsonArray("hits");
            long total = result.getAsJsonObject("hits").get("total").getAsLong();
            long count = 0;
            while (array.size() > 0) {
                StringBuilder bulk = new StringBuilder();
                for (JsonElement e : array) {
                    JsonObject doc = e.getAsJsonObject();
                    String csvLine = jsonToCSV.apply(doc);
                    if (csvLine != null) {
                        bulk.append(csvLine + "\n");
                        count++;
                    }
                }
                // append file
                FileUtil.string2File(filePath, bulk.toString(), true);
                scrollId = result.get("_scroll_id").getAsString();
                resp = getScrollSearch(scrollId);
                if (null == resp) {
                    return;
                }
                array = JsonUtil.toJsonMap(resp).getAsJsonObject("hits").getAsJsonArray("hits");
                LOGGER.debug("index: " + index + ShellUtil.printProgress((double) count / total));
            }
        } catch (Exception e) {
            LOGGER.error("dump es data fail", e);
        }
    }

    private static String[] propertiesKey = {"bucket", "region", "endpoint", "access_key", "secret_key"};

    /**
     * 新建一个 s3 repository
     *
     * @param properties-需要包含bucket, regoin, endpoint, accessKey, secretKey 具体参数的意义请参考 es 文档
     * @param repo                   repository 的名字
     * @return 返回结果
     */
    public boolean createS3Repository(String repo, Properties properties) {
        JsonObject settings = new JsonObject();
        for (String key : propertiesKey) {
            if (properties.containsKey(key))
                settings.addProperty(key, properties.getProperty(key));
        }
        JsonObject repository = new JsonObject();
        repository.addProperty("type", "s3");
        repository.add("settings", settings);
        String url = String.format("%s/_snapshot/%s", host, repo);
        HttpPut httpPut = new HttpPut(url);
        httpPut.setConfig(requestConfig);
        httpPut.setEntity(new ByteArrayEntity(repository.toString().getBytes()));
        try {
            HttpResponse response = client.execute(httpPut);
            if (200 == response.getStatusLine().getStatusCode())
                return true;
            LOGGER.error(getEntity(response));
            return false;
        } catch (IOException e) {
            LOGGER.error("create s3 repo fail", e);
        }
        return false;
    }

    /**
     * 判断repo是否存在
     *
     * @param repo repo名称
     * @return 返回结果
     */
    public boolean checkRepositoryExist(String repo) {
        String url = String.format("%s/_snapshot/%s", host, repo);
        HttpResponse response = null;
        try {
            response = client.execute(getMethod(url));
            if (200 == response.getStatusLine().getStatusCode())
                return true;
            return false;
        } catch (IOException e) {
            LOGGER.error("", e);
        } finally {
            HttpClientUtils.closeQuietly(response);
        }
        return false;
    }

    /**
     * 在某个 repository 下生成 snapshot
     *
     * @param repositoryName
     * @param snapshotName
     * @param config
     */
    public boolean createSnapshot(String repositoryName, String snapshotName, JsonObject config) {
        String url = String.format("%s/_snapshot/%s/%s", host, repositoryName, snapshotName);
        HttpPut httpPut = new HttpPut(url);
        httpPut.setConfig(requestConfig);
        httpPut.setEntity(new ByteArrayEntity(config.toString().getBytes()));
        try {
            client.execute(httpPut);
            return true;
        } catch (IOException e) {
            LOGGER.error("", e);
        }
        return false;
    }

    /**
     * 从某个 repository 下恢复 snapshot
     *
     * @param repositoryName
     * @param snapshotName
     * @param config
     */
    public boolean restoreSnapshot(String repositoryName, String snapshotName, JsonObject config) {
        String url = String.format("%s/_snapshot/%s/%s/_restore", host, repositoryName, snapshotName);
        HttpPost httpPost = new HttpPost(url);
        httpPost.setConfig(requestConfig);
        httpPost.setEntity(new ByteArrayEntity(config.toString().getBytes()));
        HttpResponse response = null;
        try {
            response = client.execute(httpPost);
            return true;
        } catch (IOException e) {
            LOGGER.error("", e);
        } finally {
            HttpClientUtils.closeQuietly(response);
        }
        return false;
    }

    /**
     * reindex api
     *
     * @param entity 例如 {"source": {"index": "a"}, "dest": {"index": "b"}}
     * @return 返回结果
     */
    public String reindex(JsonObject entity) {
        HttpPost httpPost = new HttpPost(host + "/_reindex?wait_for_completion=false");
        httpPost.setConfig(requestConfig);
        httpPost.setEntity(new ByteArrayEntity(entity.toString().getBytes()));
        try {
            HttpResponse response = client.execute(httpPost);
            return new String(FileUtil.stream2byte(response.getEntity().getContent()));
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        return null;
    }

    /**
     * update by query api
     *
     * @param index 支持多个 index, 用逗号分隔
     * @param type  支持多个 type, 用逗号分隔
     * @return 返回结果
     */
    public String updataByQuery(String index, String type, JsonObject query) {
        return updataByQuery(index, type, true, 1000, 1, true, query);
    }

    public String updataByQuery(String index, String type, SearchRequest searchRequest) {
        try {
            return updataByQuery(index, type, true, 1000, 1, true, JsonUtil.toJsonMap(searchRequest.toJson()));
        } catch (Exception e) {
            LOGGER.error("format search query error", e);
            return null;
        }
    }

    /**
     * update by query api
     *
     * @param index            支持多个 index, 用逗号分隔
     * @param type             支持多个 type, 用逗号分隔
     * @param proceedConflicts 遇到版本冲突是否更新
     * @param scrollSize       不设置默认是 1000
     * @param slices           并行 scroll 数量
     * @return 返回结果
     */
    public String updataByQuery(String index, String type, boolean proceedConflicts, int scrollSize, int slices, boolean wait, JsonObject query) {
        return xxByQuery("update", index, type, proceedConflicts, scrollSize, slices, wait, query);
    }

    public String updataByQuery(String index, String type, boolean proceedConflicts, int scrollSize, int slices, boolean wait, SearchRequest searchRequest) {
        try {
            return xxByQuery("update", index, type, proceedConflicts, scrollSize, slices, wait, JsonUtil.toJsonMap(searchRequest.toJson()));
        } catch (Exception e) {
            LOGGER.error("format search query error", e);
        }
        return null;
    }

    /**
     * delete by query api
     *
     * @param index 支持多个 index, 用逗号分隔
     * @param type  支持多个 type, 用逗号分隔
     * @return 返回结果
     */
    public String deleteByQuery(String index, String type, JsonObject query) {
        return deleteByQuery(index, type, true, 1000, 1, true, query);
    }

    public String deleteByQuery(String index, String type, SearchRequest searchRequest) {
        try {
            return deleteByQuery(index, type, true, 1000, 1, true, JsonUtil.toJsonMap(searchRequest.toJson()));
        } catch (Exception e) {
            LOGGER.error("format search query error", e);
        }
        return null;
    }

    /**
     * delete by query api
     *
     * @param index            支持多个 index, 用逗号分隔
     * @param type             支持多个 type, 用逗号分隔
     * @param proceedConflicts 遇到版本冲突是否更新
     * @param scrollSize       不设置默认是 1000
     * @param slices           并行 scroll 数量
     * @return 返回结果
     */
    public String deleteByQuery(String index, String type, boolean proceedConflicts, int scrollSize, int slices, boolean wait, JsonObject query) {
        return xxByQuery("delete", index, type, proceedConflicts, scrollSize, slices, wait, query);
    }

    public String deleteByQuery(String index, String type, boolean proceedConflicts, int scrollSize, int slices, boolean wait, SearchRequest searchRequest) {
        try {
            return xxByQuery("delete", index, type, true, 1000, 1, wait, JsonUtil.toJsonMap(searchRequest.toJson()));
        } catch (Exception e) {
            LOGGER.error("format search query error", e);
        }
        return null;
    }

    private String xxByQuery(String action, String index, String type, boolean proceedConflicts, int scrollSize,
                             int slices, boolean wait, JsonObject query) {
        if (action == null || action.isEmpty()) {
            throw new RuntimeException("action is required");
        }
        if (action.equals("update")) {
            action = "_update_by_query";
        } else if (action.equals("delete")) {
            action = "_delete_by_query";
        } else {
            throw new RuntimeException("action is neither update or delete");
        }
        if (index == null || index.trim().isEmpty()) {
            throw new RuntimeException("update by Query must contains index");
        }
        String url = null;
        if (null == type || type.trim().isEmpty())
            url = String.format(getIndex, host, index);
        else
            url = String.format(getIndexType, host, index, type);
        List<String> parameters = new LinkedList<>();
        if (proceedConflicts) {
            parameters.add("conflicts=proceed");
        }
        if (scrollSize != 1000) {
            parameters.add("scroll_size=" + scrollSize);
        }
        if (slices != 1) {
            parameters.add("slices=" + slices);
        }
        if (!wait) {
            parameters.add("wait_for_completion=false");
        }
        if (parameters.isEmpty()) {
            url += action;
        } else {
            url += action + "?" + StringUtil.joinString(parameters.toArray(new String[parameters.size()]), "&", null, null);
        }
        HttpPost httpPost = new HttpPost(url);
        httpPost.setConfig(requestConfig);
        httpPost.setEntity(new ByteArrayEntity(query.toString().getBytes()));
        try {
            HttpResponse response = client.execute(httpPost);
            return new String(FileUtil.stream2byte(response.getEntity().getContent()));
        } catch (Exception e) {
            LOGGER.error("", e);
        }
        return null;
    }

    /**
     * 将 jsonObject 组成 jsonArray
     *
     * @param jObjects
     * @return 返回结果
     */
    public JsonArray jObjectMakeupJArray(JsonObject... jObjects) {
        JsonArray jArray = new JsonArray();
        for (JsonObject jObject : jObjects) {
            jArray.add(jObject);
        }
        return jArray;
    }

    /**
     * 将 String 组成 jsonArray
     *
     * @param strings
     * @return 返回结果
     */
    public JsonArray stringMakeupJArray(String... strings) {
        JsonArray jArray = new JsonArray();
        for (String str : strings) {
            jArray.add(str);
        }
        return jArray;
    }


    /**
     * {a,b}
     * 用中括号和逗号进行包装, 用于聚合返回的结果中带上多个源字段，或者搜索多个字段
     * ["a", "b"]
     *
     * @param array
     * @return 返回结果
     */
    public static String joinStringForMultiFields(String[] array) {
        for (int i = 0; i < array.length; i++) {
            array[i] = "\"" + array[i] + "\"";
        }
        return StringUtil.joinString(array, ",", "[", "]");
    }

    /**
     * 把某个字段的多个可匹配值作为或条件子查询
     *
     * @param array
     * @return 返回结果
     */
    public static String joinORSubQuery(String[] array) {
        for (int i = 0; i < array.length; i++) {
            if (array[i].contains(" "))
                array[i] = "\"" + array[i] + "\"";
        }
        return StringUtil.joinString(array, " OR ", "(", ")");
    }

}
