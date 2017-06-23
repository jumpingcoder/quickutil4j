package com.quickutil.platform;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

/**
 * @author shijie.ruan
 * 使用连接池
 */
public class ElasticUtil {
	private static final String[] replaceArray = { "\t", "\n" };
	private static final String indexBulkHead = "{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_id\":\"%s\"}}\n";
	private static final String successLog = "success--/%s/%s/%s--%s";
	private static final String successBatchLog = "success--%s--%s";
	private static final String failLog = "fail--/%s/%s/%s--%s";
	private static final String failreason = "failreason--";
	private static final String failException = "failreason--exception";
	private static final String bulkAPI = "%s/_bulk";

	private static final String hostFormat = "%s/";
	private static final String hostIndexFormat = "%s/%s/";
	private static final String hostIndexTypeFormat = "%s/%s/%s/";

	private final String host;
	private final String name;

	public final HttpClient client;

	private static RequestConfig requestConfig = RequestConfig.custom()
			.setConnectionRequestTimeout(60000)
			.setConnectTimeout(60000)
			.setSocketTimeout(60000).build();

	private static PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();

	private static HttpRequestRetryHandler httpRequestRetryHandler = new HttpRequestRetryHandler() {
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

	static {
		cm.setMaxTotal(50);
		cm.setDefaultMaxPerRoute(50);
	}
	public ElasticUtil(String host, String name) {
		this.host = host;
		if (null == name) {
			this.name = StringUtil.getRandomString(10);
		} else {
			this.name = name;
		}

		cm.setMaxPerRoute(new HttpRoute(new HttpHost(host)), 50);

		this.client =  HttpClients.custom()
				.setConnectionManager(cm)
				.setRetryHandler(httpRequestRetryHandler).build();
	}

	public String getName() {
		return this.name;
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
	 * 使用id查询数据
	 *
	 * @param index-ES的index
	 * @param type-ES的type
	 * @param id-ES的id
	 * @return
	 */
	public String selectById(String index, String type, String id) {
		String url = String.format("%s/%s/%s/%s/_source", host, index, type, id);
		try {
			HttpResponse response = client.execute(getMethod(url));
			if (response == null)
				return null;
			if (response.getStatusLine().getStatusCode() == 200)
				return getEntity(response);
			else
				return null;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 使用模板查询数据
	 *
	 * @param requestUrl-请求的ES的URL
	 * @param template-查询的DSL模板
	 * @param paramMap-参数
	 * @return
	 */
	public String selectByTemplate(String requestUrl, String template, Map<String, Object> paramMap) {
		try {
			for (String paramKey : paramMap.keySet()) {
				String json = JsonUtil.toJson(paramMap.get(paramKey));
				template = template.replace("@" + paramKey, json);
			}
			for (String replace : replaceArray) {
				template = template.replace(replace, "");
			}
			String result = selectByJson(requestUrl, template);
			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 使用json查询数据
	 *
	 * @param url-请求的ES的URL,除去 Host
	 * @param entity-查询的DSL
	 * @return
	 */
	public String selectByJson(String url, String entity) {
		try {
			HttpResponse response = client.execute(postMethod(url, entity));
			if (null == response) { return null; }
			String fLog = failreason + "with url:" + url + "\n";
			handleResp(response, null, fLog);
			return getEntity(response);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 写入数据
	 *
	 * @param index-ES的index
	 * @param type-ES的type
	 * @param id-ES的id
	 * @param source-写入的内容
	 * @return
	 */
	public boolean insert(String index, String type, String id, String source) {
		String url = null;
		try {
			long start = System.currentTimeMillis();
			url = String.format("%s/%s/%s/%s", host, index, type, id).replace(" ", "");
			HttpResponse response = client.execute(postMethod(url, source));
			String sLog = String.format(successLog, index, type, id, System.currentTimeMillis() - start);
			String fLog = failreason + getEntity(response)+ "\n" + url + source + "\n";
			handleResp(response, sLog, fLog);
		} catch (Exception e) {
			System.out.println(failException + "\n" + url + source + "\n");
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 更新数据
	 *
	 * @param index-ES的index
	 * @param type-ES的type
	 * @param id-ES的id
	 * @param source-更新的内容
	 * @param isupsert-是否是upsert
	 * @return
	 */
	public boolean update(String index, String type, String id, Object source, boolean isupsert) {
		String sourceStr = null, url = null;
		try {
			url = String.format("/%s/%s/%s/_update", index, type, id).replace(" ", "");
			long start = System.currentTimeMillis();
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("doc", source);
			map.put("doc_as_upsert", isupsert);
			sourceStr = JsonUtil.toJson(map);
			HttpResponse response = client.execute(postMethod(url, sourceStr));
			String sLog = String.format(successLog, index, type, id, System.currentTimeMillis() - start);
			String fLog = failreason + getEntity(response) + "\n" + url + sourceStr + "\n";
			handleResp(response, sLog, fLog);
		} catch (Exception e) {
			System.out.println(failException + "\n" + url + sourceStr + "\n");
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 缓存队列式批量写入，每秒写入一次
	 *
	 * @param index-ES的index
	 * @param type-ES的type
	 * @param id-ES的id
	 * @param source-写入的内容
	 * @return
	 */
	private long lasttime = 0;
	private StringBuffer sb = new StringBuffer();
	private int count = 0;

	public boolean bulkInsertBuffer(String index, String type, String id, String source) {
		long start = System.currentTimeMillis();
		String entity = null;
		try {
			sb.append(String.format(indexBulkHead, index, type, id) + source + "\n");
			count++;
			if (System.currentTimeMillis() - lasttime > 1000) {
				entity = sb.toString();
				lasttime = System.currentTimeMillis();
				sb = new StringBuffer();
				System.out.println("content count:" + count);
				count = 0;
				HttpResponse response = client.execute(postMethod(String.format(bulkAPI, host), entity));
				String sLog = String.format(successLog, index, type, id, System.currentTimeMillis() - start);
				String fLog = failreason + getEntity(response) + "\n" + entity;
				handleResp(response, sLog, fLog);
			} else {
				return true;
			}
		} catch (Exception e) {
			System.out.println(failException + "\n" + entity);
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 批量写入
	 *
	 * @param index-ES的index
	 * @param type-ES的type
	 * @param source-写入的内容，key为id，value为source
	 * @return
	 */
	public String bulkInsert(String index, String type, Map<String, String> source) {
		long start = System.currentTimeMillis();
		String entity = null;
		try {
			StringBuilder bulk = new StringBuilder();
			Iterator it = source.keySet().iterator();
			while (it.hasNext()) {
				String key = (String)it.next();
				bulk.append(String.format(indexBulkHead, index, type, key) + source.get(key) + "\n");
			}
			entity = bulk.toString();
			HttpResponse response = client.execute(postMethod(String.format(bulkAPI, host), entity));
			String content = getEntity(response);
			String sLog = String.format(successBatchLog, source.size(), System.currentTimeMillis() - start);
			String fLog = failreason + content + "\n" + entity;
			if (!handleResp(response, sLog, fLog))
				return null;
			return content;
		} catch (Exception e) {
			System.out.println(failException);
			System.out.println(entity);
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 批量更新
	 *
	 * @param index-ES的index
	 * @param type-ES的type
	 * @param source-更新的内容
	 * @param isUpsert-true 为追加字段, FALSE 为覆盖文档
	 * @return
	 */
	public boolean bulkUpdate(String index, String type, Map<String, Object> source, boolean isUpsert) {
		long start = System.currentTimeMillis();
		String updateBulkHead = "{\"update\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_id\":\"%s\"}}\n";
		String entity = null;
		try {
			StringBuilder bulk = new StringBuilder();
			for (String key : source.keySet()) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("doc", source.get(key));
				map.put("doc_as_upsert",  isUpsert);
				bulk.append(String.format(updateBulkHead, index, type, key) + JsonUtil.toJson(map) + "\n");
			}
			entity = bulk.toString();

			HttpResponse response = client.execute(postMethod(String.format(bulkAPI, host), entity));
			String sLog = String.format(successBatchLog, source.size(), System.currentTimeMillis() - start);
			String fLog = failreason + getEntity(response) + "\n" + entity;
			handleResp(response, sLog, fLog);
		} catch (Exception e) {
			System.out.println(failException + "\n" + entity);
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 批量更新
	 *
	 * @param index-ES的index
	 * @param type-ES的type
	 * @param entity-
	 * @return
	 */
	public String mSearch(String index, String type, String entity) {
		String url = null;
		try {
			url = null == type ? String.format(hostIndexFormat, host, index) + "_msearch" :
					String.format(hostIndexTypeFormat, host, index, type) + "_msearch";
			HttpResponse response = client.execute(postMethod(url, entity));
			String content = getEntity(response);
			String fLog = failreason + content + "\n" + url;
			handleResp(response, null, fLog);
			return content;
		} catch (Exception e) {
			System.out.println(failException + "\n" + url);
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 正则获取所有相关的 index 名字
	 * @return
	 */
	public String[] getIndexName(String indexNameReg) {
		String url = host + "/_cat/indices/" + indexNameReg;
		try {
			HttpResponse response = client.execute(getMethod(host + "/_cat/indices/" + indexNameReg));
			if (!isSuccess(response)) {
				System.out.println("get index name error, with response: " + getEntity(response));
				return null;
			}
			String[] indicesStats = getEntity(response).split("\\n");
			String[] indicesNames = new String[indicesStats.length];
			// 返回的 index 信息形如"green open activation-2014 5 1 2944 4522 2.6mb 1.3mb ",需要获取名字
			for(int i = 0; i < indicesStats.length; i++) {
				String indexName = indicesStats[i].split("\\s+")[2];
				indicesNames[i] = indexName;
			}
			return indicesNames;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 获取首次 scroll 查询
	 * @param index
	 * @return
	 */
	public String getFirstScrollSearch(String index, String query) {
		return getFirstScrollSearch(index, null, query);
	}

	/**
	 * 获取首次 scroll 查询
	 * @param index
	 * @param type
	 * @return
	 */
	public String getFirstScrollSearch(String index, String type, String query) {
		long start = System.currentTimeMillis();
		String url = null == type ? String.format(hostIndexFormat + "_search?scroll=5m", host, index) :
				String.format(hostIndexTypeFormat + "_search?scroll=5m", host, index, type);
		if (null == query) {
			query = "";
		}
		try {
			HttpResponse response = client.execute(postMethod(url, query));
			String content = getEntity(response);
			String sLog = String.format("success--scroll--%s", System.currentTimeMillis() - start);
			String fLog = failreason + content + "\n" + query;
			if (!handleResp(response, sLog, fLog)) {
				return null;
			}
			return content;
		} catch (Exception e) {
			System.out.println(failException + "\n" + query);
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 获取后续的 scroll 搜索的 url
	 * @param scrollId
	 * @return
	 */
	public String getScrollSearch(String scrollId) {
		String url = String.format("%s/_search/scroll?scroll=5m&scroll_id=%s", host, scrollId);
		long start = System.currentTimeMillis();
		try {
			HttpResponse response = client.execute(getMethod(url));
			String content = getEntity(response);
			String sLog = String.format("success--scroll--%s", System.currentTimeMillis() - start);
			String fLog = failreason + content;
			if (!handleResp(response, sLog, fLog)) {
				return null;
			}
			return content;
		} catch (Exception e) {
			System.out.println(failException);
			e.printStackTrace();
		}
		return null;
	}

	private String getEntity(HttpResponse response) throws IOException {
		return EntityUtils.toString(response.getEntity());
	}

	/**
	 * 替换 json 文件中的占位符
	 * 拷贝自 ElasticsearchUtil, 由于 ElasticsearchUtil 不提供返回只替换完占位符后的 json, 所以拷贝到这里使用
	 * @param template
	 * @param paramMap
	 * @return
	 */
	public static String formatTemplate(String template, Map<String, Object> paramMap) {
		try {
			for (String paramKey : paramMap.keySet()) {
				String json = JsonUtil.toJson(paramMap.get(paramKey));
				template = template.replace("@" + paramKey, json);
			}
			for (String replace : replaceArray) {
				template = template.replace(replace, "");
			}
			return template;
		} catch (Exception ex) {
			ex.printStackTrace();
			return null;
		}
	}

	/**
	 * 查看 index 是否存在
	 * @param index
	 * @return
	 */
	public boolean checkIndexExist(String index) {
		String getIndexExistUrl = String.format(hostIndexFormat, host, index);
		try {
			HttpResponse response = client.execute(getMethod(getIndexExistUrl));
			if (404 == response.getStatusLine().getStatusCode()) {
				return false;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * 创建索引
	 * @param index
	 * @param mappings
	 * @return
	 */
	public boolean createIndex(String index, String mappings) {
		String createIndexUrl = String.format(hostIndexFormat, host, index);
		try {
			HttpResponse response = HttpUtil.httpPut(createIndexUrl, mappings.getBytes());
			if (200 != response.getStatusLine().getStatusCode()) {
				System.out.println("create index fail, response: " + getEntity(response));
				return false;
			}
			return true;
		} catch (Exception var3) {
			var3.printStackTrace();
			return false;
		}
	}

	/**
	 * 获取 index 的 mapping
	 * @param index
	 * @return
	 */
	public String getMapping(String index) {
		String getMappingsUrl = String.format(hostIndexFormat + "_mapping", host, index);
		try {
			HttpResponse response = HttpUtil.httpGet(getMappingsUrl);
			return new String(FileUtil.stream2byte(response.getEntity().getContent()));
		} catch (Exception var3) {
			var3.printStackTrace();
			return null;
		}
	}

	/**
	 * 根据 mapping 文件创建 index
	 * @param mappingsFile
	 */
	public void initIndex(String host, String mappingsFile) {
		JsonArray indexArray = JsonUtil.toJsonArray(
				FileUtil.stream2string(PropertiesUtil.getInputStream(mappingsFile)));
		for(JsonElement e : indexArray) {
			String indexName = e.getAsJsonObject().get("index").getAsString();
			System.out.println("indexName: " + indexName);
			e.getAsJsonObject().remove("index");
			String createIndexJson = e.getAsJsonObject().toString();
			System.out.println(e.getAsJsonObject());
			if (checkIndexExist(indexName)) {
				System.out.println("index: " + indexName + " already exist");
			} else {
				createIndex(indexName, createIndexJson);
			}
		}
	}

	/**
	 * 将 jsonObject 组成 jsonArray
	 * @param jObjects
	 * @return
	 */
	public JsonArray jObjectMakeupJArray(JsonObject... jObjects) {
		JsonArray jArray = new JsonArray();
		for (JsonObject jObject : jObjects) { jArray.add(jObject); }
		return jArray;
	}

	/**
	 * 将 String 组成 jsonArray
	 * @param strings
	 * @return
	 */
	public JsonArray stringMakeupJArray(String ... strings) {
		JsonArray jArray = new JsonArray();
		for (String str : strings) { jArray.add(str); }
		return jArray;
	}

	/**
	 * 查询 es 的结果保存成 csv
	 * @param index
	 * @param type
	 * @param query
	 * @param filePath
	 * @param jObjectToCsvFunc
	 */
	public void dumpESDataToCsv(
			String index,
			String type,
			String query,
			String filePath,
			Function<JsonObject, String> jObjectToCsvFunc) {
		assert(index != null && filePath != null && jObjectToCsvFunc != null);
		if (null == query) {
			query = "{}";
		}
		try {
			File f = new File(filePath);
			if (!f.exists()) {
				f.createNewFile();
			}
			String resp = getFirstScrollSearch(index, type, query);
			if (null == resp && responseHasError(resp)) {
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
					String csvLine = jObjectToCsvFunc.apply(doc);
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
				array = JsonUtil.toJsonMap(resp)
						.getAsJsonObject("hits").getAsJsonArray("hits");
				System.out.println("index: " + index + TimeUtil.printProgress((double)count/total));
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	private boolean isSuccess(HttpResponse response) {
		return 200 == response.getStatusLine().getStatusCode()
				|| 201 == response.getStatusLine().getStatusCode();
	}

	private boolean handleResp(HttpResponse resp, String successLog, String failLog) {
		if (isSuccess(resp)) {
			if (null != successLog) {
				System.out.println(successLog);
			}
			return true;
		} else {
			System.out.println(failLog);
			return false;
		}
	}

	/**
	 * 判断 es 返回的 response 是否包含错误
	 * 批量接口错误返回 errors 的值为 true, 单条接口错误返回 error
	 * @param response
	 * @return
	 */
	public boolean responseHasError(JsonObject response) {
		if (response.has("error")) {
			return true;
		}
		if (response.has("errors") && "true".equals(response.get("errors").toString())) {
			return true;
		}
		return false;
	}

	/**
	 * 由于批量操作时 response 很大,将字符串变成 json 代价大,所以使用查看前 100 个字符中是否包含 error,errors,
	 * 如果没有则返回 false, 有则进一步解析 json 看是否值真的有 error, 还是由于内容中包含 error 关键词引起的
	 * @param response
	 * @return
	 */
	public boolean responseHasError(String response) {
		String prefix = response.substring(0, 100);
		if (prefix.contains("error") ||
				(prefix.contains("errors") && prefix.contains("true"))) {
			return responseHasError(JsonUtil.toJsonMap(response));
		} else {
			return false;
		}
	}

	public static JsonArray getError(JsonObject response) {
		if (response.has("error")) {
			JsonArray result = new JsonArray();
			result.add(response);
			return result;
		}
		if (response.has("errors") && "true".equals(response.get("errors").toString())) {
			JsonArray result = new JsonArray();
			for (JsonElement elem: response.getAsJsonArray("items")) {
				JsonObject item = elem.getAsJsonObject();
				if (item.toString().contains("error")) {
					System.out.println("get a error:" + item);
					result.add(item);
				}
			}
			return result;
		}
		return new JsonArray();
	}

	public static JsonArray getError(String response) {
		return getError(JsonUtil.toJsonMap(response));
	}
}
