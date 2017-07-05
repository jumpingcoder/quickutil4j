package com.quickutil.platform;

import static com.quickutil.platform.BulkResponse.itemFalse;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.quickutil.platform.aggs.AvgAggs;
import com.quickutil.platform.aggs.CardinalityAggs;
import com.quickutil.platform.aggs.DateHistogramAggs;
import com.quickutil.platform.aggs.DateHistogramAggs.Interval;
import com.quickutil.platform.aggs.DateRangeAggs;
import com.quickutil.platform.aggs.ExtendedStatsAggs;
import com.quickutil.platform.aggs.HistogramAggs;
import com.quickutil.platform.aggs.Order;
import com.quickutil.platform.aggs.Order.Sort;
import com.quickutil.platform.aggs.Range;
import com.quickutil.platform.aggs.RangeAggs;
import com.quickutil.platform.aggs.SumAggs;
import com.quickutil.platform.aggs.TermsAggs;
import com.quickutil.platform.query.BoolQuery;
import com.quickutil.platform.query.MatchAllQuery;
import com.quickutil.platform.query.QueryStringQuery;
import com.quickutil.platform.query.QueryStringQuery.Operator;
import com.quickutil.platform.query.RangeQuery;
import com.quickutil.platform.query.ScriptQuery;
import com.quickutil.platform.query.TermQuery;
import com.quickutil.platform.query.WildcardQuery;
import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
 */
public class ElasticUtil {
	private static final String[] replaceArray = { "\t", "\n" };

	private static final String hostFormat = "%s/";
	private static final String hostIndexFormat = "%s/%s/";
	private static final String hostIndexTypeFormat = "%s/%s/%s/";

	private final String host;
	private final Version version;

	public final HttpClient client;

	private static RequestConfig requestConfig = RequestConfig.custom()
			.setConnectionRequestTimeout(60000).setConnectTimeout(60000).setSocketTimeout(60000).build();

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
			if (!(request instanceof HttpEntityEnclosingRequest)) { // 如果请求是幂等的，就再次尝试
				return true;
			}
			return false;
		}
	};

	static {
		cm.setMaxTotal(50); cm.setDefaultMaxPerRoute(50);
	}
	public ElasticUtil(String host, Version version) {
		this.host = host;
		this.version = version;
		cm.setMaxPerRoute(new HttpRoute(new HttpHost(host)), 50);
		this.client =  HttpClients.custom().setConnectionManager(cm)
				.setRetryHandler(httpRequestRetryHandler).build();
	}

	public static void main(String[] args) throws FormatQueryException {
		ElasticUtil elasticUtil = new ElasticUtil("http://10.10.3.166:9200", Version.es5);
//		JsonObject a = new JsonObject();
//		a.addProperty("len", 100);
//		JsonObject b = new JsonObject();
//		b.addProperty("len", 101);
//		JsonObject c = new JsonObject();
//		c.addProperty("len", 102);
//		JsonObject d = new JsonObject();
//		d.addProperty("len", 103);
//		Map<String, JsonObject> sources = new HashMap<>();
//		sources.put("300", a);
//		sources.put("301", b);
//		sources.put("302", c);
//		sources.put("303", d);
//		BulkResponse bulkResponse = elasticUtil.bulkUpdateByScript("test1", "logs", sources, "test", true);
//		if (bulkResponse.hasFail()) {
//			for (int i = 0; i < bulkResponse.getResponseItem().length; i++) {
//				if (0 != bulkResponse.getResponseItem()[i]) {
//					System.out.println(i + " request has error");
//				}
//			}
//		}
		MatchAllQuery matchAllQuery = new MatchAllQuery();
		BoolQuery boolQuery = new BoolQuery().addMustNotQuery (matchAllQuery);
		String stringQuery = "100 101 102";
		QueryStringQuery queryStringQuery = new QueryStringQuery(stringQuery).setDefaultField("len").setDefaultOperator(
				Operator.OR);
		RangeQuery rangeQuery = new RangeQuery("postDate").setGte("20170626").setFormat("yyyyMMdd").setTimeZone("-12:00");
		ScriptQuery scriptQuery = new ScriptQuery("test_query");
		JsonObject params = new JsonObject();
		params.addProperty("len1", 101);
		scriptQuery.setParams(params);
		TermQuery termQuery = new TermQuery("alias", "jie");
		WildcardQuery wildcardQuery = new WildcardQuery("alias", "ji*");
		SearchRequest searchRequest = new SearchRequest(queryStringQuery);

		AvgAggs avgAggs = new AvgAggs("lenAvg", "name");
		CardinalityAggs cardinalityAggs = new CardinalityAggs("distinct", "postDate", false);
		CardinalityAggs cardinalityAggsFor5 = new CardinalityAggs("distinct", "name", true);
		cardinalityAggsFor5.setMissing("dis");
		CardinalityAggs cardinalityAggsFor2 = new CardinalityAggs("distinct", "name", false);
		cardinalityAggsFor2.setMissing("dis");
		DateHistogramAggs dateHistogramAggs = new DateHistogramAggs("dateHis", "postDate", Interval.day);
		DateRangeAggs dateRangeAggs = new DateRangeAggs("dataRange", "postDate");
		dateRangeAggs.addRange(new Range("now-1M/M", "now", "haha"));
		dateRangeAggs.addRange(new Range().setFrom("now-10M/M"));
		dateRangeAggs.setKeyed(true);
		ExtendedStatsAggs extendedStatsAggs = new ExtendedStatsAggs("ext_stats", "len");
		extendedStatsAggs.setMissing(102.0);
		HistogramAggs histogramAggs = new HistogramAggs("his", "len", 10);
		histogramAggs.setMinDocCount(1);
		RangeAggs rangeAggs = new RangeAggs("lenRange", "len");
		rangeAggs.addRange(new Range("0", "100"));
		rangeAggs.addRange(new Range().setFrom("101"));
		TermsAggs termsAggsFor5 = new TermsAggs("termsa", "name", true);
		termsAggsFor5.setOrder(new Order("_count"));
		TermsAggs termsAggsFor2 = new TermsAggs("termsa", "name", false);
		termsAggsFor2.setOrder(new Order("_count"));



		String[] sumAggsFields = { "pageviews","sessions","newUsers" };
		String[] avgAggsFields = { "pageviewsPerSession","bounceRate" };
		for (String sumAggsField: sumAggsFields) {
			termsAggsFor2.addSubAggs(new SumAggs(sumAggsField, sumAggsField));
		}
		for (String avgAggsField: avgAggsFields) {
			termsAggsFor2.addSubAggs(new AvgAggs(avgAggsField, avgAggsField));
		}
		searchRequest = new SearchRequest(matchAllQuery, termsAggsFor2);
		System.out.println(termsAggsFor2.getSubAggsSize());
		System.out.println(searchRequest.toJson());

		searchRequest.setSize(0);
		Order order = new Order("len", Sort.asc);
		searchRequest.addSort(order);
		String response = elasticUtil.search("test", "logs", searchRequest);

		System.out.println(response);
	}

	public Version getVersion() { return this.version; }

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
	 * 有任何错误返回都返回空
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
			else if (200 != response.getStatusLine().getStatusCode()) {
				System.out.format("get url: %s fail. with response: \n%s", url, getEntity(response));
				return null;
			} else {
				return getEntity(response);
			}
		} catch (Exception e) {
			e.printStackTrace();
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
	 * @return
	 */
	public boolean insert(String index, String type, String id, JsonObject source) {
		String url = null;
		try {
			if (null == index || null == type || id == null) {
				System.out.println("[index], [type], [id] must be not null"); return false;
			}
			url = String.format("%s/%s/%s/%s", host, index, type, id).replace(" ", "");
			String sourceString = (null == source ? "{}" : JsonUtil.toJson(source));
			HttpResponse response = client.execute(postMethod(url, sourceString));
			// index response status can be 201 or 200, 201 indicate first create, 200 indicate update
			if (200 == response.getStatusLine().getStatusCode() ||
					201 == response.getStatusLine().getStatusCode()) {
				return true;
			} else {
				System.out.println("fail on url: " + url + "\nwith source: " + sourceString +
						"\nresponse: " + getEntity(response));
				return false;
			}
		} catch (Exception e) {
			System.out.println("fail on url: " + url);
			e.printStackTrace();
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
	 * @param isUpsert-true 表示如果文档不存在则插入,false 时如果不存在则不插入
	 * @return
	 */
	public boolean update(String index, String type, String id, JsonObject source, boolean isUpsert) {
		String sourceString = null, url = null;
		try {
			if (null == index || null == type || id == null) {
				System.out.println("[index], [type], [id] must be not null"); return false;
			}
			url = String.format("%s/%s/%s/%s/_update", host, index, type, id).replace(" ", "");
			Map<String, Object> map = new HashMap<String, Object>();
			if (null == source) { source = new JsonObject(); }
			map.put("doc", source);
			map.put("doc_as_upsert", isUpsert);
			sourceString = JsonUtil.toJson(map);
			HttpResponse response = client.execute(postMethod(url, sourceString));
			if (200 == response.getStatusLine().getStatusCode() ||
					201 == response.getStatusLine().getStatusCode()) {
				return true;
			} else if (404 == response.getStatusLine().getStatusCode()) {
				System.out.format("[%s][%s][%s]: document missing\n", index, type, id);
				return false;
			} else {
				System.out.println("fail on url: " + url + "\nwith source: " + sourceString +
						"\nresponse: " + getEntity(response));
				return false;
			}
		} catch (Exception e) {
			System.out.println("fail on url: " + url + "\n" + sourceString + "\n");
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 发起批量请求, 支持 index, create, update, delete(被屏蔽), 其中 index, update, create 下一行都需要是
	 * 文档的内容, delete 下一行不能是文档的内容
	 * @param url
	 * @param entity
	 * @return
	 */
	private BulkResponse bulk(String url, String entity, int size) {
		byte[] responseItems = new byte[size];
		try {
			HttpResponse response = client.execute(postMethod(url, entity));
			if (200 != response.getStatusLine().getStatusCode()) {
				System.out.println("bulk insert error, with resposne: " + getEntity(response));
				return getAllFlaseResponse(size);
			} else {
				JsonObject responseObject = JsonUtil.toJsonMap(getEntity(response));
				boolean hasErrors = responseObject.get("errors").getAsBoolean();
				if (!hasErrors) {
					return new BulkResponse(true, responseItems);
				} else {
					System.out.println(responseObject);
					JsonArray responseArray = responseObject.getAsJsonArray("items");
					for (int i = 0; i < responseArray.size(); i++) {
						int status = getBulkItemStatus(responseArray.get(i).getAsJsonObject());
						if (200 != status && 201 != status) { responseItems[i] = itemFalse; }
					}
					return new BulkResponse(false, responseItems);
				}
			}
		} catch (Exception e) {
			System.out.println("bulk operation error for url: " + url);
			e.printStackTrace();
		  return getAllFlaseResponse(size);
		}
	}

	private int getBulkItemStatus(JsonObject item) {
		if (item.has("create")) {
			return item.getAsJsonObject("create").get("status").getAsInt();
		} else if (item.has("index")) {
			return item.getAsJsonObject("index").get("status").getAsInt();
		} else if (item.has("update")) {
			return item.getAsJsonObject("update").get("status").getAsInt();
		} else if (item.has("delete")) {
			return item.getAsJsonObject("delete").get("status").getAsInt();
		} else {
			System.out.println("unknown bulk action for response item: " + item);
			return 400;
		}
	}

	private BulkResponse getAllFlaseResponse(int size) {
		byte[] responseItems = new byte[size];
		Arrays.fill(responseItems, itemFalse);
		return new BulkResponse(false, responseItems);
	}

	/**
	 * 缓存队列式批量写入，每秒写入一次, 时间不到1s, 返回写入成功,其实在程序缓存中,
	 * 这时候如果程序崩溃或者重启会丢失这一秒的数据
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
		String entity = null;
		try {
			String actionAndMeta = "{\"index\":{\"_index\":\"%s\",\"_type\":\"%s\",\"_id\":\"%s\"}}\n";
			sb.append(String.format(actionAndMeta, index, type, id) + source + "\n");
			count++;
			if (System.currentTimeMillis() - lasttime > 1000) {
				entity = sb.toString();
				lasttime = System.currentTimeMillis();
				sb = new StringBuffer();
				System.out.println("content count:" + count);
				count = 0;
				BulkResponse response = bulk(String.format("%s/_bulk", host), entity, count);
				if (response.hasFail()) {
					// 如果请求失败,这里打印了所有的请求,有可能量很大, TODO:@massage 如果不是用它来记录客户端上报的数据的话我想删掉这行打印
					System.out.println("bulk insert buffer fail, the source is:\n" + entity);
					return false;
				} else {
					return true;
				}
			} else {
				return true;
			}
		} catch (Exception e) {
			System.out.println("bulk insert buffer fail, the source is:\n" + entity);
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 批量写入,写入同一个 index 和 type
	 *
	 * @param index-ES的index
	 * @param type-ES的type
	 * @param source-写入的内容，key为id，value为source
	 * @return
	 */
	public BulkResponse bulkInsert(String index, String type, Map<String, String> source) {
		String idFormat = "{\"index\": {\"_id\": \"%s\"}}\n";
		if (null == index || null == type) {
			System.out.println("bulk insert into one index and type must specify index and type");
			return getAllFlaseResponse(source.size());
		}
		StringBuilder entity = new StringBuilder();
		for (String id: source.keySet()) {
			entity.append(String.format(idFormat, id)).append(source.get(id)).append("\n");
		}
		String urlFormat = "%s/%s/%s/_bulk";
		return bulk(String.format(urlFormat, host, index, type), entity.toString(), source.size());
	}

	/**
	 * 批量更新,同一个 index 和 type
	 *
	 * @param index-ES的index
	 * @param type-ES的type
	 * @param source-更新的内容, key为id，value为source
	 * @param upsert-文档不存在时插入,其实控制粒度是对于每一个文档的,但是这里为了方便输入,粒度为同一次批量的文档
	 * @return
	 */
	public BulkResponse bulkUpdate(String index, String type, Map<String, JsonObject> source,
			boolean upsert) {
		String idFormat = "{\"update\": {\"_id\": \"%s\"}}\n";
		if (null == index || null == type) {
			System.out.println("bulk update into one index and type must specify index and type");
			return getAllFlaseResponse(source.size());
		}
		StringBuilder entity = new StringBuilder();
		for (String id : source.keySet()) {
			JsonObject item = new JsonObject();
			item.add("doc", source.get(id));
			if (upsert) { item.addProperty("doc_as_upsert", true); }
			entity.append(String.format(idFormat, id)).append(item).append("\n");
		}
		String urlFormat = "%s/%s/%s/_bulk";
		return bulk(String.format(urlFormat, host, index, type), entity.toString(), source.size());
	}

	/**
	 * 批量更新,使用同一个脚本同一个 index 和 type.
	 * @param index-ES的index
	 * @param type-ES的type
	 * @param source-更新的内容, key为id，value为source
	 * @param scriptFile-放在 elasticsearch home 目录下的 config/script 目录下的groovy脚本,
	 * 为了安全,不支持在请求中带上脚本
	 * 之所以是 groovy 脚本,因为 groovy 是 2.x 和 5.x 都支持的
	 * @param upsert-文档不存在时插入,其实控制粒度是对于每一个文档的,但是这里为了方便输入,粒度为同一次批量的文档
	 * @return
	 */
	public BulkResponse bulkUpdateByScript(String index, String type, Map<String, JsonObject> source,
			String scriptFile, boolean upsert) {
		String idFormat = "{\"update\": {\"_id\": \"%s\"}}\n";
		if (null == index || null == type) {
			System.out.println("bulk update into one index and type must specify index and type");
			byte[] responseItems = new byte[source.size()];
			Arrays.fill(responseItems, itemFalse);
			return new BulkResponse(false, responseItems);
		}
		StringBuilder entity = new StringBuilder();
		for (String id : source.keySet()) {
			JsonObject item = new JsonObject();
			JsonObject scriptObject = new JsonObject();
			scriptObject.addProperty("lang", "groovy");
			scriptObject.addProperty("file", scriptFile);
			scriptObject.add("params", source.get(id));
			item.add("script", scriptObject);
			if (upsert) {
				item.add("upsert", source.get(id));
			}
			entity.append(String.format(idFormat, id)).append(item).append("\n");
		}
		String urlFormat = "%s/%s/%s/_bulk";
		return bulk(String.format(urlFormat, host, index, type), entity.toString(), source.size());
	}

	/**
	 * 查询或者聚合请求
	 * @param index ES的index(可以包含*作为通配符)
	 * @param type ES的type(可以为空)
	 * @param searchRequest
	 * @return
	 */
	public String search(String index, String type, SearchRequest searchRequest) {
		try {
		 	String url = null == type ? String.format(hostIndexFormat, host, index) + "_search" :
				  String.format(hostIndexTypeFormat, host, index, type) + "_search";
			System.out.println(searchRequest.toJson());
		 	HttpResponse response = client.execute(postMethod(url, searchRequest.toJson()));
			if (200 != response.getStatusLine().getStatusCode()) {
				System.out.println("search fail on url: " + url + ", response:\n" + getEntity(response));
				return null;
			}
			return getEntity(response);
		} catch (Exception e) {
			System.out.println("format search request fail, pls check");
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 对同一个 index(可以包含*作为通配符) 和 type(可以为空) 进行批量搜索
	 *
	 * @param index-ES的index
	 * @param type-ES的type(可以为空)
	 * @param searches-请求的列表
	 * @return
	 */
	public String mSearch(String index, String type, List<SearchRequest> searches) {
		String url = null;
		try {
			url = null == type ? String.format(hostIndexFormat, host, index) + "_msearch" :
					String.format(hostIndexTypeFormat, host, index, type) + "_msearch";
			StringBuilder entity = new StringBuilder();
			for (SearchRequest searchRequest: searches) {
				entity.append("{}\n"); entity.append(searchRequest.toJson());
			}
			HttpResponse response = client.execute(postMethod(url, entity.toString()));
			if (200 != response.getStatusLine().getStatusCode()) {
				System.out.println("search fail on url: " + url + "with source:\n" + entity.toString());
				return null;
			}
			return getEntity(response);
		} catch (Exception e) {
			System.out.println("search fail on url: " + url);
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 正则获取所有相关的 index 名字
	 * @return
	 */
	public String[] getIndexName(String indexNameReg) {
		try {
			HttpResponse response = client.execute(getMethod(host + "/_cat/indices/" + indexNameReg));
			if (200 != response.getStatusLine().getStatusCode()) {
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

	public String getFirstScrollSearch(String index) {
		return getFirstScrollSearch(index, null, null);
	}

	public String getFirstScrollSearch(String index, SearchRequest searchRequest) {
		return getFirstScrollSearch(index, null, searchRequest);
	}

	/**
	 * 获取首次 scroll 查询
	 * @param index 可以是通配符,不能为空
	 * @param type 可以为空
	 * @param searchRequest 可以为空
	 * @return
	 */
	public String getFirstScrollSearch(String index, String type, SearchRequest searchRequest) {
		String url = null == type ? String.format(hostIndexFormat + "_search?scroll=5m", host, index) :
				String.format(hostIndexTypeFormat + "_search?scroll=5m", host, index, type);
		try {
			String query = (null ==  searchRequest? "" : searchRequest.toJson());
			HttpResponse response = client.execute(postMethod(url, query));
			if (200 != response.getStatusLine().getStatusCode()) {
				System.out.println("fail on scroll search :" + url + "\n response: " + getEntity(response));
				return null;
			}
			return getEntity(response);
		} catch (Exception e) {
			System.out.println("fail on scroll search url: " + url);
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
		try {
			HttpResponse response = client.execute(getMethod(url));
			if (200 != response.getStatusLine().getStatusCode()) {
				System.out.println("fail on scroll search :" + url + "\n response: " + getEntity(response));
				return null;
			}
			return getEntity(response);
		} catch (Exception e) {
			System.out.println("fail on scroll search url: " + url);
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
	 * 创建索引, 请先运行 checkIndexExist 判断是否存在,不要直接运行
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
	 * @param searchRequest
	 * @param filePath
	 * @param jObjectToCsvFunc
	 */
	public void dumpESDataToCsv(
			String index,
			String type,
			SearchRequest searchRequest,
			String filePath,
			Function<JsonObject, String> jObjectToCsvFunc) {
		assert(index != null && filePath != null && jObjectToCsvFunc != null);
		try {
			File f = new File(filePath);
			if (!f.exists()) { f.createNewFile(); }
			String resp = getFirstScrollSearch(index, type, searchRequest);
			if (null == resp) { System.out.println("search get empty result, terminate."); return; }
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
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public enum Version { es2, es5 }
}
