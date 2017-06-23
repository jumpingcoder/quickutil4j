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

	/**
	 * Index a JSON source associated with a given index and type.
	 * <p>
	 * The id is optional, if it is not provided, one will be generated automatically.
	 *
	 * @param request The index request
	 * @return The result future
	 * @see Requests#indexRequest(String)
	 */
	ActionFuture<IndexResponse> index(IndexRequest request);

	/**
	 * Index a document associated with a given index and type.
	 * <p>
	 * The id is optional, if it is not provided, one will be generated automatically.
	 *
	 * @param request  The index request
	 * @param listener A listener to be notified with a result
	 * @see Requests#indexRequest(String)
	 */
	void index(IndexRequest request, ActionListener<IndexResponse> listener);

	/**
	 * Index a document associated with a given index and type.
	 * <p>
	 * The id is optional, if it is not provided, one will be generated automatically.
	 */
	IndexRequestBuilder prepareIndex();

	/**
	 * Updates a document based on a script.
	 *
	 * @param request The update request
	 * @return The result future
	 */
	ActionFuture<UpdateResponse> update(UpdateRequest request);

	/**
	 * Updates a document based on a script.
	 *
	 * @param request  The update request
	 * @param listener A listener to be notified with a result
	 */
	void update(UpdateRequest request, ActionListener<UpdateResponse> listener);

	/**
	 * Updates a document based on a script.
	 */
	UpdateRequestBuilder prepareUpdate();

	/**
	 * Updates a document based on a script.
	 */
	UpdateRequestBuilder prepareUpdate(String index, String type, String id);

	/**
	 * Index a document associated with a given index and type.
	 * <p>
	 * The id is optional, if it is not provided, one will be generated automatically.
	 *
	 * @param index The index to index the document to
	 * @param type  The type to index the document to
	 */
	IndexRequestBuilder prepareIndex(String index, String type);

	/**
	 * Index a document associated with a given index and type.
	 * <p>
	 * The id is optional, if it is not provided, one will be generated automatically.
	 *
	 * @param index The index to index the document to
	 * @param type  The type to index the document to
	 * @param id    The id of the document
	 */
	IndexRequestBuilder prepareIndex(String index, String type, @Nullable String id);

	/**
	 * Deletes a document from the index based on the index, type and id.
	 *
	 * @param request The delete request
	 * @return The result future
	 * @see Requests#deleteRequest(String)
	 */
	ActionFuture<DeleteResponse> delete(DeleteRequest request);

	/**
	 * Deletes a document from the index based on the index, type and id.
	 *
	 * @param request  The delete request
	 * @param listener A listener to be notified with a result
	 * @see Requests#deleteRequest(String)
	 */
	void delete(DeleteRequest request, ActionListener<DeleteResponse> listener);

	/**
	 * Deletes a document from the index based on the index, type and id.
	 */
	DeleteRequestBuilder prepareDelete();

	/**
	 * Deletes a document from the index based on the index, type and id.
	 *
	 * @param index The index to delete the document from
	 * @param type  The type of the document to delete
	 * @param id    The id of the document to delete
	 */
	DeleteRequestBuilder prepareDelete(String index, String type, String id);

	/**
	 * Executes a bulk of index / delete operations.
	 *
	 * @param request The bulk request
	 * @return The result future
	 * @see org.elasticsearch.client.Requests#bulkRequest()
	 */
	ActionFuture<BulkResponse> bulk(BulkRequest request);

	/**
	 * Executes a bulk of index / delete operations.
	 *
	 * @param request  The bulk request
	 * @param listener A listener to be notified with a result
	 * @see org.elasticsearch.client.Requests#bulkRequest()
	 */
	void bulk(BulkRequest request, ActionListener<BulkResponse> listener);

	/**
	 * Executes a bulk of index / delete operations.
	 */
	BulkRequestBuilder prepareBulk();

	/**
	 * Gets the document that was indexed from an index with a type and id.
	 *
	 * @param request The get request
	 * @return The result future
	 * @see Requests#getRequest(String)
	 */
	ActionFuture<GetResponse> get(GetRequest request);

	/**
	 * Gets the document that was indexed from an index with a type and id.
	 *
	 * @param request  The get request
	 * @param listener A listener to be notified with a result
	 * @see Requests#getRequest(String)
	 */
	void get(GetRequest request, ActionListener<GetResponse> listener);

	/**
	 * Gets the document that was indexed from an index with a type and id.
	 */
	GetRequestBuilder prepareGet();

	/**
	 * Gets the document that was indexed from an index with a type (optional) and id.
	 */
	GetRequestBuilder prepareGet(String index, @Nullable String type, String id);

	/**
	 * Multi get documents.
	 */
	ActionFuture<MultiGetResponse> multiGet(MultiGetRequest request);

	/**
	 * Multi get documents.
	 */
	void multiGet(MultiGetRequest request, ActionListener<MultiGetResponse> listener);

	/**
	 * Multi get documents.
	 */
	MultiGetRequestBuilder prepareMultiGet();

	/**
	 * Search across one or more indices and one or more types with a query.
	 *
	 * @param request The search request
	 * @return The result future
	 * @see Requests#searchRequest(String...)
	 */
	ActionFuture<SearchResponse> search(SearchRequest request);

	/**
	 * Search across one or more indices and one or more types with a query.
	 *
	 * @param request  The search request
	 * @param listener A listener to be notified of the result
	 * @see Requests#searchRequest(String...)
	 */
	void search(SearchRequest request, ActionListener<SearchResponse> listener);

	/**
	 * Search across one or more indices and one or more types with a query.
	 */
	SearchRequestBuilder prepareSearch(String... indices);

	/**
	 * A search scroll request to continue searching a previous scrollable search request.
	 *
	 * @param request The search scroll request
	 * @return The result future
	 * @see Requests#searchScrollRequest(String)
	 */
	ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request);

	/**
	 * A search scroll request to continue searching a previous scrollable search request.
	 *
	 * @param request  The search scroll request
	 * @param listener A listener to be notified of the result
	 * @see Requests#searchScrollRequest(String)
	 */
	void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener);

	/**
	 * A search scroll request to continue searching a previous scrollable search request.
	 */
	SearchScrollRequestBuilder prepareSearchScroll(String scrollId);

	/**
	 * Performs multiple search requests.
	 */
	ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request);

	/**
	 * Performs multiple search requests.
	 */
	void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener);

	/**
	 * Performs multiple search requests.
	 */
	MultiSearchRequestBuilder prepareMultiSearch();

	/**
	 * Computes a score explanation for the specified request.
	 *
	 * @param request  The request encapsulating the query and document identifier to compute a score explanation for
	 * @param listener A listener to be notified of the result
	 */
	void explain(ExplainRequest request, ActionListener<ExplainResponse> listener);
}
