package com.quickutil.platform;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.quickutil.platform.aggs.AggsDSL;
import com.quickutil.platform.aggs.Order;
import com.quickutil.platform.query.QueryDSL;
import java.util.LinkedList;
import java.util.List;

/**
 * @author shijie.ruan
 */
public class SearchRequest {
	private Integer size = null, from = null;
	private List<String> includeSource = new LinkedList<>();
	private List<Order> sort = new LinkedList<>();

	private QueryDSL query = null;
	private AggsDSL aggs = null;

	public SearchRequest(QueryDSL query) {
		this.query = query;
	}

	public SearchRequest(AggsDSL aggs) {
		this.aggs = aggs;
	}

	public SearchRequest(QueryDSL query, AggsDSL aggs) {
		this.query = query; this.aggs = aggs;
	}

	public SearchRequest setSize(int size) {
		this.size = size; return this;
	}

	public SearchRequest setFrom(int from) {
		this.from = from; return this;
	}

	public SearchRequest addSort(Order order) {
		sort.add(order); return this;
	}
	/**
	 * 增加返回的源字段,不设置默认返回全部的 source, 支持通配符,用于文档很大,只需要某些字段的情况
	 * @param sourceField
	 * @return
	 */
	public SearchRequest addIncludeSource(String sourceField) {
		this.includeSource.add(sourceField); return this;
	}

	public String toJson() throws FormatQueryException {
		JsonObject queryObject = new JsonObject();
		if (null != query) {
			queryObject.add("query", query.toJson());
		}
		if (null != aggs) {
			queryObject.addProperty("aggs", aggs.toJson());
		}
		if (null != size) {
			queryObject.addProperty("size", size);
		}
		if (null != from) {
			queryObject.addProperty("from", from);
		}
		if (!sort.isEmpty()) {
			JsonArray sortList = new JsonArray();
			for (Order order: sort) { sortList.add(order.toJson()); }
			queryObject.add("sort", sortList);
		}
		if (!includeSource.isEmpty()) {
			JsonArray source = new JsonArray();
			for (String field: includeSource) { source.add(field); }
			queryObject.add("_source", source);
		}
		return JsonUtil.toJson(queryObject);
	}
}
