package com.quickutil.platform.def;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.quickutil.platform.JsonUtil;
import com.quickutil.platform.aggs.AggsDSL;
import com.quickutil.platform.aggs.Order;
import com.quickutil.platform.exception.FormatQueryException;
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
	private List<AggsDSL> aggsList = new LinkedList<>();

	public SearchRequest(QueryDSL query) {
		this.query = query;
	}

	public SearchRequest(AggsDSL aggs) {
		this.aggsList.add(aggs);
	}

	public SearchRequest(QueryDSL query, AggsDSL aggs) {
		this.query = query; this.aggsList.add(aggs);
	}

	public SearchRequest addAggs(AggsDSL aggs) {
		this.aggsList.add(aggs); return this;
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
		if (!aggsList.isEmpty()) {
			if (1 == aggsList.size()) {
				queryObject.add("aggs", aggsList.get(0).toJson());
			} else {
				JsonObject aggsObject = new JsonObject();
				for (AggsDSL aggs: aggsList) {
					aggsObject.add(aggs.getAggsName(), aggs.toJson().getAsJsonObject(aggs.getAggsName()));
				}
				queryObject.add("aggs", aggsObject);
			}
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
