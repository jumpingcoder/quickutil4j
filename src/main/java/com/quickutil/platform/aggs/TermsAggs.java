package com.quickutil.platform.aggs;

import com.google.gson.JsonObject;
import com.quickutil.platform.def.SearchRequest;
import com.quickutil.platform.exception.FormatQueryException;

/**
 * @author shijie.ruan
 */
public class TermsAggs extends AggsDSL {
	private String fieldName = null, scriptFileName = null;
	private Order order;
	private Integer size, minDocCount;
	private JsonObject params;

	public TermsAggs(String aggsName, String fieldName, boolean isKeyword) {
		super("terms", aggsName);
		this.fieldName = fieldName;
		if (isKeyword) {
			this.fieldName += ".keyword";
		}
	}

	/**
	 * 仅支持 groovy 脚本文件
	 * @param scriptFileName-脚本文件名,不含后缀
	 */
	public TermsAggs(String aggsName, String scriptFileName) {
		super("terms", aggsName);
		this.scriptFileName = scriptFileName;
	}

	public TermsAggs setSize(int size) {
		this.size = size;
		return this;
	}

	public TermsAggs setOrder(Order order) {
		this.order = order;
		return this;
	}

	public TermsAggs setMinDocCount(int minDocCount) {
		this.minDocCount = minDocCount;
		return this;
	}

	public TermsAggs setParams(JsonObject params) {
		this.params = params;
		return this;
	}

	@Override
	public JsonObject toJson() throws FormatQueryException {
		JsonObject termsObject = new JsonObject();
		if (null != fieldName) {
			termsObject.addProperty("field", fieldName);
		} else if (null != scriptFileName) {
			JsonObject scriptObject = new JsonObject();
			scriptObject.addProperty("file", scriptFileName);
			scriptObject.addProperty("lang", "groovy");
			if (null != params)
				scriptObject.add("params", params);
			termsObject.add("script", scriptObject);
		} else {
			throw new FormatQueryException("terms aggregation must init by field name or script file");
		}
		if (null != size) {
			if (0 == size) //https://www.elastic.co/guide/en/elasticsearch/reference/current/breaking_50_aggregations_changes.html, size = 0, is no longer supported in 5.x
				size = 10000;
			termsObject.addProperty("size", size);
		}
		if (null != order) {
			termsObject.add("order", order.toJson());
		}
		if (null != minDocCount) {
			termsObject.addProperty("min_doc_count", minDocCount);
		}
		return warpAggs(termsObject);
	}

	public static void main(String[] args) throws FormatQueryException {
		TermsAggs termsAggs = new TermsAggs("aa", "assistant_concat")
				.setSize(0).setOrder(new Order("_count"));
		SearchRequest searchRequest = new SearchRequest(termsAggs).setSize(0);
		System.out.println(searchRequest.toJson());
		;
	}
}
