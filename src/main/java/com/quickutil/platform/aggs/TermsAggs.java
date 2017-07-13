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
	private Boolean useKeyWord = false;

	public TermsAggs(String aggsName, String fieldName) {
		super("terms", aggsName);
		this.fieldName = fieldName;
	}

	public TermsAggs(String aggsName) {
		super("terms", aggsName);
	}

	public TermsAggs setFieldName(String fieldName) {
		this.fieldName = fieldName;
		return this;
	}

	/**
	 * 仅支持 groovy 脚本文件
	 */
	public TermsAggs setScriptFileName(String scriptFileName) {
		this.scriptFileName = scriptFileName;
		return this;
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

	/**
	 * 5.x 在默认情况下会将字符串存储为 text 和 keyword 字段, 字段名默认指向 text, 用于搜索, 不支持聚合,排序
	 * 如果你没有进行 mapping 设置,而在使用字符串字段进行聚合和排序的时候报错如下:
	 * Fielddata is disabled on text fields by default. Set fielddata=true on [] in order to load
	 * fielddata in memory by uninverting the inverted index.
	 * 你可以调用这个函数使用它的 keyword 字段进行排序或者聚合,但是还是建议设置 mapping, 如果需要使用字符串进行
	 * 排序和聚合等,请使用 keyword datatype.这样节省空间,而且在排序和聚合时不用使用.keyword
	 */
	public TermsAggs useKeyWord() {
		this.useKeyWord = true;
		return this;
	}

	@Override
	public JsonObject toJson() throws FormatQueryException {
		JsonObject termsObject = new JsonObject();
		if (null != fieldName) {
			if (useKeyWord) {
				termsObject.addProperty("field", fieldName + ".keyword");
			} else {
				termsObject.addProperty("field", fieldName);
			}
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
	}
}
