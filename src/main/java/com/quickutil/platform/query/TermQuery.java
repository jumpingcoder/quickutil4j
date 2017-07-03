package com.quickutil.platform.query;

import com.google.gson.JsonObject;

/**
 * @author shijie.ruan
 */
public class TermQuery extends QueryDSL {
	private String field;
	private String value;

	/**
	 * term query, 查询 field 字段的特定 value,注意,这个 value 不会分词,
	 * 如果 field 进行了分词, value 必须于分词后的值匹配
	 * @param field
	 * @param value
	 */
	public TermQuery(String field, String value) {
		super("term");
		this.field = field;
		this.value = value;
	}

	@Override
	public JsonObject toJson() {
		JsonObject termQuery = new JsonObject();
		if (null != boost) {
			JsonObject fieldObject = new JsonObject();
			fieldObject.addProperty("value", value);
			fieldObject.addProperty("boost", boost);
			termQuery.add(field, fieldObject);
		} else {
			termQuery.addProperty(field, value);
		}
		JsonObject queryDSL = new JsonObject();
		queryDSL.add(type, termQuery);
		return queryDSL;
	}
}
