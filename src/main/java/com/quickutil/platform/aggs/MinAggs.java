package com.quickutil.platform.aggs;

import com.google.gson.JsonObject;
import com.quickutil.platform.exception.FormatQueryException;

/**
 * @author junjie.zhao
 */
public class MinAggs extends AggsDSL {
	private String fieldName, missing = null;;

	public MinAggs(String aggsName, String fieldName) {
		super("min", aggsName);
		this.fieldName = fieldName;
	}

	/**
	 * 如果字段不存在,设置默认值用来平均,如果不设置,不存在该字段的文档将被忽略
	 * @param missing
	 * @return
	 */
	public MinAggs setMissing(String missing) {
		this.missing = missing;
		return this;
	}

	@Override
	public JsonObject toJson() throws FormatQueryException {
		JsonObject minObject = new JsonObject();
		minObject.addProperty("field", fieldName);
		if (null != missing) {
			minObject.addProperty("missing", missing);
		}
		return warpAggs(minObject);
	}
}
