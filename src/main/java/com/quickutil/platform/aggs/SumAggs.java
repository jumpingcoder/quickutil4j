package com.quickutil.platform.aggs;

import com.google.gson.JsonObject;
import com.quickutil.platform.exception.FormatQueryException;

/**
 * @author shijie.ruan
 */
public class SumAggs extends AggsDSL {
	private String fieldName, missing = null;;

	public SumAggs(String aggsName, String fieldName) {
		super("sum", aggsName);
		this.fieldName = fieldName;
	}

	/**
	 * 如果字段不存在,设置默认值用来平均,如果不设置,不存在该字段的文档将被忽略
	 * @param missing
	 * @return
	 */
	public SumAggs setMissing(String missing) {
		this.missing = missing;
		return this;
	}

	@Override
	public JsonObject toJson() throws FormatQueryException {
		JsonObject avgObject = new JsonObject();
		avgObject.addProperty("field", fieldName);
		if (null != missing) {
			avgObject.addProperty("missing", missing);
		}
		return warpAggs(avgObject);
	}
}
