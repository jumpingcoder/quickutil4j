package com.quickutil.platform.aggs;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.quickutil.platform.exception.FormatQueryException;

/**
 * @author junjie.zhao
 */
public class PercentilesAggs extends AggsDSL {

	private String fieldName, missing = null;
	private Double[] percents = null;


	public PercentilesAggs(String aggsName, String fieldName, Double[] percents) {
		super("percentiles", aggsName);
		this.fieldName = fieldName;
		this.percents = percents;
	}

	/**
	 * 如果字段不存在,设置默认值用来平均,如果不设置,不存在该字段的文档将被忽略
	 */
	public PercentilesAggs setMissing(String missing) {
		this.missing = missing;
		return this;
	}

	@Override
	public JsonObject toJson() throws FormatQueryException {
		JsonObject minObject = new JsonObject();
		minObject.addProperty("field", fieldName);
		if (percents != null) {
			JsonArray array = new JsonArray();
			for (Double d : percents) {
				array.add(d);
			}
			minObject.add("percents", array);
		}
		if (null != missing) {
			minObject.addProperty("missing", missing);
		}
		return warpAggs(minObject);
	}
}
