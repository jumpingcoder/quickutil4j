package com.quickutil.platform.aggs;

import com.google.gson.JsonObject;
import com.quickutil.platform.FormatQueryException;
import com.quickutil.platform.JsonUtil;

/**
 * 扩展的 stat 聚合,包含count, max, min, avg, sum, sum_of_squares, variance, std_deviation,
 * std_deviation_bounds(upper, lower)
 * 可以应用于数值字段(或者用脚本根据字段生成数值,暂不支持)
 * @author shijie.ruan
 */
public class ExtendedStatsAggs extends AggsDSL {
	private String fieldName = null;
	private Number missing = null;

	public ExtendedStatsAggs(String aggsName, String fieldName) {
		super("extended_stats", aggsName);
		this.fieldName = fieldName;
	}

	public ExtendedStatsAggs setMissing(Number missing) {
		this.missing = missing;
		return this;
	}

	@Override
	public JsonObject toJson() throws FormatQueryException {
		JsonObject statObject = new JsonObject();
		statObject.addProperty("field", fieldName);
		if (null != missing) {
			statObject.addProperty("missing", missing);
		}
		return warpAggs(statObject);
	}
}
