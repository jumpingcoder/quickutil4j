package com.quickutil.platform.aggs;

import com.google.gson.JsonObject;
import com.quickutil.platform.exception.FormatQueryException;

/**
 * @author shijie.ruan
 */
public class ValueCountAggs extends AggsDSL {
	private String field;

	public ValueCountAggs(String aggsName, String fieldName) {
		super("value_count", aggsName);
		this.field = fieldName;
	}

	@Override
	public JsonObject toJson() throws FormatQueryException {
		JsonObject valueCountObject = new JsonObject();
		valueCountObject.addProperty("field", field);
		return warpAggs(valueCountObject);
	}
}
