package com.quickutil.platform.query;

import com.google.gson.JsonObject;
import com.quickutil.platform.exception.FormatQueryException;

/**
 * @author shijie.ruan
 */
public class ExistQuery extends QueryDSL {
	private String fieldName;

	public ExistQuery(String fieldName) {
		super("exists");
		this.fieldName = fieldName;
	}

	@Override
	public JsonObject toJson() throws FormatQueryException {
		JsonObject existsQuery = new JsonObject();
		existsQuery.addProperty("field", fieldName);
		JsonObject queryDSL = new JsonObject();
		queryDSL.add(type, existsQuery);
		return queryDSL;
	}
}
