package com.quickutil.platform.query;

import com.google.gson.JsonObject;
import com.quickutil.platform.exception.FormatQueryException;

/**
 * @author shijie.ruan
 */
public class WildcardQuery extends QueryDSL {
	private String field;
	private String wildcardString;

	public WildcardQuery(String field, String wildcardString) {
		super("wildcard");
		this.field = field; this.wildcardString = wildcardString;
	}

	public void setBoost(double boost) {
		this.boost = boost;
	}

	@Override
	public JsonObject toJson() throws FormatQueryException {
		JsonObject wildcardQuery = new JsonObject();
		if (null != boost) {
			JsonObject fieldObject = new JsonObject();
			fieldObject.addProperty("wildcard", wildcardString);
			fieldObject.addProperty("boost", boost);
			wildcardQuery.add(field, fieldObject);
		} else {
			wildcardQuery.addProperty(field, wildcardString);
		}
		JsonObject queryDSL = new JsonObject();
		queryDSL.add(type, wildcardQuery);
		return queryDSL;
	}
}
