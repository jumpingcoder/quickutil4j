package com.quickutil.platform.query;

import com.google.gson.JsonObject;
import com.quickutil.platform.FormatQueryException;
import com.quickutil.platform.JsonUtil;

/**
 * @author shijie.ruan
 */
public class WildcardQuery extends QueryDSL {
	private String field;
	private String wildcardString;

	WildcardQuery(String field, String wildcardString) {
		super("wildcard");
		this.field = field; this.wildcardString = wildcardString;
	}

	public void setBoost(double boost) {
		this.boost = boost;
	}

	@Override
	public String toJson() throws FormatQueryException {
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
		return JsonUtil.toJson(queryDSL);
	}
}
