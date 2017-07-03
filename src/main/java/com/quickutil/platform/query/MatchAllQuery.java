package com.quickutil.platform.query;

import com.google.gson.JsonObject;

/**
 * @author shijie.ruan
 */
public class MatchAllQuery extends QueryDSL {

	public MatchAllQuery() {
		super("match_all");
	}

	public void setBoost(double boost) {
		this.boost = boost;
	}

	@Override
	public JsonObject toJson() {
		JsonObject matchAllQuery = new JsonObject();
		if (null != boost) {
			matchAllQuery.addProperty("boost", boost);
		}
		JsonObject queryDSL = new JsonObject();
		queryDSL.add(type, matchAllQuery);
		return queryDSL;
	}
}
