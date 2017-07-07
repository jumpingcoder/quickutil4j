package com.quickutil.platform.query;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.quickutil.platform.exception.FormatQueryException;

import java.util.LinkedList;
import java.util.List;

/**
 * @author shijie.ruan
 * 常量分值 query, 符合 query 的一种
 */
public class BoolQuery extends QueryDSL {
	private List<QueryDSL> mustSubQuery = new LinkedList<>();
	private List<QueryDSL> filterSubQuery = new LinkedList<>();
	private List<QueryDSL> shouldSubQuery = new LinkedList<>();
	private List<QueryDSL> mustNotSubQuery = new LinkedList<>();
	private Integer minimumShouldMatch = null;

	public BoolQuery() {
		super("bool");
	}

	/**
	 * 添加必须满足的 query, 对分值有贡献
	 * @param must
	 * @return
	 */
	public BoolQuery addMustQuery(QueryDSL must) {
		mustSubQuery.add(must);
		return this;
	}

	/**
	 * 添加必须满足的 query, 对分值无贡献
	 * @param filter
	 * @return
	 */
	public BoolQuery addFilterQuery(QueryDSL filter) {
		filterSubQuery.add(filter);
		return this;
	}

	/**
	 * 添加可选的满足的 query
	 * @param should
	 * @return
	 */
	public BoolQuery addShouldQuery(QueryDSL should) {
		shouldSubQuery.add(should);
		return this;
	}

	/**
	 * 添加必须不满足的 query
	 * @param mustNot
	 * @return
	 */
	public BoolQuery addMustNotQuery(QueryDSL mustNot) {
		mustNotSubQuery.add(mustNot);
		return this;
	}

	public BoolQuery setMininumShouldMatch(int mininumShouldMatch) {
		this.minimumShouldMatch = mininumShouldMatch;
		return this;
	}

	public BoolQuery setBoost(double boost) {
		this.boost = boost;
		return this;
	}

	@Override
	public JsonObject toJson() throws FormatQueryException {
		JsonObject boolObject = new JsonObject();
		if (null != minimumShouldMatch && minimumShouldMatch > shouldSubQuery.size()) {
			System.out.format("bool query minimum_should_match[%d] bigger than should query size[%d], "
					+ "result will be empty\n", minimumShouldMatch, shouldSubQuery.size());
		}
		if (!mustSubQuery.isEmpty()) {
			JsonArray must = new JsonArray();
			for (QueryDSL subQuery: mustSubQuery) { must.add(subQuery.toJson()); }
			boolObject.add("must", must);
		}
		if (!filterSubQuery.isEmpty()) {
			JsonArray filter = new JsonArray();
			for (QueryDSL subQuery: filterSubQuery) { filter.add(subQuery.toJson()); }
			boolObject.add("filter", filter);
		}
		if (!shouldSubQuery.isEmpty()) {
			JsonArray should = new JsonArray();
			for (QueryDSL subQuery: shouldSubQuery) { should.add(subQuery.toJson()); }
			boolObject.add("should", should);
		}
		if (!mustNotSubQuery.isEmpty()) {
			JsonArray mustNot = new JsonArray();
			for (QueryDSL subQuery: mustNotSubQuery) { mustNot.add(subQuery.toJson()); }
			boolObject.add("must_not", mustNot);
		}
		if (null != minimumShouldMatch) {
			boolObject.addProperty("minimum_should_match", minimumShouldMatch);
		}
		if (null != boost) {
			boolObject.addProperty("boost", boost);
		}
		JsonObject queryDSL = new JsonObject();
		queryDSL.add(type, boolObject);
		return queryDSL;
	}
}
