package com.quickutil.platform.aggs;

import com.google.gson.JsonObject;
import com.quickutil.platform.exception.FormatQueryException;

import java.util.LinkedList;
import java.util.List;

/**
 *
 "aggregations" : {
	 "<aggregation_name>" : {
		 "<aggregation_type>" : {
			  <aggregation_body>
			 }
			 [,"meta" : {  [<meta_data_body>] } ]?
			 [,"aggregations" : { [<sub_aggregation>]+ } ]?
		 }
	 [,"<aggregation_name_2>" : { ... } ]*
 }
 聚合可以分为三类(不算 matrix aggs 这种实验性的 feature)
 1. bucketing: 桶型聚合,每个桶有一个自己的名字和一个统计标准, 符合查询条件的 doc 会根据桶的标准落到每一个桶中,
 这种聚合最后会得到一系列的桶和桶中的文档数
 2. metric: 对于符合 query 的所有文档计算一个全局的统计值
 3. pipeline: 聚合其他聚合的结果
 * @author shijie.ruan
 */
public abstract class AggsDSL {
	protected String type;
	protected String aggsName;
	protected List<AggsDSL> subAggsList = new LinkedList<>();

	AggsDSL(String type, String aggsName) {
		this.type = type;
		this.aggsName = aggsName;
	}

	public AggsDSL addSubAggs(AggsDSL subAggs) {
		this.subAggsList.add(subAggs);
		return this;
	}

	public String getAggsName() {
		return this.aggsName;
	}

	public List<AggsDSL> getSubAggsList() {
		return this.subAggsList;
	}

	public int getSubAggsSize() {
		return this.subAggsList.size();
	}

	public AggsDSL clearSubAggs() {
		this.subAggsList.clear();
		return this;
	}

	public abstract JsonObject toJson() throws FormatQueryException;

	protected JsonObject warpAggs(JsonObject child) throws FormatQueryException {
		JsonObject wrap = new JsonObject();
		wrap.add(type, child);
		if (!subAggsList.isEmpty()) {
			if (1 == subAggsList.size()) {
				wrap.add("aggs", subAggsList.get(0).toJson());
			} else {
				JsonObject subAggsObject = new JsonObject();
				for (AggsDSL subAggs : subAggsList) {
					subAggsObject.add(subAggs.aggsName, subAggs.toJson().getAsJsonObject(subAggs.aggsName));
				}
				wrap.add("aggs", subAggsObject);
			}
		}
		JsonObject aggs = new JsonObject();
		aggs.add(aggsName, wrap);
		return aggs;
	}
}
