package com.quickutil.platform.aggs;

import com.google.gson.JsonObject;
import com.quickutil.platform.FormatQueryException;

/**
 * @author shijie.ruan
 */
public class CardinalityAggs extends AggsDSL {
	private String fieldName = null, missing = null;
	private Integer precisionThreshold = null;

	public CardinalityAggs(String aggsName, String fieldName) {
		super("cardinality", aggsName);
	}

	/**
	 * 设置精确值参数, cardinality 聚合是近似值,使用 HyperLogLog++ 算法,基于哈希函数
	 * 设置大的值可以让算法更精确, 最大值为40000, 默认值 3000
	 */
	public CardinalityAggs setPrecisionThreshold(int precisionThreshold) {
		this.precisionThreshold = precisionThreshold;
		return this;
	}

	/**
	 * 如果字段不存在,设置默认的 bucket 名字将不存在该字段的文档归入该 bucket,如果不设置,不存在字段的文档将被忽略
	 * @param missing
	 * @return
	 */
	public CardinalityAggs setMissing(String missing) {
		this.missing = missing;
		return this;
	}

	public String toJson() throws FormatQueryException {
		JsonObject cardinalityObject = new JsonObject();
		cardinalityObject.addProperty("field", fieldName);
		if (null != missing) {
			cardinalityObject.addProperty("missing", missing);
		}
		if (null != precisionThreshold) {
			cardinalityObject.addProperty("precision_threshold", precisionThreshold);
		}
		return warpAggs(cardinalityObject);
	}
}
