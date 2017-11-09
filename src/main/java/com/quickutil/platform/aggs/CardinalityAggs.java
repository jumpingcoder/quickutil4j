package com.quickutil.platform.aggs;

import com.google.gson.JsonObject;
import com.quickutil.platform.exception.FormatQueryException;

/**
 * @author shijie.ruan
 */
public class CardinalityAggs extends AggsDSL {
	private String fieldName = null, missing = null;
	private Integer precisionThreshold = null;
	private Boolean useKeyWord = false;

	/**
	 *
	 * @param aggsName
	 * @param fieldName
	 */
	public CardinalityAggs(String aggsName, String fieldName) {
		super("cardinality", aggsName);
		this.fieldName = fieldName;
	}

	/**
	 * 设置精确值参数, cardinality 聚合是近似值,使用 HyperLogLog++ 算法,基于哈希函数 设置大的值可以让算法更精确, 最大值为40000, 默认值 3000
	 */
	public CardinalityAggs setPrecisionThreshold(int precisionThreshold) {
		this.precisionThreshold = precisionThreshold;
		return this;
	}

	/**
	 * 如果字段不存在,设置默认的 bucket 名字将不存在该字段的文档归入该 bucket,如果不设置,不存在字段的文档将被忽略
	 * 
	 * @param missing
	 * @return
	 */
	public CardinalityAggs setMissing(String missing) {
		this.missing = missing;
		return this;
	}

	/**
	 * 5.x 在默认情况下会将字符串存储为 text 和 keyword 字段, 字段名默认指向 text, 用于搜索, 不支持聚合,排序
	 * 如果你没有进行 mapping 设置,而在使用字符串字段进行聚合和排序的时候报错如下:
	 * Fielddata is disabled on text fields by default. Set fielddata=true on [] in order to load
	 * fielddata in memory by uninverting the inverted index.
	 * 你可以调用这个函数使用它的 keyword 字段进行排序或者聚合,但是还是建议设置 mapping, 如果需要使用字符串进行
	 * 排序和聚合等,请使用 keyword datatype.这样节省空间,而且在排序和聚合时不用使用.keyword
	 */
	public CardinalityAggs useKeyWord() {
		this.useKeyWord = true;
		return this;
	}

	public JsonObject toJson() throws FormatQueryException {
		JsonObject cardinalityObject = new JsonObject();
		if (useKeyWord) {
			cardinalityObject.addProperty("field", fieldName + ".keyword");
		} else {
			cardinalityObject.addProperty("field", fieldName);
		}

		if (null != missing) {
			cardinalityObject.addProperty("missing", missing);
		}
		if (null != precisionThreshold) {
			cardinalityObject.addProperty("precision_threshold", precisionThreshold);
		}
		return warpAggs(cardinalityObject);
	}
}
