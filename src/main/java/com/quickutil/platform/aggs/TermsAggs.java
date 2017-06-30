package com.quickutil.platform.aggs;

import com.google.gson.JsonObject;
import com.quickutil.platform.FormatQueryException;
import com.quickutil.platform.JsonUtil;

/**
 * @author shijie.ruan
 */
public class TermsAggs extends AggsDSL {
	private String fieldName;
	private Order order;
	private Integer size;
	private Integer minDocCount;

	public TermsAggs(String aggsName, String fieldName) {
		super("terms", aggsName);
		this.fieldName = fieldName;
	}

	public TermsAggs setSize(int size) { this.size = size; return this; }

	public TermsAggs setOrder(Order order) { this.order = order; return this; }

	public TermsAggs setMinDocCount(int minDocCount) { this.minDocCount = minDocCount; return this; }

	@Override
	public String toJson() throws FormatQueryException {
		JsonObject termsObject = new JsonObject();
		termsObject.addProperty("field", fieldName);
		if (null != size) {
			termsObject.addProperty("size", size);
		}
		if (null != order) {
			termsObject.add("order", order.toJson());
		}
		if (null != minDocCount) {
			termsObject.addProperty("min_doc_count", minDocCount);
		}
		return warpAggs(termsObject);
	}
}
