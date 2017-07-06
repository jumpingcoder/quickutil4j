package com.quickutil.platform.aggs;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.quickutil.platform.FormatQueryException;
import com.quickutil.platform.JsonUtil;
import com.quickutil.platform.aggs.Range;

/**
 * @author shijie.ruan
 * 应用于数值类型的 histogram 聚合
 */
public class HistogramAggs extends AggsDSL {
  private String fieldName = null;
  private Integer minDocCount = null;
  private Number interval = null, extendedBoundMin, extendedBoundMax;
  private Order order;

	public HistogramAggs(String aggsName, String fieldName, Number interval) {
		super("histogram", aggsName);
		this.fieldName = fieldName;
		this.interval = interval;
	}

	public HistogramAggs setMinDocCount(Integer minDocCount) {
		this.minDocCount = minDocCount;
		return this;
	}

	public HistogramAggs setExtendedBoundMin(Number extendedBoundMin) {
		this.extendedBoundMin = extendedBoundMin;
		return this;
	}

	public HistogramAggs setExtendedBoundMax(Number extendedBoundMax) {
		this.extendedBoundMax = extendedBoundMax;
		return this;
	}

	public HistogramAggs setOrder(Order order) {
		this.order = order;
		return this;
	}

	@Override
	public JsonObject toJson() throws FormatQueryException {
		JsonObject histogramObject = new JsonObject();
		histogramObject.addProperty("field", fieldName);
		histogramObject.addProperty("interval", interval);
		if (null != minDocCount) {
			histogramObject.addProperty("min_doc_count", minDocCount);
		}
		if (null != extendedBoundMin || null != extendedBoundMax) {
			JsonObject extendedBounds = new JsonObject();
			if (null != extendedBoundMin) {extendedBounds.addProperty("min", extendedBoundMin);}
			if (null != extendedBoundMin) {extendedBounds.addProperty("max", extendedBoundMax);}
			histogramObject.add("extended_bounds", extendedBounds);
		}
		return warpAggs(histogramObject);
	}
}
