package com.quickutil.platform.aggs;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.quickutil.platform.FormatQueryException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author shijie.ruan
 */
public class RangeAggs extends AggsDSL {
	private String fieldName;
	private Boolean keyed;
	private List<Range> ranges = new LinkedList<>();

	public RangeAggs(String aggsName, String fieldName) {
		super("range", aggsName);
		this.fieldName = fieldName;
	}

	public RangeAggs(String aggsName, String fieldName, boolean keyed) {
		super("range", aggsName);
		this.fieldName = fieldName;
		this.keyed = keyed;
	}

	public RangeAggs addRange(Range range) {
		this.ranges.add(range);
		return this;
	}

	public void setKeyed(boolean keyed) {
		this.keyed = keyed;
	}

	@Override
	public JsonObject toJson() throws FormatQueryException {
		if (ranges.isEmpty()) {
			throw new FormatQueryException("ranges must be not empty");
		}
		JsonObject rangeObject = new JsonObject();
		rangeObject.addProperty("field", fieldName);
		JsonArray rangesArray = new JsonArray();
		for (Range range : ranges) {
			rangesArray.add(range.toJson());
		}
		rangeObject.add("ranges", rangesArray);
		if (null != keyed) {
			rangeObject.addProperty("keyed", keyed);
		}
		return warpAggs(rangeObject);
	}
}
