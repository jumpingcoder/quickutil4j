package com.quickutil.platform.aggs;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.quickutil.platform.FormatQueryException;
import com.quickutil.platform.JsonUtil;
import java.util.LinkedList;
import java.util.List;

/**
 * @author shijie.ruan
 */
public class DateRangeAggs extends AggsDSL {
	private String fieldName = null, format = null, timeZone = null;
	private Boolean keyed = null;
	private List<Range> ranges = new LinkedList<>();

	public DateRangeAggs(String aggsName, String fieldName) {
		super("date_range", aggsName);
		this.fieldName = fieldName;
	}

	/**
	 * 增加一个 range
	 * @param range
	 * @return
	 */
	public DateRangeAggs addRange(Range range) { ranges.add(range); return this; }

	/**
	 * 返回的 key_as_string 中的时间格式,支持 joda 中的时间格式
	 * http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html
	 * 常见的为 yyyy-MM-dd
	 * @param format
	 * @return
	 */
	public DateRangeAggs setFormat(String format) {
		this.format = format;
		return this;
	}

	/**
	 * es 中的时间字段是使用 utc 时间存储的,所以聚合默认也是使用 utc 时间,如果制定了 timezone 字段,则聚合是按照
	 * 指定的时区进行的,会将 es 中存储的 utc 时间变成指定时区的时间再进行聚合
	 * 支持 ISO 8601 UTC offset (e.g. +01:00 or -08:00) 或者是时区 id 例如 America/Los_Angeles
	 * @param timeZone
	 * @return
	 */
	public DateRangeAggs setTimeZone(String timeZone) {
		this.timeZone = timeZone;
		return this;
	}

	@Override
	public String toJson() throws FormatQueryException {
		if (ranges.isEmpty()) {
			throw new FormatQueryException("ranges must be not empty");
		}
		JsonObject dateRangeObject = new JsonObject();
		dateRangeObject.addProperty("field", fieldName);
		JsonArray rangesArray = new JsonArray();
		for (Range range: ranges) { rangesArray.add(range.toJson()); }
		dateRangeObject.add("ranges", rangesArray);
		if (null != format) {
			dateRangeObject.addProperty("format", format);
		}
		if (null != keyed) {
			dateRangeObject.addProperty("keyed", keyed);
		}
		return warpAggs(dateRangeObject);
	}
}
