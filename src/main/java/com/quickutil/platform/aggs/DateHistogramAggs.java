package com.quickutil.platform.aggs;

import com.google.gson.JsonObject;
import com.quickutil.platform.exception.FormatQueryException;

/**
 * @author shijie.ruan
 */
public class DateHistogramAggs extends AggsDSL {
	private String fieldName = null, format = null, timeZone, interval = null;
	private String extendedBoundMin, extendedBoundMax;
	private Boolean keyed;
	private Integer minDocCount;

	public DateHistogramAggs(String aggsName, String fieldName, Interval interval) {
		super("date_histogram", aggsName);
		this.fieldName = fieldName;
		this.interval = interval.toString();
	}

	public DateHistogramAggs(String aggsName, String fieldName, String interval) {
		super("date_histogram", aggsName);
		this.fieldName = fieldName;
		this.interval = interval;
	}

	/**
	 * 返回的 key_as_string 中的时间格式,支持 joda 中的时间格式 http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html 常见的为 yyyy-MM-dd
	 * 
	 * @param format
	 * @return
	 */
	public DateHistogramAggs setFormat(String format) {
		this.format = format;
		return this;
	}

	/**
	 * es 中的时间字段是使用 utc 时间存储的,所以聚合默认也是使用 utc 时间,如果制定了 timezone 字段,则聚合是按照 指定的时区进行的,会将 es 中存储的 utc 时间变成指定时区的时间再进行聚合 支持 ISO 8601 UTC offset (e.g. +01:00 or -08:00) 或者是时区 id 例如
	 * America/Los_Angeles
	 * 
	 * @param timeZone
	 * @return
	 */
	public DateHistogramAggs setTimeZone(String timeZone) {
		this.timeZone = timeZone;
		return this;
	}

	/**
	 * 默认情况下是 false, 如果设置为 key, 返回的 buckets 就是一个 JsonObject 而不是 JsonArray, buckets 中的每一个 bucket 都有一个 key
	 * 
	 * @param keyed
	 * @return
	 */
	public DateHistogramAggs setKeyed(boolean keyed) {
		this.keyed = keyed;
		return this;
	}

	/**
	 * 设置 interval
	 * 
	 * @param interval
	 * @return
	 */
	public DateHistogramAggs setInterval(Interval interval) {
		this.interval = interval.toString();
		return this;
	}

	/**
	 * 使用字符串设置 interval, 支持 1d = 1 day, 90m = 90 mins, 注意,使用这个方法不会较验被设置的 interval 的合法性
	 * 
	 * @param interval
	 * @return
	 */
	public DateHistogramAggs setInterval(String interval) {
		this.interval = interval;
		return this;
	}

	public DateHistogramAggs setMinDocCount(int minDocCount) {
		this.minDocCount = minDocCount;
		return this;
	}

	public DateHistogramAggs setExtendedBoundMin(Long extendedBoundMin) {
		return setExtendedBoundMin(Long.toString(extendedBoundMin));
	}

	public DateHistogramAggs setExtendedBoundMin(String extendedBoundMin) {
		this.extendedBoundMin = extendedBoundMin;
		return this;
	}

	public DateHistogramAggs setExtendedBoundMax(Long extendedBoundMax) {
		return setExtendedBoundMax(Long.toString(extendedBoundMax));
	}

	public DateHistogramAggs setExtendedBoundMax(String extendedBoundMax) {
		this.extendedBoundMax = extendedBoundMax;
		return this;
	}

	public JsonObject toJson() throws FormatQueryException {
		JsonObject dateHistogramObject = new JsonObject();
		dateHistogramObject.addProperty("field", fieldName);
		dateHistogramObject.addProperty("interval", interval);
		if (null != format) {
			dateHistogramObject.addProperty("format", format);
		}
		if (null != timeZone) {
			dateHistogramObject.addProperty("time_zone", timeZone);
		}
		if (null != keyed) {
			dateHistogramObject.addProperty("keyed", keyed);
		}
		if (null != minDocCount) {
			dateHistogramObject.addProperty("min_doc_count", minDocCount);
		}
		if (null != extendedBoundMin || null != extendedBoundMax) {
			JsonObject extendedBounds = new JsonObject();
			if (null != extendedBoundMin) {
				try {
					Long extendedBoundMinLong = Long.parseLong(extendedBoundMin);
					extendedBounds.addProperty("min", extendedBoundMinLong);
				} catch (NumberFormatException e) {
					extendedBounds.addProperty("min", extendedBoundMin);
				}
			}
			if (null != extendedBoundMax) {
				try {
					Long extendedBoundMaxLong = Long.parseLong(extendedBoundMax);
					extendedBounds.addProperty("max", extendedBoundMaxLong);
				} catch (NumberFormatException e) {
					extendedBounds.addProperty("max", extendedBoundMax);
				}

			}
			dateHistogramObject.add("extended_bounds", extendedBounds);
		}
		return warpAggs(dateHistogramObject);
	}

	public enum Interval {
		year, quarter, month, week, day, hour, minute
	}
}
