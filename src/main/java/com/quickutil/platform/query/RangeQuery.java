package com.quickutil.platform.query;

import com.google.gson.JsonObject;
import com.quickutil.platform.FormatQueryException;
import com.quickutil.platform.JsonUtil;

/**
 * @author shijie.ruan
 */
public class RangeQuery extends QueryDSL {
	private String field;
	private String gte = null, lte = null, lt = null, gt = null;
	private String format = null;
	private String timeZone = null;


	RangeQuery(String field) {
		super("range");
		this.field = field;
	}

	public void setGte(String gte) {
		this.gte = gte;
	}

	public void setLte(String lte) {
		this.lte = lte;
	}

	public void setLt(String lt) {
		this.lt = lt;
	}

	public void setGt(String gt) {
		this.gt = gt;
	}

	public void setBoost(double boost) {
		this.boost = boost;
	}

	/**
	 * 日期字段的格式
	 * epoch_millis: number of milliseconds since the epoch,
	 * epoch_second: number of seconds since the epoch,
	 * date_optional_time: ISO datetime parser where the date is mandatory and the time is optional,
	 * basic_date: yyyyMMdd,
	 * basic_date_time: yyyyMMdd'T'HHmmss.SSSZ,
	 * basic_date_time_no_millisL: yyyyMMdd'T'HHmmss,
	 * date: yyyy-MM-dd
	 * date_hour_minute_second: yyyy-MM-dd'T'HH:mm:ss
	 * 更过请查看:
	 * https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html
	 * @param format
	 */
	public void setFormat(String format) {
		this.format = format;
	}

	/**
	 * 设置时区, 有利于将指定的日期变成 UTC 时间进行比较,如果不指定, 日期默认为 UTC 时间
	 * @param timeZone
	 */
	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}

	@Override
	public String toJson() throws FormatQueryException {
		JsonObject rangeObject = new JsonObject();
		if (null != gt && null != gte) {
			throw new FormatQueryException("either gt or gte can be set");
		}
		if (null != lt && null != lte) {
			throw new FormatQueryException("either lt or lte can be set");
		}
		if (null != gt) { rangeObject.addProperty("gt", gt); }
		if (null != gte) { rangeObject.addProperty("gte", gte); }
		if (null != lt) { rangeObject.addProperty("lt", lt); }
		if (null != lte) { rangeObject.addProperty("lte", lte); }
		if (null != format) { rangeObject.addProperty("format", format); }
		if (null != timeZone) { rangeObject.addProperty("time_zone", timeZone);	}
		if (null != boost) { rangeObject.addProperty("boost", boost); }
		JsonObject fieldObject = new JsonObject();
		fieldObject.add(field, rangeObject);
		JsonObject queryDSL = new JsonObject();
		queryDSL.add(type, fieldObject);
		return JsonUtil.toJson(queryDSL);
	}
}
