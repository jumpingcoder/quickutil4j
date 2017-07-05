package com.quickutil.platform.query;

import com.google.gson.JsonObject;
import com.quickutil.platform.FormatQueryException;

/**
 * @author shijie.ruan
 */
public class RangeQuery extends QueryDSL {
	private String field;
	private String gte = null, lte = null, lt = null, gt = null;
	private String format = null;
	private String timeZone = null;


	public RangeQuery(String field) {
		super("range");
		this.field = field;
	}

	public RangeQuery setGte(Integer gte) {
		return setGte(Integer.toString(gte));
	}

	public RangeQuery setGte(Long gte) {
		return setGte(Long.toString(gte));
	}

	public RangeQuery setGte(String gte) {
		this.gte = gte; return this;
	}

	public RangeQuery setLte(Integer lte) {
		return setLte(Integer.toString(lte));
	}

	public RangeQuery setLte(Long lte) {
		return setLte(Long.toString(lte));
	}

	public RangeQuery setLte(String lte) {
		this.lte = lte; return this;
	}

	public RangeQuery setLt(Integer lt) {
		return setLt(Integer.toString(lt));
	}

	public RangeQuery setLt(Long lt) {
		return setLt(Long.toString(lt));
	}

	public RangeQuery setLt(String lt) {
		this.lt = lt; return this;
	}

	public RangeQuery setGt(Integer gt) {
		return setGt(Integer.toString(gt));
	}

	public RangeQuery setGt(Long gt) {
		return setGt(Long.toString(gt));
	}

	public RangeQuery setGt(String gt) {
		this.gt = gt; return this;
	}

	public RangeQuery setBoost(double boost) {
		this.boost = boost; return this;
	}

	/**
	 * 设置查询的日期字段的格式,比如 gte("20170703").setFormat("yyyyMMdd")
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
	public RangeQuery setFormat(String format) {
		this.format = format; return this;
	}

	/**
	 * 设置时区, 有利于将指定的日期变成 UTC 时间进行比较,如果不指定, 日期默认为 UTC 时间
	 * @param timeZone
	 */
	public RangeQuery setTimeZone(String timeZone) {
		this.timeZone = timeZone; return this;
	}

	@Override
	public JsonObject toJson() throws FormatQueryException {
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
		return queryDSL;
	}
}
