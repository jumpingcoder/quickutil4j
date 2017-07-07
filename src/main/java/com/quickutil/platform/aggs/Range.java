package com.quickutil.platform.aggs;

import com.google.gson.JsonObject;
import com.quickutil.platform.exception.FormatQueryException;

/**
 * @author shijie.ruan
 */
public class Range {
	private String from = null, to = null, key = null;
	public Range(String from, String to, String key) { this.from = from; this.to = to; this.key = key; }
	public Range(Integer from, Integer to, String key) { this.from = Integer.toString(from); this.to = Integer.toString(to); this.key = key; }
	public Range(String from, String to) { this.from = from; this.to = to; }
	public Range(Integer from, Integer to) { this.from = Integer.toString(from); this.to = Integer.toString(to); }
	public Range() {}
	public Range setFrom(String from) { this.from = from; return this; }
	public Range setFrom(Integer from) { this.from = Integer.toString(from); return this;  }
	public Range setTo(String to) { this.to = to; return this; }
	public Range setTo(Integer to) { this.to = Integer.toString(to); return this; }
	public Range setKey(String key) { this.key = key; return this; }

	public JsonObject toJson() throws FormatQueryException {
		if (null == from && null == to) {
			throw new FormatQueryException("range should at least specify from or to");
		}
		JsonObject range = new JsonObject();
		if (null != from) { range.addProperty("from", from); }
		if (null != to) { range.addProperty("to", to); }
		if (null != key) { range.addProperty("key", key); }
		return range;
	}
}
