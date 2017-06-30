package com.quickutil.platform.aggs;

import com.google.gson.JsonObject;

/**
 * @author shijie.ruan
 */
public class Order {
	private String field = null;
	private Sort sort = Sort.desc;

	Order(String field) {
		this.field = field;
	}

	Order(String field, Sort sort) {
		this.field = field;
		this.sort = sort;
	}

	public JsonObject toJson() {
		JsonObject order = new JsonObject();
		order.addProperty(field, sort.toString());
		return order;
	}

	public enum MetaField {
		_count, _key
	}

	public enum Sort {
		asc, desc
	}
}
