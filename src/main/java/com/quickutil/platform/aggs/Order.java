package com.quickutil.platform.aggs;

import com.google.gson.JsonObject;

/**
 * @author shijie.ruan
 */
public class Order {
	private String field = null;
	private Sort sort = Sort.desc;
	private Boolean useKeyWord = false;

	public Order(String field) {
		this.field = field;
	}

	public Order(String field, Sort sort) {
		this.field = field;
		this.sort = sort;
	}

	/**
	 * 5.x 在默认情况下会将字符串存储为 text 和 keyword 字段, 字段名默认指向 text, 用于搜索, 不支持聚合,排序
	 * 如果你没有进行 mapping 设置,而在使用字符串字段进行聚合和排序的时候报错如下:
	 * Fielddata is disabled on text fields by default. Set fielddata=true on [] in order to load
	 * fielddata in memory by uninverting the inverted index.
	 * 你可以调用这个函数使用它的 keyword 字段进行排序或者聚合,但是还是建议设置 mapping, 如果需要使用字符串进行
	 * 排序和聚合等,请使用 keyword datatype.这样节省空间,而且在排序和聚合时不用使用.keyword
	 */
	public Order useKeyWord() {
		this.useKeyWord = true;
		return this;
	}

	public JsonObject toJson() {
		JsonObject order = new JsonObject();
		if (useKeyWord)
			order.addProperty(field + ".keyword", sort.toString());
		else
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
