package com.quickutil.platform.query;

import com.quickutil.platform.FormatQueryException;

/**
 * @author shijie.ruan
 */
public abstract class QueryDSL {
	public final String type;
	protected Double boost = null;

	QueryDSL(String type) {
		this.type = type;
		System.out.println("haha");
	}

	public abstract String toJson() throws FormatQueryException;
}
