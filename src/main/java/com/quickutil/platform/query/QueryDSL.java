package com.quickutil.platform.query;

/**
 * @author shijie.ruan
 */
public abstract class QueryDSL {
	public final String type;
	protected Double boost = null;

	QueryDSL(String type) {
		this.type = type;
	}

	public abstract String toJson() throws FormatQueryException;
}
