package com.quickutil.platform.query;

import com.google.gson.JsonObject;
import com.quickutil.platform.exception.FormatQueryException;

/**
 * @author shijie.ruan
 */
public abstract class QueryDSL {
	public final String type;
	protected Double boost = null;

	QueryDSL(String type) {
		this.type = type;
	}

	public abstract JsonObject toJson() throws FormatQueryException;
}
